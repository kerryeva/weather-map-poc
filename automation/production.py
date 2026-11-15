"""00/12 UTC 自动生产入口。仅在需要制图时让工作流安装完整依赖。"""
import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts'))
from batch import acquire_lease, model_store, release_lease
from model_sources import BASE, definition, model_plan, stamp
from storage import PREFIX, Publisher, RUN_ID, SafetyStop, encoded

LEADS = list(range(0, 121, 3))
UTC = timezone.utc


def cycle_id(run):
    return run.strftime('%Y%m%d%H')


def read_cycle(spec, run, session):
    """固定日期目录不受 latest 在检查与制图之间移动的影响。"""
    bundle = {}
    for domain in sorted({field['domain'] for field in spec['fields']}):
        url = BASE + domain + '/' + run.strftime('%Y/%m/%d/%H%MZ/meta.json')
        response = session.get(url, timeout=(10, 30))
        if response.status_code == 404:
            # 部分源先公布正在传输的目录，满足 F120 即可，不必等全部远期预报。
            response = session.get(BASE + domain + '/in-progress.json', timeout=(10, 30))
            if response.status_code == 404:
                return None
        response.raise_for_status()
        meta = response.json()
        if stamp(meta['reference_time']) != run:
            return None
        bundle[domain] = meta
    try:
        model_plan(spec, {d: [m] for d, m in bundle.items()}, LEADS)
    except ValueError:
        return None
    return bundle


def reconcile(store, available, latest):
    """处理完整目录已切换、旧批清理或最新指针因中断未收尾的情况。"""
    run_id = available['run_id']
    receipt_key = store.prefix + 'scheduler-complete.json'
    if latest and latest['run_id'] == run_id and store.read_json(receipt_key) == {'run_id': run_id}:
        return
    lease = acquire_lease(store)
    try:
        publisher = Publisher(store)
        if publisher.available != available or publisher.latest != latest:
            raise SafetyStop('目录刚被另一任务更新，下轮重新检查')
        manifest = store.read_json(store.prefix + 'runs/' + run_id + '/manifest.json')
        if not manifest or manifest.get('run_id') != run_id or manifest.get('status') != 'complete':
            raise SafetyStop('完整起报清单缺失，保留现有资料')
        if latest and latest['run_id'][:10] > run_id[:10]:
            raise SafetyStop('最新完整起报与可用目录冲突')
        if sorted(t['lead'] for t in available['times']) != LEADS or sorted(t['lead'] for t in manifest['times']) != LEADS:
            raise SafetyStop('完整起报时次不足，保留旧批')
        publisher.target = run_id
        publisher.finish_run(manifest, LEADS)
        publisher.put_json(receipt_key, {'run_id': run_id})
    finally:
        release_lease(store, lease)


def select_job(store, model, now, fetch):
    prefix = store.prefix
    available = store.read_json(prefix + 'available.json') or {}
    lock = store.read_json(PREFIX + 'writer-lock.json')
    if lock and lock['expires'] > now.timestamp():
        return f"writer active: {available.get('run_id', 'starting')}, {len(available.get('times', []))}/41; next check in 30 min", None
    latest = store.read_json(prefix + 'latest.json') or {}
    for item in (available, latest):
        if item and not RUN_ID.fullmatch(item.get('run_id', '')):
            raise SafetyStop('发布目录中的起报编号异常')
    if available.get('complete'):
        reconcile(store, available, latest)
        latest = available
    completed = latest.get('run_id', '')[:10]
    active = available.get('run_id', '')[:10]
    target = now.replace(hour=12 if now.hour >= 12 else 0, minute=0, second=0, microsecond=0)
    candidates = [target, target - timedelta(hours=12)]
    if active and not available.get('complete'):
        resume = datetime.strptime(active, '%Y%m%d%H').replace(tzinfo=UTC)
        # 首批尚无完整回退时优先续做，避免撤下插件唯一可读的批次。
        candidates = [resume] if not latest else (candidates + [resume])
    for run in dict.fromkeys(candidates):
        if cycle_id(run) <= completed or (active and cycle_id(run) < active):
            continue
        bundle = fetch(run)
        if bundle:
            attempt = store.read_json(prefix + 'scheduler-attempt.json') or {}
            if attempt.get('cycle') == cycle_id(run) and now.timestamp() < attempt['retry_after']:
                return 'retry cooling down: ' + cycle_id(run), None
            return 'ready: ' + model + ' ' + cycle_id(run), bundle
    if completed and now-datetime.strptime(completed,'%Y%m%d%H').replace(tzinfo=UTC)>timedelta(hours=30):
        raise RuntimeError('发布目录超过30小时未更新，未发现可运行的完整源；下个定时任务会继续检查')
    return 'no newer complete source; last published: ' + (completed or 'none'), None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('phase', choices=['check', 'begin'])
    args = parser.parse_args()
    model = os.environ['MODEL']
    key = model + '_R2_'
    for suffix, env in [('BUCKET','TARGET_BUCKET'), ('ACCESS_KEY_ID','TARGET_KEY'), ('SECRET_ACCESS_KEY','TARGET_SECRET')]:
        os.environ[key + suffix] = os.environ[env]
    store = model_store(model, 'weather-v1/' + model.lower() + '/full/')
    path = Path(os.environ['RUNNER_TEMP']) / 'scheduled-metadata.json'
    now = datetime.now(UTC)
    if args.phase == 'begin':
        run = stamp(next(iter(json.loads(path.read_text()).values()))['reference_time'])
        attempt_key = store.prefix + 'scheduler-attempt.json'
        previous = store.read_json(attempt_key) or {}
        delay = min(6, previous.get('hours', 0) * 2 or 1) if previous.get('cycle') == cycle_id(run) else 1
        store.put(attempt_key, encoded({'cycle':cycle_id(run),'hours':delay,'retry_after':now.timestamp()+delay*3600}))
        return
    spec = definition(model, 'full')
    with requests.Session() as session:
        message, bundle = select_job(store, model, now, lambda run: read_cycle(spec, run, session))
    if bundle:
        path.write_text(json.dumps(bundle))
    print(model, message)
    with open(os.environ['GITHUB_OUTPUT'], 'a') as out:
        print('publish=' + ('true' if bundle else 'false'), file=out)
    with open(os.environ['GITHUB_STEP_SUMMARY'], 'a') as out:
        print(f'{model} — {message}', file=out)


if __name__ == '__main__':
    main()
