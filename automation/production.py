"""00/06/12/18 UTC 自动生产入口。仅在需要制图时让工作流安装完整依赖。"""
import argparse
import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts'))
from batch import acquire_lease, model_store, release_lease
from model_sources import BASE, definition, model_plan, stamp
from storage import PREFIX, Publisher, RUN_ID, SafetyStop, encoded

LEADS = list(range(0, 97, 3))
UTC = timezone.utc
CYCLE = timedelta(hours=6)


def cycle_id(run):
    return run.strftime('%Y%m%d%H')


def read_cycle(spec, run, session):
    """固定日期目录不受 latest 在检查与制图之间移动的影响。"""
    bundle = {}
    for domain in sorted({field['domain'] for field in spec['fields']}):
        url = BASE + domain + '/' + run.strftime('%Y/%m/%d/%H%MZ/meta.json')
        response = session.get(url, timeout=(10, 30))
        if response.status_code == 404:
            # 部分源先公布正在传输的目录，满足 F096 即可，不必等全部远期预报。
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



def upgrade_four_day_policy(store):
    """在独占写入租约下完成一次升级，失败后继续清理同一组过时资料。"""
    receipt = store.prefix + 'four-day-policy.json'
    policy = {'forecast_hours': 96, 'vorticity': 'relative', 'cape_ceiling': 2000}
    existing = store.read_json(receipt)
    if existing == policy:
        return
    if existing is not None:
        raise SafetyStop('四天产品升级记录异常')
    lock = store.read_json(PREFIX + 'writer-lock.json')
    if lock and lock['expires'] > datetime.now(UTC).timestamp():
        return
    lease = acquire_lease(store)
    try:
        owner_key = store.prefix + 'owner.json'
        owner = {'protocol': 'weather-tiles-v1', 'prefix': store.prefix}
        recorded = store.read_json(owner_key)
        objects = store.list()
        if recorded != owner:
            if recorded is not None or any(k.startswith(store.prefix) for k in objects):
                raise SafetyStop('发布前缀归属异常，停止升级清理')
            store.put(owner_key, encoded(owner))
        runs_prefix = store.prefix + 'runs/'
        obsolete = [k for k in objects if k.startswith(runs_prefix)]
        if any(not RUN_ID.fullmatch(k[len(runs_prefix):].split('/')[0]) for k in obsolete):
            raise SafetyStop('旧批次名称异常，停止升级清理')
        pointers = [store.prefix + name for name in
                    ('available.json', 'latest.json', 'scheduler-attempt.json', 'scheduler-complete.json')]
        store.delete([k for k in pointers if k in objects])
        store.delete(obsolete)
        remaining = store.list()
        if any(k.startswith(runs_prefix) or k in pointers for k in remaining):
            raise SafetyStop('过时资料尚未清理完毕')
        store.put(receipt, encoded(policy))
        print('Four-day upgrade: removed', len(obsolete), 'obsolete objects,',
              sum(objects[k]['bytes'] for k in obsolete), 'bytes')
    finally:
        release_lease(store, lease)


def candidate_runs(now, completed, unfinished):
    target = now.replace(hour=(now.hour // 6) * 6, minute=0, second=0, microsecond=0)
    if unfinished:
        run = datetime.strptime(unfinished, '%Y%m%d%H').replace(tzinfo=UTC)
        yield run
        if not completed:
            return
    elif completed:
        run = datetime.strptime(completed, '%Y%m%d%H').replace(tzinfo=UTC)
    else:
        # 空桶首次启动，从最近四批中选最新的可用起报。
        for offset in range(4):
            yield target - offset * CYCLE
        return
    run += CYCLE
    while run <= target:
        yield run
        run += CYCLE


# 未记录容量的旧任务使用 polygon-clipping 默认的一百万个端点。
DEFAULT_QUEUE_LIMIT = 1_000_000


def select_job(store, model, now, fetch, queue_limit=DEFAULT_QUEUE_LIMIT):
    prefix = store.prefix
    available = store.read_json(prefix + 'available.json') or {}
    lock = store.read_json(PREFIX + 'writer-lock.json')
    if lock and lock['expires'] > now.timestamp():
        return f"writer active: {available.get('run_id', 'starting')}, {len(available.get('times', []))}/{len(LEADS)}; next check in 30 min", None
    latest = store.read_json(prefix + 'latest.json') or {}
    for item in (available, latest):
        if item and not RUN_ID.fullmatch(item.get('run_id', '')):
            raise SafetyStop('发布目录中的起报编号异常')
    if available.get('complete'):
        reconcile(store, available, latest)
        latest = available
    completed = latest.get('run_id', '')[:10]
    active = available.get('run_id', '')[:10]
    unfinished = active if active > completed and not available.get('complete') else None
    for run in candidate_runs(now, completed, unfinished):
        bundle = fetch(run)
        if bundle:
            attempt = store.read_json(prefix + 'scheduler-attempt.json') or {}
            if attempt.get('cycle') == cycle_id(run) and now.timestamp() < attempt['retry_after']:
                larger_queue = queue_limit > attempt.get('queue_limit', DEFAULT_QUEUE_LIMIT)
                if unfinished != cycle_id(run) or not larger_queue:
                    return 'retry cooling down: ' + cycle_id(run), None
            return 'ready: ' + model + ' ' + cycle_id(run), bundle
    if completed and now-datetime.strptime(completed,'%Y%m%d%H').replace(tzinfo=UTC)>timedelta(hours=30):
        raise RuntimeError('发布目录超过30小时未更新，未发现可运行的完整源；下个定时任务会继续检查')
    return 'no newer complete source; last published: ' + (completed or 'none'), None


def processor_revision(store, cycle, current):
    """同一次起报从第一次开工到续跑都使用同一版制图程序。"""
    available = store.read_json(store.prefix + 'available.json') or {}
    run_id = available.get('run_id', '')
    attempt = store.read_json(store.prefix + 'scheduler-attempt.json') or {}
    revision = current
    if run_id[:10] == cycle and not available.get('complete'):
        if not RUN_ID.fullmatch(run_id):
            raise SafetyStop('正在制作的批次编号无效')
        progress = store.read_json(store.prefix + 'runs/' + run_id + '/progress.json') or {}
        revision = progress.get('processor_commit')
        if not revision and attempt.get('cycle') == cycle:
            revision = attempt.get('processor_commit')
        # 升级前已在两个正式桶发布的批次没有记录提交号。
        if not revision and run_id in ('2026111418-35409f439bc2', '2026111418-b72254d378ed'):
            revision = 'b6340d697af566e3ffaf96f595c729c88bb698c7'
        if not revision:
            raise SafetyStop('在制起报缺少制图版本，保留现有资料')
    if not re.fullmatch(r'[0-9a-f]{40}', revision):
        raise SafetyStop('制图版本标识异常')
    return revision


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
    queue_limit = int(os.environ.get('POLYGON_CLIPPING_MAX_QUEUE_SIZE', DEFAULT_QUEUE_LIMIT))
    if args.phase == 'begin':
        run = stamp(next(iter(json.loads(path.read_text()).values()))['reference_time'])
        attempt_key = store.prefix + 'scheduler-attempt.json'
        previous = store.read_json(attempt_key) or {}
        delay = min(6, previous.get('hours', 0) * 2 or 1) if previous.get('cycle') == cycle_id(run) else 1
        store.put(attempt_key, encoded({'cycle':cycle_id(run),'processor_commit':os.environ['PROCESSOR_COMMIT'],'hours':delay,'queue_limit':queue_limit,'retry_after':now.timestamp()+delay*3600}))
        return
    upgrade_four_day_policy(store)
    spec = definition(model, 'full')
    with requests.Session() as session:
        message, bundle = select_job(store, model, now, lambda run: read_cycle(spec, run, session), queue_limit)
    revision = ''
    if bundle:
        run = stamp(next(iter(bundle.values()))['reference_time'])
        revision = processor_revision(store, cycle_id(run), os.environ['GITHUB_SHA'])
        path.write_text(json.dumps(bundle))
    print(model, message)
    with open(os.environ['GITHUB_OUTPUT'], 'a') as out:
        print('publish=' + ('true' if bundle else 'false'), file=out)
        print('processor=' + revision, file=out)
    with open(os.environ['GITHUB_STEP_SUMMARY'], 'a') as out:
        print(f'{model} — {message}', file=out)


if __name__ == '__main__':
    main()
