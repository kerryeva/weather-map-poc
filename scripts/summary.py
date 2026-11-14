"""只向 Actions 日志和摘要写入小型统计，不创建大型 Artifact 或数据缓存。"""
from pathlib import Path
import argparse,base64,json,os,zlib

parser=argparse.ArgumentParser();parser.add_argument('report',type=Path);parser.add_argument('--archive-log',action='store_true');args=parser.parse_args()
if not args.report.exists():
    raise SystemExit('报告尚未生成；检查准备阶段日志。')
report=json.loads(args.report.read_text());times=report.get('times',[])
total=sum(row.get('product_bytes',0) for row in times)
disk=report.get('disk',{})
lines=['# IFS 单批资源测试',f"状态：{report['status']}",f"起报：{report['run']}",
    f"范围：{report['processing']['profile']}；完成时次：{len(times)}/{len(report['plan'])}",
    f"成品合计：{total/1e9:.6f} GB（此任务模式：{report['mode']}）",
    f"耗时：{report.get('elapsed_seconds',0)/60:.2f} 分钟",
    f"临时目录抽样峰值：{disk.get('observed_peak_work_bytes',0)/1e9:.6f} GB",
    f"文件系统最低剩余空间：{disk.get('lowest_free_bytes',0)/1e9:.3f} GB",
    f"源代码及参数指纹：{report['processing_sha256']}",
    '','|预报小时|成品GB|实际生成产品数|','|---:|---:|---:|']
for row in times:
    lines.append(f"|{row['lead']}|{row['product_bytes']/1e9:.6f}|{sum(p['status']=='verified' for p in row['products'])}|")
lines+=['','本报告没有证明私有 R2/真实宿主的显示性能，也没有证明整月免费。']
text='\n\n'.join(lines)+'\n';print(text)
summary=os.environ.get('GITHUB_STEP_SUMMARY')
if summary:
    with open(summary,'a') as stream:stream.write(text)
if args.archive_log:
    # 所有源数据均公开；不含环境变量、凭据、账户ID或私有业务代码。
    compact=json.dumps(report,ensure_ascii=False,separators=(',',':')).encode()
    packed=zlib.compress(compact,9)
    if len(packed)>1_000_000:raise SystemExit('报告超过日志归档大小上限')
    print('REPORT_ZLIB_BASE64='+base64.b64encode(packed).decode())
