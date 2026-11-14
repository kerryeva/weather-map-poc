"""核对三个公开源目录使用同一起报；只输出有边界的双模式测试矩阵。"""
import argparse,json,os
from pathlib import Path
from batch import build_plan

DOMAINS=('ecmwf_ifs025','ncep_gfs025','ncep_gfs013')

def validate_bundle(bundle):
    if set(bundle)!=set(DOMAINS):raise ValueError('必须冻结IFS及GFS两个数据域')
    runs=[]
    for domain in DOMAINS:
        run,_=build_plan(bundle[domain],list(range(0,121,3)))
        runs.append(run)
    if len(set(runs))!=1:raise ValueError('三个数据域的起报时间不一致，禁止混用')
    return runs[0]

def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--metadata',type=Path,required=True)
    args=parser.parse_args()
    if args.metadata.stat().st_size>2_000_000:raise ValueError('冻结目录过大')
    bundle=json.loads(args.metadata.read_text());run=validate_bundle(bundle)
    matrix={'include':[{'model':model,'lead':lead} for lead in range(0,121,3) for model in ('IFS','GFS')]}
    with open(os.environ['GITHUB_OUTPUT'],'a') as stream:stream.write('matrix='+json.dumps(matrix,separators=(',',':'))+'\n')
    print('Frozen public IFS/GFS run:',run.isoformat(),'tasks:',len(matrix['include']))

if __name__=='__main__':main()
