# SPDX-License-Identifier: GPL-2.0-only
"""独立的GPLv2 OM读取工具；通过普通Float32文件与其他进程交换公开气象数据。"""
from pathlib import Path
import json,sys
import numpy as np
from omfiles import OmFileReader

def main():
    source=Path(sys.argv[1]);fields=Path(sys.argv[2]);variables=json.loads(sys.argv[3]);lead=int(sys.argv[4])
    fields.mkdir();rows=[]
    with OmFileReader(str(source)) as root:
        names={root.get_child_by_index(i).name for i in range(root.num_children)}
        for name in variables:
            if lead==0 and name=='precipitation':continue
            if name not in names:raise ValueError('源文件缺少变量：'+name)
            value=root.get_child_by_name(name).read_array((...,)).astype('<f4')
            if value.shape!=(721,1440):raise ValueError('IFS 原生网格尺寸变化')
            if np.isinf(value).any() or not np.isfinite(value).any():raise ValueError('变量无效：'+name)
            value.tofile(fields/(name+'.f32'))
            rows.append({'variable':name,'shape':list(value.shape),'finite':int(np.isfinite(value).sum()),
                'min':float(np.nanmin(value)),'max':float(np.nanmax(value))})
    (fields/'metadata.json').write_text(json.dumps(rows)+'\n')

if __name__=='__main__':main()
