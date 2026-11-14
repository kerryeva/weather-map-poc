import {readFile,writeFile,mkdir,link} from 'node:fs/promises';
import {endianness} from 'node:os';
import {GRID} from '../src/catalog.mjs';
import {regrid,GFS013_GRID} from '../src/regrid.mjs';

const job=JSON.parse(await readFile(process.argv[2],'utf8'));
if(endianness()!=='LE'||!['ecmwf_ifs025','ncep_gfs025','ncep_gfs013'].includes(job.domain))throw Error('不支持的源网格');
await mkdir(job.outputDir,{recursive:true});
const rows=JSON.parse(await readFile(job.inputDir+'/metadata.json','utf8'));
for(const row of rows) {
  if(!/^[A-Za-z0-9_]+$/.test(row.variable))throw Error('变量名无效');
  const source=job.inputDir+'/'+row.variable+'.f32',target=job.outputDir+'/'+row.variable+'.f32';
  const grid=job.domain==='ncep_gfs013'?GFS013_GRID:GRID;
  if(row.shape[0]!==grid.ny||row.shape[1]!==grid.nx)throw Error('源网格尺寸变化');
  if(job.domain!=='ncep_gfs013')await link(source,target);
  else {
    const bytes=await readFile(source);
    if(bytes.length!==grid.nx*grid.ny*4)throw Error('原生字段长度不正确');
    const values=regrid(new Float32Array(bytes.buffer,bytes.byteOffset,bytes.length/4),grid);
    await writeFile(target,Buffer.from(values.buffer));
  }
}
