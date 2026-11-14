import {readFile,writeFile,mkdir,rm} from 'node:fs/promises';
import {createHash} from 'node:crypto';
import {gzipSync,gunzipSync} from 'node:zlib';
import {spawnSync} from 'node:child_process';
import {endianness} from 'node:os';
import {catalog,GRID,STYLES} from '../src/catalog.mjs';
import {derive} from '../src/fields.mjs';
import {smoothField} from '../src/smoothing.mjs';
import {lineContours} from '../src/lines.mjs';
import {fillBands} from '../src/fill.mjs';
import {buildTiles} from '../src/tiles.mjs';

const config=JSON.parse(await readFile(process.argv[2],'utf8'));
const {fieldsDir,scratchDir,outputDir,profile,lead,maxzoom,python}=config;
if(endianness()!=='LE'||!Number.isInteger(lead)||lead<0||lead>120||lead%3||!Number.isInteger(maxzoom)||maxzoom<0||maxzoom>5)throw Error('运行参数无效');
await mkdir(scratchDir,{recursive:true});await mkdir(outputDir,{recursive:true});
const loaded=new Map(),derived=new Map(),rows=[];
async function load(name){
  if(!loaded.has(name)){
    const bytes=await readFile(`${fieldsDir}/${name}.f32`);
    if(bytes.length!==GRID.nx*GRID.ny*4)throw Error('原生网格尺寸变化：'+name);
    loaded.set(name,new Float32Array(bytes.buffer,bytes.byteOffset,bytes.length/4));
  }
  return loaded.get(name);
}
function runPython(script,args){
  const result=spawnSync(python,[new URL(script,import.meta.url).pathname,...args],{encoding:'utf8',maxBuffer:1048576});
  if(result.status!==0)throw Error(`${script} 失败：${result.stderr}`);
  return JSON.parse(result.stdout);
}
for(const product of catalog(profile)){
  // 起报时刻不存在“前一累计时段”，单独记录，不能伪造零降水。
  if(lead===0&&product.id==='rain'){
    rows.push({key:product.key,status:'not_applicable',reason:'initial_time_has_no_preceding_accumulation'});continue;
  }
  const start=performance.now(),cpu=process.cpuUsage(),folder=`${scratchDir}/${product.key}`;
  await mkdir(folder,{recursive:true});
  const data=derive(product,await Promise.all(product.inputs.map(load)),derived);
  let metrics,file;
  if(product.kind==='wind'){
    file=`${outputDir}/${product.key}.wind.f32.gz`;
    const raw=Buffer.from(data.buffer,data.byteOffset,data.byteLength),packed=gzipSync(raw,{level:9});
    if(!gunzipSync(packed).equals(raw))throw Error('风场压缩校验失败');
    await writeFile(file,packed);
    metrics={bytes:packed.length,sha256:createHash('sha256').update(packed).digest('hex'),wind_grid:{nx:720,ny:361,order:'north_to_south',components:['u','v']}};
  }else{
    let finite=0,min=Infinity,max=-Infinity;
    for(const v of data){if(v===Infinity||v===-Infinity)throw Error('产品含无穷值');if(Number.isFinite(v)){finite++;min=Math.min(min,v);max=Math.max(max,v);}}
    if(!finite)throw Error('产品全场缺测：'+product.key);
    // 降水保留原场，CAPE 弱平滑；其余沿用已验证的候选参数。
    const sigma=product.kind==='line'?2:product.id==='rain'?0:product.id==='cape'?.5:2;
    const geo=product.kind==='line'?lineContours(smoothField(data,GRID,sigma),GRID,STYLES[product.id].interval):fillBands(product.id,data,GRID,{sigma});
    await writeFile(folder+'/product.geojson',JSON.stringify(geo));
    const sourceCheck=runPython('./clean_source.py',[folder+'/product.geojson']);
    const checked=JSON.parse(await readFile(folder+'/product.geojson','utf8'));
    const tiles=await buildTiles(checked,folder,{maxzoom,python});
    file=`${outputDir}/${product.key}.pmtiles`;
    metrics={...runPython('./pack_one.py',[folder,file,String(maxzoom)]),finite,min,max,sigma,source_check:sourceCheck,tiles};
  }
  const used=process.cpuUsage(cpu);
  const row={key:product.key,kind:product.kind,file,status:'verified',...metrics,elapsed_seconds:(performance.now()-start)/1000,cpu_seconds:(used.user+used.system)/1e6};
  // Node 的 maxRSS 在各平台均以 KiB 返回。
  row.node_peak_rss_bytes=process.resourceUsage().maxRSS*1024;
  rows.push(row);await writeFile(outputDir+'/products.json',JSON.stringify(rows,null,2)+'\n');
  await rm(folder,{recursive:true});
  console.log(JSON.stringify({product:product.key,bytes:row.bytes,seconds:Math.round(row.elapsed_seconds)}));
}
await writeFile(outputDir+'/products.json',JSON.stringify(rows,null,2)+'\n');
