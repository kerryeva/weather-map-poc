// 一个时次内部使用至多四个独立处理进程；不改变数值、平滑、几何或打包算法。
import {readFile,writeFile,mkdir} from 'node:fs/promises';
import {spawn} from 'node:child_process';
import {catalog} from '../src/catalog.mjs';

const config=JSON.parse(await readFile(process.argv[2],'utf8'));
const count=config.productWorkers;
if(!Number.isInteger(count)||count<1||count>4)throw Error('产品并行数只允许1至4');
const products=catalog(config.profile,config.model??'IFS'),rows=new Array(products.length),children=new Set();
await mkdir(config.outputDir,{recursive:true});
await mkdir(config.scratchDir,{recursive:true});
let next=0,failed=null;
async function slot(){
 try{
  while(next<products.length&&!failed){
    const index=next++,product=products[index];
    const outputDir=`${config.outputDir}/parts/${product.key}`;
    await mkdir(outputDir,{recursive:true});
    const job=`${outputDir}/job.json`;
    await writeFile(job,JSON.stringify({...config,outputDir,productKey:product.key,productWorkers:1}));
    const child=spawn(process.execPath,['--max-old-space-size=3072',new URL('./render.mjs',import.meta.url).pathname,job],{stdio:['ignore','pipe','inherit']});
    children.add(child);child.stdout.pipe(process.stdout,{end:false});
    try{
      await new Promise((resolve,reject)=>{
        child.once('error',reject);
        child.once('close',(code,signal)=>code===0?resolve():reject(Error(`${product.key} 处理失败：${code??signal}`)));
      });
      const result=JSON.parse(await readFile(outputDir+'/products.json','utf8'));
      if(result.length!==1||result[0].key!==product.key)throw Error('子进程返回产品清单不一致');
      rows[index]=result[0];
    }finally{children.delete(child);}
  }
 }catch(error){
   failed??=error;
   for(const active of children)active.kill('SIGTERM');
   throw error;
 }
}
const settled=await Promise.allSettled(Array.from({length:Math.min(count,products.length)},slot));
if(failed)throw failed;
const rejected=settled.find(result=>result.status==='rejected');if(rejected)throw rejected.reason;
if(rows.filter(Boolean).length!==products.length)throw Error('产品并行处理未完成，禁止发布');
await writeFile(config.outputDir+'/products.json',JSON.stringify(rows,null,2)+'\n');
