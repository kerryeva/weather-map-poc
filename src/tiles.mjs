import {mkdir,writeFile,readFile,rm} from 'node:fs/promises';
import {createReadStream,createWriteStream} from 'node:fs';
import {once} from 'node:events';import {createInterface} from 'node:readline';
import {spawnSync} from 'node:child_process';import {gzipSync,gunzipSync} from 'node:zlib';
import geojsonvt from 'geojson-vt';import vtpbf from 'vt-pbf';import {VectorTile} from '@mapbox/vector-tile';import {PbfReader} from 'pbf';

export async function buildTiles(geojson,folder,{maxzoom=5,python='python3',clean=true,saveDecoded=false}={}){
 await mkdir(folder,{recursive:true});await rm(folder+'/tiles',{recursive:true,force:true});
 const idx=geojsonvt(geojson,{maxZoom:maxzoom,indexMaxZoom:maxzoom,indexMaxPoints:0,tolerance:.5,extent:8192,buffer:64});
 const raw=folder+'/tile-input.jsonl',ready=folder+'/tile-ready.jsonl',stream=createWriteStream(raw);
 for(let z=0;z<=maxzoom;z++)for(let x=0;x<2**z;x++)for(let y=0;y<2**z;y++){
  const tile=idx.getTile(z,x,y);if(!tile?.features.length)continue;
  if(!stream.write(JSON.stringify({z,x,y,features:tile.features})+'\n'))await once(stream,'drain');
 }stream.end();await once(stream,'finish');
 if(clean){const r=spawnSync(python,[new URL('../scripts/clean_tiles.py',import.meta.url).pathname,raw,ready],{encoding:'utf8',maxBuffer:1048576});if(r.status!==0)throw Error('瓦片几何处理失败：'+r.stderr);}
 let count=0,bytes=0;const decoded=[];
 for await(const line of createInterface({input:createReadStream(clean?ready:raw),crlfDelay:Infinity})){
  const tile=JSON.parse(line);if(!tile.features.length)continue;
  const bin=gzipSync(vtpbf.fromGeojsonVt({weather:{features:tile.features}},{extent:8192,version:2}),{level:9});
  const path=`${folder}/tiles/${tile.z}/${tile.x}`;await mkdir(path,{recursive:true});await writeFile(path+`/${tile.y}.mvt`,bin);count++;bytes+=bin.length;
  if(saveDecoded&&[0,2,maxzoom].includes(tile.z)){
   const v=new VectorTile(new PbfReader(gunzipSync(bin))).layers.weather;
   for(let i=0;i<v.length;i++)decoded.push({z:tile.z,x:tile.x,y:tile.y,feature:v.feature(i).toGeoJSON(tile.x,tile.y,tile.z)});
  }
 }
 if(saveDecoded)await writeFile(folder+'/decoded.json',JSON.stringify(decoded));
 // 合法的空白产品也写入一个空瓦片，不能把它误当成缺失或打包异常。
 if(count===0){
  const bin=gzipSync(vtpbf.fromGeojsonVt({weather:{features:[]}},{extent:8192,version:2}),{level:9});
  await mkdir(folder+'/tiles/0/0',{recursive:true});await writeFile(folder+'/tiles/0/0/0.mvt',bin);count=1;bytes=bin.length;
 }
 const repair=clean?JSON.parse(await readFile(ready.replace('.jsonl','.metrics.json'),'utf8')):null;
 await rm(raw);if(clean)await rm(ready);
 return {tiles:count,compressed_bytes:bytes,repair};
}
