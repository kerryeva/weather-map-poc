import {contours} from 'd3-contour';
// 经度两侧各复制的列数。1 列不够：等值线在 ±180 附近需要足够的缓冲才能连续描出，
// 否则会被"贴边丢弃"的规则截断，在 180° 经线上留下一条几十像素的缝。
const PAD=3;
// Web 墨卡托能表示的最大纬度，超出部分地图不显示。
const LIMIT=85.05112878;
export function lineContours(values,grid,interval){
 const {nx,ny,lonMin,latMin,dx,dy}=grid,width=nx+2*PAD,padded=new Float64Array(width*ny);
 if(values.length!==nx*ny||!values.every(Number.isFinite)||!Number.isFinite(interval)||interval<=0)throw Error('等值线格点或间隔无效');
 let min=Infinity,max=-Infinity;for(const v of values){min=Math.min(min,v);max=Math.max(max,v);}
 if((max-min)/interval>2000)throw Error('等值线数量异常，请检查单位');
 // 左右各复制 PAD 列，模拟全球经度环流；列 i（原始）落在 padded 的 i+PAD 上。
 for(let y=0;y<ny;y++){
  const row=y*width,src=y*nx;
  padded.set(values.subarray(src,src+nx),row+PAD);
  for(let k=0;k<PAD;k++){padded[row+k]=values[src+nx-PAD+k];padded[row+PAD+nx+k]=values[src+k];}
 }
 // 格点坐标：经度取格点值（lonMin + i*dx），纬度同理（latMin + j*dy）。
 const lonOf=x=>lonMin+(x-PAD)*dx,latOf=y=>latMin+y*dy;
 const levels=[];for(let v=Math.ceil(min/interval)*interval;v<=max;v+=interval)levels.push(v);
 const features=[],cleanup={duplicate_points_removed:0,zero_length_parts_removed:0};
 for(const contour of contours().size([width,ny]).thresholds(levels).smooth(true)(padded))for(const polygon of contour.coordinates)for(const ring of polygon){
  let part=[];const emit=()=>{
   // 临界值插值可能在浮点舍入后全部落在同一点；仅去掉连续重复点，不连接不同片段。
   const unique=[];
   for(const point of part){
    const previous=unique.at(-1);
    if(previous&&previous[0]===point[0]&&previous[1]===point[1])cleanup.duplicate_points_removed++;
    else unique.push(point);
   }
   if(unique.length>1)features.push({type:'Feature',properties:{value:contour.value},geometry:{type:'LineString',coordinates:unique}});
   else if(part.length>1)cleanup.zero_length_parts_removed++;
   part=[];
  };
  for(let i=1;i<ring.length;i++){
   const aa=ring[i-1],bb=ring[i];
   // 截断到墨卡托能表示的范围。经度也在这里截到 ±180：越界坐标会被 geojson-vt
   // 复制到另一侧，反而在接缝处画出锯齿。两侧同时截到 ±180，拼起来才是连续的。
   const a=[lonOf(aa[0]),latOf(aa[1])],b=[lonOf(bb[0]),latOf(bb[1])],vx=b[0]-a[0],vy=b[1]-a[1];let lo=0,hi=1,ok=true;
   for(const [p,q] of [[-vx,a[0]+180],[vx,180-a[0]],[-vy,a[1]+LIMIT],[vy,LIMIT-a[1]]]){if(p===0){if(q<0)ok=false;continue;}const t=q/p;if(p<0)lo=Math.max(lo,t);else hi=Math.min(hi,t);}
   if(!ok||lo>hi){emit();continue;}const first=[a[0]+lo*vx,a[1]+lo*vy],last=[a[0]+hi*vx,a[1]+hi*vy];
   if(part.length&&Math.hypot(part.at(-1)[0]-first[0],part.at(-1)[1]-first[1])>1e-8)emit();if(!part.length)part.push(first);part.push(last);if(hi<1)emit();
  }emit();
 }
 return {type:'FeatureCollection',features,contour_cleanup:cleanup};
}
