import {contours} from 'd3-contour';
export function lineContours(values,grid,interval){
 const {nx,ny,lonMin,latMin,dx,dy}=grid,width=nx+2,padded=new Float64Array(width*ny);
 if(values.length!==nx*ny||!values.every(Number.isFinite)||!Number.isFinite(interval)||interval<=0)throw Error('等值线格点或间隔无效');
 let min=Infinity,max=-Infinity;for(const v of values){min=Math.min(min,v);max=Math.max(max,v);}
 if((max-min)/interval>2000)throw Error('等值线数量异常，请检查单位');
 for(let y=0;y<ny;y++){padded[y*width]=values[y*nx+nx-1];padded.set(values.subarray(y*nx,(y+1)*nx),y*width+1);padded[y*width+nx+1]=values[y*nx];}
 const levels=[];for(let v=Math.ceil(min/interval)*interval;v<=max;v+=interval)levels.push(v);
 const features=[],limit=85.05112878;
 for(const contour of contours().size([width,ny]).thresholds(levels).smooth(true)(padded))for(const polygon of contour.coordinates)for(const ring of polygon){
  let part=[];const emit=()=>{if(part.length>1)features.push({type:'Feature',properties:{value:contour.value},geometry:{type:'LineString',coordinates:part}});part=[];};
  for(let i=1;i<ring.length;i++){
   const aa=ring[i-1],bb=ring[i];
   // 全球网格人工封边不能当成物理等值线。
   if(aa[0]<=.5||aa[0]>=width-.5||aa[1]<=.5||aa[1]>=ny-.5||bb[0]<=.5||bb[0]>=width-.5||bb[1]<=.5||bb[1]>=ny-.5){emit();continue;}
   const a=[lonMin+(aa[0]-1.5)*dx,latMin+(aa[1]-.5)*dy],b=[lonMin+(bb[0]-1.5)*dx,latMin+(bb[1]-.5)*dy],vx=b[0]-a[0],vy=b[1]-a[1];let lo=0,hi=1,ok=true;
   for(const [p,q] of [[-vx,a[0]+180],[vx,180-a[0]],[-vy,a[1]+limit],[vy,limit-a[1]]]){if(p===0){if(q<0)ok=false;continue;}const t=q/p;if(p<0)lo=Math.max(lo,t);else hi=Math.min(hi,t);}
   if(!ok||lo>hi){emit();continue;}const first=[a[0]+lo*vx,a[1]+lo*vy],last=[a[0]+hi*vx,a[1]+hi*vy];
   if(part.length&&Math.hypot(part.at(-1)[0]-first[0],part.at(-1)[1]-first[1])>1e-8)emit();if(!part.length)part.push(first);part.push(last);if(hi<1)emit();
  }emit();
 }
 return {type:'FeatureCollection',features};
}
