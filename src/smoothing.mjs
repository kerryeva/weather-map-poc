// 保留双精度直到缺测权重归一化完成，防止常量在分级阈值两侧抖动。
export function gaussian64(values,nx,ny,sigma=2){
 if(!Number.isInteger(nx)||!Number.isInteger(ny)||nx<1||ny<1||values.length!==nx*ny||!Number.isFinite(sigma)||sigma<0)throw Error('格点或平滑参数无效');
 if(sigma===0)return Float64Array.from(values);
 const radius=Math.floor(4*sigma+.5),weights=Array.from({length:2*radius+1},(_,i)=>Math.exp(-.5*((i-radius)/sigma)**2)),total=weights.reduce((a,b)=>a+b,0);
 return separable(values,nx,ny,weights,total);
}
function separable(values,nx,ny,weights,total){
 const radius=(weights.length-1)/2;
 const temp=new Float64Array(values.length),out=new Float64Array(values.length);
 for(let y=0;y<ny;y++)for(let x=0;x<nx;x++){let sum=0;for(let k=-radius;k<=radius;k++)sum+=values[y*nx+((x+k)%nx+nx)%nx]*weights[k+radius];temp[y*nx+x]=sum/total;}
 for(let y=0;y<ny;y++)for(let x=0;x<nx;x++){let sum=0;for(let k=-radius;k<=radius;k++){let j=((y+k)%(2*ny)+2*ny)%(2*ny);if(j>=ny)j=2*ny-1-j;sum+=temp[j*nx+x]*weights[k+radius];}out[y*nx+x]=sum/total;}
 return out;
}
// 五点二项式核在交替格点上的正负偏差恰好相消，空间标准差为一个格距。
export function smoothRainTrace(values,grid){
 return maskedSmooth(values,grid,(v,nx,ny)=>separable(v,nx,ny,[1,4,6,4,1],16));
}
export function smoothField(values,grid,sigma=2){
 return maskedSmooth(values,grid,(v,nx,ny)=>gaussian64(v,nx,ny,sigma));
}
function maskedSmooth(values,grid,filter){
 if(values.length!==grid.nx*grid.ny)throw Error('格点尺寸异常');
 if(values.some(v=>v===Infinity||v===-Infinity))throw Error('格点含无穷值');
 if(values.every(Number.isFinite))return Float32Array.from(filter(values,grid.nx,grid.ny));
 const valid=Float64Array.from(values,v=>Number.isFinite(v)?1:0);
 const numerator=filter(Float64Array.from(values,v=>Number.isFinite(v)?v:0),grid.nx,grid.ny);
 const weights=filter(valid,grid.nx,grid.ny);
 return Float32Array.from(numerator,(v,i)=>valid[i]&&weights[i]>0?v/weights[i]:NaN);
}
