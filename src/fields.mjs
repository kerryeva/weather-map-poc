import {GRID} from './catalog.mjs';

export function dewpointSpread(t, rh) {
  if (!Number.isFinite(t)||!Number.isFinite(rh)||rh<=0||t<=-243.04) return NaN;
  if (rh>=100) return 0;
  const gamma=Math.log(rh/100)+17.625*t/(243.04+t);
  return Math.max(0,t-243.04*gamma/(17.625-gamma));
}

export const omega = (w,t,p) => Number.isFinite(w)&&Number.isFinite(t)&&t>-273.15 ? -w*p*100/(287.058*(t+273.15))*9.80665 : NaN;

// 球面中心差分；经度周期相连，极点处不伪造有限值。输出单位为 10^-5 s^-1。
export function dynamics(u,v,grid=GRID) {
  const {nx,ny,dx,dy,latMin}=grid;
  if (u.length!==nx*ny||v.length!==u.length) throw Error('风场尺寸不一致');
  const relative=new Float32Array(u.length).fill(NaN),absolute=new Float32Array(u.length).fill(NaN),divergence=new Float32Array(u.length).fill(NaN);
  const rad=Math.PI/180,R=6371000,rotation=7.292115e-5;
  for (let y=1;y<ny-1;y++) {
    const phi=(latMin+y*dy)*rad,c=Math.cos(phi),cp=Math.cos(phi+dy*rad),cm=Math.cos(phi-dy*rad);
    if (Math.abs(c)<.02) continue;
    for (let x=0;x<nx;x++) {
      const i=y*nx+x,l=y*nx+(x+nx-1)%nx,r=y*nx+(x+1)%nx,n=i+nx,s=i-nx;
      const dv=(v[r]-v[l])/(2*dx*rad),du=(u[r]-u[l])/(2*dx*rad);
      relative[i]=(dv-(u[n]*cp-u[s]*cm)/(2*dy*rad))/(R*c)*1e5;
      absolute[i]=relative[i]+2*rotation*Math.sin(phi)*1e5;
      divergence[i]=(du+(v[n]*cp-v[s]*cm)/(2*dy*rad))/(R*c)*1e5;
    }
  }
  return {relative,absolute,divergence};
}

export function derive(product,inputs,cache=new Map()) {
  if (product.id==='wind') {
    const packed=new Float32Array(720*361*2);let to=0;
    for (let y=720;y>=0;y-=2) for (let x=0;x<1440;x+=2) {
      const i=y*1440+x;
      if (!Number.isFinite(inputs[0][i])||!Number.isFinite(inputs[1][i])) throw Error('风场存在缺测');
      packed[to]=inputs[0][i];packed[to+720*361]=inputs[1][i];to++;
    }
    return packed;
  }
  if (product.id==='dew') return Float32Array.from(inputs[0],(t,i)=>dewpointSpread(t,inputs[1][i]));
  if (product.id==='vertical') return Float32Array.from(inputs[0],(w,i)=>omega(w,inputs[1][i],Number(product.level)));
  if (['vort','div'].includes(product.id)) {
    const key=product.level;
    if (!cache.has(key)) cache.set(key,dynamics(...inputs));
    return cache.get(key)[product.id==='div'?'divergence':product.variant];
  }
  return inputs[0];
}
