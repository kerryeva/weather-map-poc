// 网格坐标依据 Open-Meteo GfsDomain.swift；只处理公开数据，不读取插件。
import {GRID} from './catalog.mjs';
export const GFS013_GRID={nx:3072,ny:1536,lonMin:-180,latMin:-.11714935*(1536-1)/2,dx:360/3072,dy:.11714935};

export function regrid(values,source,target=GRID) {
  const {nx,ny,lonMin,latMin,dx,dy}=source;
  if(values.length!==nx*ny||nx<2||ny<2||dx<=0||dy<=0||Math.abs(nx*dx-360)>.001)throw Error('源网格尺寸或坐标异常');
  const output=new Float32Array(target.nx*target.ny);
  for(let y=0;y<target.ny;y++) {
    const sy=Math.max(0,Math.min(ny-1,(target.latMin+y*target.dy-latMin)/dy));
    const y0=Math.floor(sy),y1=Math.min(ny-1,y0+1),ty=sy-y0;
    for(let x=0;x<target.nx;x++) {
      const sx=(((target.lonMin+x*target.dx-lonMin)/dx)%nx+nx)%nx;
      const x0=Math.floor(sx),x1=(x0+1)%nx,tx=sx-x0;
      output[y*target.nx+x]=(values[y0*nx+x0]*(1-tx)+values[y0*nx+x1]*tx)*(1-ty)
        +(values[y1*nx+x0]*(1-tx)+values[y1*nx+x1]*tx)*ty;
    }
  }
  return output;
}
