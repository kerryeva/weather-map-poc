import {contours} from 'd3-contour';
import polygonClipping from 'polygon-clipping';
import {STYLES as DEFINITIONS,colorFor as fillColor} from './catalog.mjs';
import {smoothField} from './smoothing.mjs';
import {rainDisplayGrid} from './rain.mjs';

// 用相邻双精度数表达闭区间/严格大于，避免固定 epsilon 错分临界值。
function nextAbove(value){const b=new DataView(new ArrayBuffer(8));b.setFloat64(0,value);b.setBigUint64(0,b.getBigUint64(0)+1n);return b.getFloat64(0);}

// 先生成共享阈值区域再求差。此处不各自平滑/简化相邻色带的边界。
export function fillBands(id,values,grid,{sigma=0,onSmoothing}={}){
 const definition=DEFINITIONS[id];if(!definition?.breaks)throw Error('产品不是分级填色');
 const smoothStart=performance.now();
 let field;
 if(id==='rain')({field,grid}=rainDisplayGrid(values,grid));
 else field=smoothField(values,grid,sigma);
 onSmoothing?.((performance.now()-smoothStart)/1000);
 let min=Infinity,max=-Infinity;
 for(const v of field)if(Number.isFinite(v)){min=Math.min(min,v);max=Math.max(max,v);}
 if(min===Infinity)return {type:'FeatureCollection',features:[]};
 const epsilon=1e-5,levels=[min-epsilon,...definition.breaks];
 if(id==='rh'||id==='dew')levels[levels.length-1]=nextAbove(levels.at(-1));
 if(id==='cape')levels[1]=nextAbove(levels[1]);
 const thresholds=[...new Set(levels)].filter(v=>v<=max+epsilon).sort((a,b)=>a-b);
 const nx=grid.nx+2,padded=new Float64Array(nx*grid.ny);
 for(let y=0;y<grid.ny;y++){const at=y*nx,from=y*grid.nx;padded[at]=field[from+grid.nx-1];padded.set(field.subarray(from,from+grid.nx),at+1);padded[at+nx-1]=field[from];}
 const cumulative=contours().size([nx,grid.ny]).thresholds(thresholds).smooth(true)(padded);
 const xmin=1.5,xmax=xmin+360/grid.dx,limit=85.05112878;
 const ymin=(-limit-grid.latMin)/grid.dy+.5,ymax=(limit-grid.latMin)/grid.dy+.5;
 const world=[[[xmin,ymin],[xmax,ymin],[xmax,ymax],[xmin,ymax],[xmin,ymin]]];
 const features=[];
 for(let i=0;i<cumulative.length;i++){
  const band=cumulative[i],color=fillColor(id,band.value);if(!color||!band.coordinates.length)continue;
  const next=cumulative[i+1]?.coordinates;
  const region=next?.length?polygonClipping.difference(band.coordinates,next):band.coordinates;
  if(!region.length)continue;const clipped=polygonClipping.intersection(region,world);if(!clipped.length)continue;
  features.push({type:'Feature',properties:{value:band.value,color},geometry:{type:'MultiPolygon',coordinates:clipped.map(p=>p.map(r=>r.map(([x,y])=>[Math.max(-180,Math.min(180,grid.lonMin+(x-1.5)*grid.dx)),Math.max(-limit,Math.min(limit,grid.latMin+(y-.5)*grid.dy))])))}});
 }
 return {type:'FeatureCollection',features};
}
