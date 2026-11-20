import {test} from 'node:test';
import assert from 'node:assert/strict';
import {catalog,fieldsFor,colorFor,GRID} from '../src/catalog.mjs';
import {dynamics,dewpointSpread,omega,derive} from '../src/fields.mjs';
import {fillBands} from '../src/fill.mjs';
import {lineContours} from '../src/lines.mjs';

test('临界值插值舍入成同一点时删除零长线，保留非零短线',()=>{
  const grid={nx:9,ny:9,lonMin:-87,latMin:-14,dx:.25,dy:.25};
  for(const delta of [1e-14,1e-12]){
    const values=new Float64Array(81).fill(-56);values[40]=-52+delta;values[60]=-48;
    const result=lineContours(values,grid,4);
    for(const feature of result.features){
      const points=feature.geometry.coordinates;assert.ok(points.length>=2);
      for(let i=1;i<points.length;i++)assert.notDeepEqual(points[i],points[i-1]);
    }
    if(delta===1e-12)assert.ok(result.features.some(f=>f.properties.value===-52),'不能按长度阈值删掉真实短线');
  }
});

test('IFS 48组合和43变量，仅提供相对涡度',()=>{
  const products=catalog();assert.equal(products.length,48);
  assert.equal(new Set(products.map(x=>x.key)).size,48);
  assert.equal(products.filter(x=>x.variant==='absolute').length,0);
  assert.equal(fieldsFor('full').length,43);
  for(const level of [1000,925,850,700,500,200])assert.equal(products.filter(x=>x.level===level).length,7);
  assert.deepEqual(catalog('500hpa').map(x=>x.key),['IFS-500-height-default']);
  assert.throws(()=>catalog('unknown'));
});
test('严格CAPE门槛与闭合RH、温露差边界',()=>{
  assert.equal(colorFor('cape',500),null);assert.ok(colorFor('cape',500.00001));
  assert.ok(colorFor('rh',100));assert.equal(colorFor('rh',100.00001),null);
  assert.ok(colorFor('dew',4));assert.equal(colorFor('dew',4.00001),null);
});
test('全零CAPE为合法空白，阈值500不生成区域',()=>{
  const grid={nx:8,ny:5,lonMin:-180,latMin:-90,dx:45,dy:45};
  for(const v of [0,500])assert.equal(fillBands('cape',new Float32Array(40).fill(v),grid).features.length,0);
});
test('球面风导数符合固体旋转解析解',()=>{
  const grid={nx:36,ny:19,dx:10,dy:10,latMin:-90};
  const u=Float32Array.from({length:grid.nx*grid.ny},(_,i)=>20*Math.cos((-90+Math.floor(i/grid.nx)*10)*Math.PI/180));
  const v=new Float32Array(u.length),result=dynamics(u,v,grid),at=12*grid.nx;
  const expected=40*Math.sin(Math.PI/6)/6371000*1e5;
  assert.ok(Math.abs(result.relative[at]-expected)<.01);
  assert.equal(result.divergence[at],0);assert.ok(Number.isNaN(result.relative[0]));
  assert.ok(Math.abs(result.absolute[at]-result.relative[at]-2*7.292115e-5*.5*1e5)<1e-5);
});
test('湿度、垂直速度的物理边界',()=>{
  assert.equal(dewpointSpread(20,100),0);assert.equal(dewpointSpread(20,110),0);
  assert.ok(Number.isNaN(dewpointSpread(20,0)));assert.ok(Math.abs(dewpointSpread(20,50)-10.738)<.02);
  assert.ok(omega(1,-10,500)<0);assert.ok(Number.isNaN(omega(1,-273.15,500)));
});
test('风场压缩网格北至南，分量顺序u后v',()=>{
  const u=Float32Array.from({length:GRID.nx*GRID.ny},(_,i)=>Math.floor(i/GRID.nx)),v=new Float32Array(u.length).fill(5);
  const packed=derive({id:'wind'},[u,v]);assert.equal(packed.length,720*361*2);
  assert.equal(packed[0],720);assert.equal(packed[720*360],0);assert.equal(packed[720*361],5);
});
test('全部7个风场共用缺测拦截，不能把缺测伪造成零风',()=>{
  const u=new Float32Array(GRID.nx*GRID.ny).fill(3),v=new Float32Array(u.length).fill(4);
  const products=catalog().filter(p=>p.kind==='wind');assert.equal(products.length,7);
  for(const product of products){
    const packed=derive(product,[u,v]);assert.equal(packed[0],3);assert.equal(packed[720*361],4);
    u[0]=NaN;assert.throws(()=>derive(product,[u,v]),/风场存在缺测/);u[0]=3;
  }
});

test('动力填色的分档、零值和两个严格越界的端点',async()=>{
  const {dynamicScale}=await import('../src/dynamic-scales.mjs');
  const grid={nx:8,ny:5,lonMin:-180,latMin:-90,dx:45,dy:45};
  for(const [id,variant,max,step] of [['div','default',15,3],['vertical','default',24,3],['vort','relative',18,2]]){
    const scale=dynamicScale(id,variant);
    assert.deepEqual(scale.breaks,Array.from({length:2*max/step+1},(_,i)=>i*step-max));
    assert.equal(new Set(scale.colors).size,scale.breaks.length+1);
    for(const [value,expected] of [[-max-1,scale.colors[0]],[-max,scale.colors[1]],[-0.1,scale.colors[max/step]],
      [0,null],[0.1,scale.colors[max/step+1]],[max,scale.colors.at(-2)],[max+1,scale.colors.at(-1)]]){
      const geo=fillBands(id,new Float32Array(40).fill(value),grid,{variant});
      assert.deepEqual(geo.features.map(f=>[f.properties.color,f.properties.scaleVersion]),expected?[[expected,2]]:[]);
    }
  }
  assert.equal(dynamicScale('vort','absolute'),null);
  const temp=new Float32Array([0]),w=new Float32Array([1]);
  assert.equal(derive({id:'vertical',level:500},[w,temp])[0],Math.fround(10*omega(1,0,500)));
});


test('CAPE 五档色标：500 以下不生成，2500 及以上共用端色',()=>{
  assert.equal(colorFor('cape',500),null);
  assert.ok(colorFor('cape',501));
  assert.notEqual(colorFor('cape',999),colorFor('cape',1000));
  assert.notEqual(colorFor('cape',1999),colorFor('cape',2000));
  assert.equal(colorFor('cape',2500),colorFor('cape',4000));
  assert.equal(colorFor('cape',2500),colorFor('cape',10000));
  const grid={nx:8,ny:5,lonMin:-180,latMin:-90,dx:45,dy:45};
  const geo=fillBands('cape',new Float32Array(40).fill(3000),grid);
  assert.ok(geo.features.length);
  assert.ok(geo.features.every(f=>f.properties.capeScaleVersion===3));
});

test('等值线坐标落在真实格点上，不偏半格',()=>{
  // 场值就是纬度本身，所以每条 10° 间隔的等值线应当精确落在 -80…80。
  const grid={nx:1440,ny:721,lonMin:-180,latMin:-90,dx:.25,dy:.25};
  const values=new Float64Array(grid.nx*grid.ny);
  for(let y=0;y<grid.ny;y++)for(let x=0;x<grid.nx;x++)values[y*grid.nx+x]=grid.latMin+y*grid.dy;
  const result=lineContours(values,grid,10);
  const lats=result.features.flatMap(f=>f.geometry.coordinates.map(p=>p[1]));
  assert.ok(lats.length>0);
  for(const lat of lats)assert.ok(Math.abs(lat/10-Math.round(lat/10))<1e-9,'纬度偏了半格：'+lat);
});
