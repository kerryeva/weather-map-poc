import {test} from 'node:test';
import assert from 'node:assert/strict';
import {catalog,fieldSources} from '../src/catalog.mjs';
import {regrid,GFS013_GRID} from '../src/regrid.mjs';

test('双模式48产品与43字段，GFS地面5变量单独路由',()=>{
  for(const model of ['IFS','GFS']) {
    const products=catalog('full',model),fields=fieldSources('full',model);
    assert.equal(products.length,48);assert.equal(fields.length,43);
    assert.ok(products.every(p=>p.key.startsWith(model+'-')));
    assert.equal(new Set(products.map(p=>p.key)).size,48);
    assert.deepEqual(catalog('500hpa',model).map(p=>p.key),[model+'-500-height-default']);
    if(model==='GFS') {
      assert.deepEqual(fields.filter(f=>f.domain==='ncep_gfs013').map(f=>f.variable).sort(),
        ['precipitation','relative_humidity_2m','temperature_2m','wind_u_component_10m','wind_v_component_10m']);
      assert.equal(fields.filter(f=>f.domain==='ncep_gfs025').length,38);
    }else assert.ok(fields.every(f=>f.domain==='ecmwf_ifs025'));
  }
  assert.throws(()=>catalog('full','other'));
});

test('双线性插值保留解析纬度线性场，处理经度接缝及极点夹取',()=>{
  const grid={nx:4,ny:3,lonMin:-180,latMin:-60,dx:90,dy:60};
  const values=Float32Array.from({length:12},(_,i)=>Math.floor(i/4)*60-60);
  const target={nx:8,ny:7,lonMin:-180,latMin:-90,dx:45,dy:30};
  const output=regrid(values,grid,target);
  for(let y=0;y<7;y++)for(let x=0;x<8;x++)assert.equal(output[y*8+x],Math.max(-60,Math.min(60,-90+30*y)));
  const seam=regrid(new Float32Array([0,10,20,30,0,10,20,30]),{...grid,ny:2},
    {nx:1,ny:1,lonMin:135,latMin:-60,dx:1,dy:1});
  assert.equal(seam[0],15);
  assert.throws(()=>regrid(new Float32Array(1),grid,target));
  assert.equal(GFS013_GRID.latMin,-89.912126125);
});

test('缺测不能被重采样变成零',()=>{
  const grid={nx:4,ny:2,lonMin:-180,latMin:-60,dx:90,dy:60};
  const values=new Float32Array(8).fill(5);values[0]=NaN;
  assert.ok(Number.isNaN(regrid(values,grid,{nx:1,ny:1,lonMin:-135,latMin:-30,dx:1,dy:1})[0]));
});
