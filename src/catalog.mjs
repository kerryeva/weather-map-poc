// 独立的公开数据产品定义。此模块不读取浏览器插件、内部接口或用户资料。
export const GRID = {nx:1440, ny:721, lonMin:-180, latMin:-90, dx:.25, dy:.25};
export const DOMAIN = 'ecmwf_ifs025';
const bands = (breaks, colors) => ({breaks, colors});
export const STYLES = {
  pressure:{interval:2.5, color:'#925c28'},
  height:{interval:40, color:'#925c28'},
  temp:{interval:4, color:'#c43434'},
  wind:{},
  rh:bands([85,90,95,100], ['#b6dceb','#6bb2d2','#247eac']),
  dew:bands([0,2,4], ['#307fb2','#9ccfe2']),
  rain:bands([.1,1,2.5,5,10,25,50,100], ['#c0e5d5','#8bceaa','#4fad83','#219eac','#357db4','#6655ad','#a43c9b']),
  cape:bands(Array.from({length:37}, (_,i)=>500+i*100), Array.from({length:36}, (_,i)=>`rgb(${Math.round(211-i*3.8)},${Math.round(184-i*4.2)},${Math.round(232-i*2.4)})`)),
  vort:bands([-40,-20,-12,-8,-4,4,8,12,20,40], ['#187e9c','#339bbb','#69b6ca','#aed6df',null,'#fff0aa','#f4d364','#dfad31','#bb811b']),
  div:bands([-40,-20,-10,-5,-2,2,5,10,20,40], ['#24549e','#4383bb','#70a7cf','#b4d4e9',null,'#f9c0b7','#ec8c7e','#d85e52','#aa3037']),
  vertical:bands([-5,-2,-1,-.5,-.1,.1,.5,1,2,5], ['#155184','#2a7fac','#63a9c6','#b4dce6',null,'#f8c6bc','#ee978c','#d66770','#a23964'])
};

export function colorFor(id, value) {
  if (!Number.isFinite(value)) return null;
  const {breaks,colors} = STYLES[id];
  if (id==='cape' && value<=500 || id==='dew' && (value<0||value>4) || id==='rh' && (value<85||value>100)) return null;
  if (value<breaks[0]) return ['vort','div','vertical'].includes(id) ? colors[0] : null;
  for (let i=0;i<breaks.length-1;i++) if (value<breaks[i+1]) return colors[i];
  return colors.at(-1);
}

export function catalog(profile='full',model='IFS') {
  if (!['full','500hpa'].includes(profile)) throw Error('未知产品范围');
  if (!['IFS','GFS'].includes(model)) throw Error('未知预报模式');
  const result=[];
  for (const level of ['surface',1000,925,850,700,500,200]) {
    const ids=level==='surface' ? ['pressure','wind','temp','rh','rain','cape'] : ['height','wind','temp','dew','vort','div','vertical'];
    for (const id of ids) {
      if (profile==='500hpa' && !(level===500 && id==='height')) continue;
      const suffix=level==='surface'?'2m':`${level}hPa`, wind=level==='surface'?'10m':suffix;
      const inputs={pressure:['pressure_msl'],height:[`geopotential_height_${suffix}`],wind:[`wind_u_component_${wind}`,`wind_v_component_${wind}`],temp:[`temperature_${suffix}`],rh:['relative_humidity_2m'],rain:['precipitation'],cape:['cape'],dew:[`temperature_${suffix}`,`relative_humidity_${suffix}`],vort:[`wind_u_component_${wind}`,`wind_v_component_${wind}`],div:[`wind_u_component_${wind}`,`wind_v_component_${wind}`],vertical:[`vertical_velocity_${suffix}`,`temperature_${suffix}`]}[id];
      for (const variant of id==='vort'?['absolute','relative']:['default']) {
        result.push({key:`${model}-${level}-${id}-${variant}`,level,id,variant,inputs,kind:id==='wind'?'wind':STYLES[id].interval?'line':'fill'});
      }
    }
  }
  return result;
}

export const fieldsFor = profile => [...new Set(catalog(profile).flatMap(p=>p.inputs))].sort();

export function fieldSources(profile='full',model='IFS') {
  const fields=new Map();
  for(const product of catalog(profile,model)) {
    const domain=model==='IFS'?'ecmwf_ifs025':product.level==='surface'&&!['pressure','cape'].includes(product.id)?'ncep_gfs013':'ncep_gfs025';
    for(const variable of product.inputs)fields.set(variable,{variable,domain});
  }
  return [...fields.values()].sort((a,b)=>a.variable.localeCompare(b.variable));
}
