// 格点与色标使用同一显示单位。colors 首尾为严格低于、严格高于端点的色带。
const ticks = (limit, step) => Array.from({length:2*limit/step+1}, (_,i)=>i*step-limit);
const divergence = {breaks:ticks(15,3), unit:'10⁻⁵ s⁻¹', colors:[
 '#173f80','#285b9a','#467eb1','#70a1c8','#9bc2dc','#d4e6ef',
 '#f9ddd2','#f4bda6','#e9977d','#d26c59','#af453e','#872b33']};
const vertical = {breaks:ticks(24,3), unit:'0.1 Pa/s', colors:[
 '#802039','#9e2e37','#ba4040','#cf5650','#de7260','#eb9074','#f2aa8e','#f7c5a8','#fbe1cc',
 '#dceff2','#bee0e9','#9acedd','#76b8d1','#579fc2','#3e85af','#2a6c99','#195480','#103c67']};
const relative = {breaks:ticks(18,2), unit:'10⁻⁵ s⁻¹', colors:[
 '#075468','#106a80','#1e7e94','#3591a4','#51a3b2','#70b5c0','#90c6cd','#afd6d9','#cce4e4','#e2eeee',
 '#fff1cb','#fbe5ab','#f7d887','#f1c768','#e9b44f','#dca03c','#c88b2e','#ad7424','#915e1d','#744918']};

export function dynamicScale(id,variant){
 if(id==='vort')return variant==='relative'?relative:null;
 if(id==='div')return divergence;
 return id==='vertical'?vertical:null;
}

export function dynamicColor(scale,value){
 if(!Number.isFinite(value)||value===0)return null;
 const {breaks,colors}=scale;
 if(value>breaks.at(-1))return colors.at(-1);
 const index=breaks.findIndex(edge=>value<edge);
 return index<0?colors.at(-2):colors[index];
}

export function dynamicThresholds(scale,min,max){
 const edges=[...scale.breaks],last=edges.at(-1);
 // D3 等值面包含阈值。单独留出零值，正端点归入内侧，超出才用端点色。
 edges.splice(edges.indexOf(0)+1,0,2**-149); // Float32 最小正值
 edges[edges.length-1]=last+2**(Math.floor(Math.log2(last))-52);
 if(min<edges[0])edges.unshift(min);
 return edges.filter(edge=>edge<=max);
}
