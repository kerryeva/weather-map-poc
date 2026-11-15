// 降水显示场。仅最低色阶参与弱平滑，较高色阶的原格点保留预报量。
import {smoothField} from './smoothing.mjs';
import {STYLES} from './catalog.mjs';

export const RAIN_TRACE_SIGMA = 0.6;

function slope(left, right) {
  return left * right > 0 ? 2 * left * right / (left + right) : 0;
}

// 单调三次插值只增加绘制边界所需的采样点，避免制造新的峰值。
function midpoint(previous, a, b, next) {
  if (!Number.isFinite(a) || !Number.isFinite(b)) return NaN;
  const delta = b - a;
  return (a + b) / 2 + (slope(a - previous, delta) - slope(delta, next - b)) / 8;
}

export function rainDisplayGrid(values, grid) {
  const {nx, ny} = grid;
  const ceiling = STYLES.rain.breaks[1];
  if (values.some(v => v === Infinity || v === -Infinity)) throw Error('降水格点含无穷值');
  const weak = smoothField(Float32Array.from(values, v => Math.min(v, ceiling)), grid, RAIN_TRACE_SIGMA);
  for (let i = 0; i < weak.length; i++) {
    // 也避免弱降水因 Float32 舍入进入更高色阶。
    if (values[i] >= ceiling || weak[i] >= ceiling) weak[i] = values[i];
  }
  const width = nx * 2, height = ny * 2 - 1;
  const field = new Float32Array(width * height);
  for (let y = 0; y < ny; y++) {
    const row = weak.subarray(y * nx, (y + 1) * nx);
    const start = 2 * y * width;
    for (let x = 0; x < nx; x++) {
      field[start + 2 * x] = row[x];
      field[start + 2 * x + 1] = midpoint(row[(x + nx - 1) % nx], row[x], row[(x + 1) % nx], row[(x + 2) % nx]);
    }
  }
  for (let y = 0; y < ny - 1; y++) {
    const start = 2 * y * width, end = start + 2 * width;
    const previous = Math.max(0, start - 2 * width), next = Math.min((height - 1) * width, end + 2 * width);
    for (let x = 0; x < width; x++) {
      field[start + width + x] = midpoint(field[previous + x], field[start + x], field[end + x], field[next + x]);
    }
  }
  return {field, grid: {...grid, nx: width, ny: height, dx: grid.dx / 2, dy: grid.dy / 2}};
}
