import assert from 'node:assert/strict';
import test from 'node:test';
import {rainDisplayGrid} from '../src/rain.mjs';

const grid = {nx: 16, ny: 9, dx: 22.5, dy: 22.5, lonMin: -180, latMin: -90};
const center = 4 * grid.nx + 8;

test('孤立的起填量减弱，连续的起填量仍可着色', () => {
  const trace = new Float32Array(grid.nx * grid.ny);
  trace[center] = 0.1;
  const isolated = rainDisplayGrid(trace, grid);
  const at = 8 * isolated.grid.nx + 16;
  assert.ok(isolated.field[at] < 0.1);
  trace.fill(0.1);
  assert.ok(rainDisplayGrid(trace, grid).field.every(v => v >= 0.1));
});

test('经度接缝、缺测和各较高雨量等级在绘图加密后保持一致', () => {
  const values = new Float32Array(grid.nx * grid.ny);
  for (const amount of [1, 2.5, 5, 10, 25, 50, 100]) {
    values.fill(0);
    values[4 * grid.nx] = amount;
    values[center] = NaN;
    const {field,grid: refined} = rainDisplayGrid(values, grid);
    const row = 8 * refined.nx;
    assert.equal(field[row], amount);
    assert.equal(field[row + 1], field[row + refined.nx - 1]);
    assert.ok(Number.isNaN(field[row + 16]));
    assert.ok(field.every(v => Number.isNaN(v) || (v >= 0 && v <= amount)));
  }
});
