# Open weather map batch test

独立的 IFS 公开格点处理与容量测试程序，不包含浏览器插件代码、内部接口、用户资料或密钥。

当前为测试候选，不能直接视为生产地图服务。包含48种基础产品、6层额外相对涡度，共54个组合，去重读取43个原生变量。500 hPa高度场可作为快速子集。降水/CAPE参数只用于候选容量评估，不代表业务质量已经验收。

## 运行

Node 26、Python 3.12：

```sh
npm ci --ignore-scripts --no-audit --no-fund
python -m venv .venv
.venv/bin/pip install --no-cache-dir -r requirements.txt
npm test
.venv/bin/python -m unittest discover -s tests -p 'test_*.py' -v
```

先检查冻结起报目录和41个时次，不下载完整格点：

```sh
.venv/bin/python scripts/batch.py --work /tmp/ifs-work --report /tmp/ifs-evidence/plan.json --plan-only
```

完整单批资源测试（逐时次生成并校验；记录成品体积后删除，不向任何云存储上传）：

```sh
.venv/bin/python scripts/batch.py --work /tmp/ifs-work --report /tmp/ifs-evidence/run.json --mode discard
```

小范围本地发布、恢复与清理测试：

```sh
.venv/bin/python scripts/batch.py --work /tmp/ifs-smoke --report /tmp/ifs-evidence/smoke.json --mode local --output /tmp/ifs-local-store --profile 500hpa --leads 0,3
```

`--leads all` 为0至120小时、每3小时一次，共41时次。起报时刻的降水不具有前一累计时段，明确标记不适用，不伪造零值。传入 `--metadata 已保存的目录.json` 可固定源起报重跑。最新目录必须已经完成；缺变量、缺时次、全场缺测或几何校验失败都会停止。

成品为每产品一个 PMTiles，风场为0.5度北至南排列、先u后v的Float32 gzip。原始OM、43个展开变量、几何和单片MVT只保留当前时次；每个产品打包校验后即清理其几何。只在整批所有时次完成后更新 `latest.json`。

## 保险丝和边界

- 只操作 `ifs-test-v1/` 测试前缀。其他文件计入目标桶容量，但不会被清理。未知归属或异常批次名立即停止。
- 已有超过两批：立即停止，不通过自动删数据掩盖异常。新一批开始前清理非最新的旧测试批次，始终保留最近一批完整资料。
- 存储安全上限为8,000,000,000字节，另保留256,000,000字节余量。每时次生成完成后只集中获取一次对象列表，按该时次所有成品总量预留空间。每文件上传仅更新本地整数计数，不逐文件远端扫描。列表读取失败、清理失败、预留不足均停止上传。
- 同一时间仅一个生产者：GitHub并发组和7小时对象租约；任务内部最多运行约5小时，单时次处理最多20分钟。意外终止时，未释放租约会暂时阻止其他任务。
- 每月本程序最多预留80万次A类操作；按SDK最多两次尝试保守预留。这个预算不会计算其他程序产生的操作，也不会封顶终端读取的B类操作。
- 临时目录抽样上限8 GB、磁盘最低保留2 GB，按两秒采样；触发时停止处理子进程。采样有短暂延迟，因此不是操作系统磁盘配额；真实峰值和可用空间仍须Linux实测。
- **容量检查覆盖一个目标桶，不覆盖同一账户的其他桶。使用前需确认这个账户的其他R2用量；不能据本程序承诺整个账户绝不收费。**外部读请求仍会计入B类操作，停止生产不等于停止所有收费来源。
- 月度免费存储按每日峰值平均计算，短期账单为零不能证明整月免费。预算提醒不具有硬停机效果。

## 私有R2发布

`--mode r2` 需要环境变量 `R2_ACCOUNT_ID`、`R2_BUCKET`、`AWS_ACCESS_KEY_ID`、`AWS_SECRET_ACCESS_KEY`。只使用测试桶的对象读写权限；包含本程序旧批次删除能力，不需要其他桶或账户管理权限。不得把凭据写入源码、报告、仓库或聊天。

先在本地/Actions的discard模式验证完整41时次，再单独授权R2上传。凭据只通过GitHub Secrets配置；账户和桶放入Variables。当前工作流无定时触发，不会自动开始连续生产。

工作流只使用公开仓库标准 `ubuntu-24.04`，没有大型运行器、气象数据Artifact或Actions缓存；小型报告压缩后放入作业日志。

首次推送 `ifs-batch-test` 分支的 `.github/workflows/ifs-measure.yml` 会冻结一份完整起报目录，将41个时次分为独立任务，最多4个并行，全部使用discard模式。这样不会把所有时次临时文件堆在一块磁盘上，也不依赖一个长达数小时的单任务。每个时次任务有30分钟上限，内部处理子进程20分钟上限；任何失败均保留报告，不通过减少产品或降低质量掩盖。

另一个手动工作流 `ifs-batch.yml` 提供顺序处理和私有R2发布候选。Linux实测完成后还须确认整批耗时是否适合单任务；未通过前不得启用七天连续生产。R2模式必须先配置上述Secrets/Variables，尚未与并行资源测试合并。

## 来源与许可

- 数据：https://github.com/open-meteo/open-data ，Open-Meteo / ECMWF，CC BY 4.0。空间源文件仅短期保留，须及时测试或自行保存样本。
- 原始变量说明：https://github.com/open-meteo/open-meteo/tree/main/Sources/App/Ecmwf
- 通用调度及地图处理源码为MIT，见LICENSE；独立OM读取工具 `scripts/extract.py` 为GPL-2.0-only，见COPYING-GPL-2.0。主程序通过子进程调用读取工具，交换普通Float32文件，不导入GPL库到使用Apache-2.0 SDK的调度进程。各第三方库保留原许可，本仓库不打包分发其二进制。
- 完整固定版本及依赖许可证报告见 `THIRD_PARTY.md`。
- GitHub规格：https://docs.github.com/en/actions/reference/runners/github-hosted-runners
- R2计费：https://developers.cloudflare.com/r2/pricing/
- R2预算提醒：https://developers.cloudflare.com/billing/manage/budget-alerts/

成功条件：完整批次所有适用产品通过校验；磁盘保留量不被侵占；存储维持两批以内且低于安全阈值；月度读写量外推保留余量。该容量测试不替代私有R2读取、真实终端速度、跨瓦片平滑和业务看图验收。
