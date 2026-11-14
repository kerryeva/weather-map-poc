# Open weather map batch test

独立的 IFS / GFS 公开格点处理与容量测试程序，不包含浏览器插件代码、内部接口、用户资料或密钥。

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

成品为每产品一个 PMTiles，风场为0.5度北至南排列、先u后v的Float32 gzip。原始OM、43个展开变量、几何和单片MVT只保留当前时次；每个产品打包校验后即清理其几何。

每个时次的全部成品校验并上传后，提交该时次清单并更新 `available.json`，使已完成时次可提前读取。未完成时次不列入可用目录，保留最近完整起报的回退入口。整批所有时次完成后才更新完整批次指针 `latest.json`，并将可用目录标记完成。读取端必须识别可用目录，才能实现边生成边使用；当前未接入任何生产插件。

报告分别记录源下载、提取、产品计算/平滑/几何/切片/打包、上传前校验、集中空间检查、上传、目录发布和清理耗时；`time_until_ready_seconds` 是一个时次实际可读取前的时间。discard模式未上传，不能把其计算时间当作R2端到端可用时间。

## 所有产品的几何与数值检查

当前目录的14个等值线、33个填色和7个风场均进入测试范围。气压/高度/温度共用等值线生成器，在瓦片裁切前清除连续重复坐标和完全零长的片段；保留真实短线，不跨片段拼接，处理数量写入报告。填色共用源几何和瓦片整数网格检查，记录退化为空的面、零面积环、退化线段及共同边界重建；无法确认修复有效时停止。风场检查有限值、网格排列及压缩回读，不能以零风代替缺测。

回归按产品目录自动枚举：689组源几何情形覆盖等值线临界值和真实短线，以及全部填色分级阈值的常量场、孤立临界格点和局部缺测；另验证整数瓦片退化与7个风场缺测拒绝。新增产品必须补齐目录约束和测试。测试覆盖不等于未来数据绝无异常；仍须逐产品质量检查、完整起报测试和真实地图平滑/接缝验收。

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

修改 `ifs-batch-test` 分支的 `.github/workflows/ifs-measure.yml` 会运行完整41时次资源测试。修复复测固定使用 `benchmarks/ifs-20261113T18Z.json` 中的公开起报，与首轮保持相同输入；源文件若已过期就停止，不能自动换起报。手动批处理的最新起报模式不受影响。

资源测试最多4个时次任务并行，每个任务使用4个产品处理进程，全部使用discard模式。这样不会把所有时次临时文件堆在一块磁盘上，也不依赖一个长达数小时的单任务。每个时次任务有30分钟上限，内部处理子进程20分钟上限；任何失败均保留报告，不通过减少产品或降低质量掩盖。该资源任务没有R2凭据，失败时只保留处理日志末尾12KB以定位具体产品；不将这一原始日志步骤用于R2发布工作流。

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


## 双模式一次性资源测试候选（2026-11-14）

本候选新增一个仅支持discard的入口 scripts/measure_models.py，不读取R2凭据，不上传成品。IFS/GFS固定2026-11-14 00UTC，来自三个已完成公开空间目录，覆盖各41个0至120小时、每3小时预报时次，各54产品组合；起报降水明确不适用。新工作流82个时次任务交错，总并发最多4个标准ubuntu-24.04任务，每任务最多4个产品进程，无schedule、Actions缓存、Artifact或付费大型运行器。

GFS 38字段来自ncep_gfs025；地面温度、湿度、10米U/V和降水5字段来自ncep_gfs013。013原生3072×1536网格按上游坐标做双线性重采样到1440×721，包含在计时中。源OM提取后删除，原生字段重采样后删除；每一时次全部生成、校验、统计后删除成品。不在本机长期保存源数据或成品。

雨量遵循当前插件所选源：IFS原生3小时、GFS在0至120小时的原生1小时；每3小时抽帧不等于3小时累计。当前测试不擅自合并GFS三帧，也不能直接将两模式雨量数值比较为同一累计时段。F000不生成降水产品。

命令示例：
```sh
python scripts/measure_models.py --model GFS --metadata benchmarks/dual-20261114T00Z.json --work /tmp/gfs-work --report /tmp/gfs-evidence/run.json --leads 3 --product-workers 4
```

测试通过要求：82个时次任务全部成功；全部产品齐全、原生变量和网格检查通过、源几何/瓦片/打包/哈希检查通过，无保险丝中止；得到两模式分别及合计的成品容量与耗时。F000每模式53文件，其余每时次54文件，预期合计4426文件。对外报告总等待时间、处理时间合计、分阶段耗时、源下载量和临时目录抽样峰值。临时目录峰值不是总RAM峰值。不得把不同起报的旧IFS时间直接当作精确配对加速对照。

本测试不代表R2实际存储或计费、不代表真实浏览器冷热缓存/视觉效果，也不证明生产EWINS可适配。源码基于此前批准的公开167a6c0版本制作，未混入本机尚未公开的滚动35日R2预算、STANDARD改动或本机凭据工具。原有R2工作流不在本次授权运行范围。未来R2整批测试应另行采用修正过的预算版本。

上游依据：
- https://github.com/open-meteo/open-data （空间布局、7日保留、CC BY 4.0）
- https://github.com/open-meteo/open-meteo/blob/main/Sources/App/Gfs/GfsDomain.swift （网格坐标、0至120小时发布步长）
- https://github.com/open-meteo/open-meteo/blob/main/Sources/App/Gfs/GfsVariable.swift （变量、单位、向后累计定义）
