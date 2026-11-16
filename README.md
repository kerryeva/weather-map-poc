# weather-map-poc
Generic weather map processing and performance experiments using public data.

完整预报覆盖起报后 0 至 96 小时，每 3 小时一张，共 33 个时次。普通时次提供 48 个产品，起报时刻降水不适用，提供 47 个。涡度仅保留相对涡度；CAPE 的 2000 J/kg 及以上共用一个色档。每桶容量上限为 9 GB，另计入 256 MB 预留空间。四天方案首次运行会在独占写入租约下清理旧方案批次，随后选择最新已齐备的起报。
