# 城市设施多来源属性实例的持续接收

设施属性接收器。目录适配器已支持单实例属性，中心采用规则存于 policy.json。来源是 NGSI-LD 网关的受限历史投影。接收器现在同时经营两条通道：`/temporal/:entity` 属性历史补采与 `/changes` 实时变更，并按实例合并，不互相清空。

## 运行

Node.js 22.23.4，依赖仅为标准库，package-lock.json 不含网络依赖。无需安装数据库或访问外站。

```sh
npm test
npm run demo
npm start
```

demo 从空临时目录启动来源与查询服务，按真实交互跑两轮接收，打印当前采用、实例历史、未采全区间和原始收件数后退出，通常不到 5 秒。持续运行时来源监听 4010，查询服务监听 4020，按 `INTERVAL_MS`（默认 2000）循环采集。

```sh
docker build --network=none -t temporal-context .
docker run --rm --network=none temporal-context npm test
docker run --rm --network=none temporal-context npm run demo
```

Dockerfile 固定 Node 镜像摘要。首次使用需预载该镜像，运行和测试不下载任何内容。现场演示可执行 npm run demo，在90秒内完成启动、实际收件与查询。

## 查询

- `GET /records`：已收原件，保留兼容。历史行、变更行都按实际内容去重。
- `GET /temporal`：全部实体的当前采用结果、采纳原因、候选、覆盖情况与最近领取状态。
- `GET /temporal/:entity`：单个实体的当前状态。可用 `?at=<number>` 指定投影时刻，默认取已观察到的最新有效时刻。
- `GET /temporal/:entity/:attribute`：单个属性的当前值、采纳原因、未采用候选、`lastKnown`、覆盖区间和实例清单。
- `GET /temporal/:entity/history`：每个实例的取值、生效/失效、来源、领取渠道、首次看到时间与修订记录。
- `GET /temporal/:entity/coverage`：尚未采全的时间段（`complete` 为来源断言完整的区间，`gaps` 为其补集），以及尚未核验的删除。

当前值按 `policy.json` 的 `sourceOrder` 选取覆盖该时刻的最高优先级来源；观测时间较新不会提升来源优先级。输出里给出每个候选的 `outcome`（选中/未选中及规则），便于回答“为什么采用它”。

## 采用与合并语义

- 实例身份为 entity、attribute、datasetId、instanceId；默认实例与命名实例独立。
- 历史补采只增不删：快照没提到的实例（例如补采窗口之后才出现的实时实例）不会被移除，单个设施补采失败也不会清空该设施。
- `deleteInstance` 只关闭目标实例，且只在通知的 `at` 之后生效；同一设施的其他数据集实例不受影响。通知早于实例首次出现时记为“未核验删除”，提示尚未拉到该实例，而不会生成一个已删除的幻影实例。
- `/changes` 的游标与消息合并原子写入；页面失败不推进游标，重复领取按来源与有效起点幂等合并。格式错误的消息进入 `quarantine`，不会变成业务值。
- `validFrom`/`validTo` 为半开区间。`complete` 只是来源的覆盖断言，`gaps` 表示尚未采全，不能据此推断删除。

## 工程结构

- src/reader.mjs 当前目录适配器；不修改来源来迎合适配结果。
- src/history.mjs 解析属性历史投影，只做校验与归一，不决定业务值。
- src/sync.mjs 一轮采集：列目录、历史补采、游标增量领取，失败隔离并记录。
- src/store.mjs 实例观测、删除证据、游标、隔离区与覆盖的持久化，原子写入、可重放。
- src/project.mjs 业务投影：按 `sourceOrder` 采用当前值，给出候选原因、来源与未采全区间。
- src/api.mjs 查询接口。
- src/source.mjs 与 src/behavior.mjs 实现本地来源网关，契约见 contracts.md。
- src/archive.mjs 保存来源与内容摘要；同内容重收不新增，内容变化保留版本。
- fixtures/source.json 是虚构来源资料和状态变化，不含真实账号、交易或个人数据。

环境变量 DATA_DIR、PORT、SOURCE_PORT、INTERVAL_MS、POLICY_FILE、SCENARIO 控制目录、端口、采集间隔、采用规则与来源资料版本。测试和演示自己创建空目录，不依赖上次执行遗留状态；`.state` 可删除以恢复空收件箱。
