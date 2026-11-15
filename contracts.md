# 来源网关契约

GET /entities 返回当前设施；GET /temporal/:entity 返回属性实例及区间 complete；GET /changes?after=... 返回 upsert/deleteInstance。实例身份为 entity、attribute、datasetId、instanceId；不带 datasetId 的默认实例与命名实例独立。validFrom/validTo 为半开区间；policy.json 中 sourceOrder 只决定当前采用顺序。

## 本地来源操作

默认场景 normal，其他场景为 overlap。设置 SCENARIO 后启动可固定一个变体。POST /__scenario 的 name 切换来源场景并清空来源会话状态，供开发人员在测试中模拟外部变化；采集适配器不调用此控制入口。GET /__audit 返回所有真实来源请求、当前场景与会话状态。除明确列出的状态变化外，GET 可重复读取，消息可能重复领取。

来源路由的 requires 表示通信前提，sets 是来源端会话状态转移。源端拒绝返回对应HTTP状态和JSON原因，适配器不能把错误响应当业务内容。所有URL限定本地来源，未知资源404；fixture中的静态资料与 src/behavior.mjs 的状态处理共同构成来源行为。接口为授权网关的有限资料投影，不宣称覆盖协议全部版本或完整线路栈。

## 收存约定

当前适配器 collect(client) 返回带字符串 id 的原件行。Archive.put(rows, provenance) 按实际内容和来源生成摘要，保留不同内容。该原件归档不自动决定业务当前值、引用已解析、区间完整或远端已确认；业务投影与领取状态需要各自维护。GET /records 保留兼容，不将未知状态伪造为成功。

## 历史通道与业务投影

`GET /temporal/:entity` 的 `attributes` 是属性实例观测，`complete` 是该次投影断言完整的时间区间。两者都只是资料：适配器按实例身份合并到自己的存储，不把 `complete` 当成实例存在性，也不把快照缺席当成删除。历史读取失败按设施记录并保留既有实例。

`GET /changes` 的 `upsert` 与 `deleteInstance` 按消息处理。游标 `next` 与消息合并一次性持久化，因此页面失败可重试、重复消息可幂等重放。`deleteInstance` 只针对同一 entity、attribute、datasetId、instanceId 的实例；收到通知时尚未见过的实例记为未核验删除，等该实例被拉到时再应用。

业务当前值由 policy.json 的 `sourceOrder` 决定：在覆盖投影时刻的实例中取顺序最靠前的来源，`validFrom` 较新不会改变来源优先级；同来源再比较 `validFrom`，最后以实例身份确定性排序。`sourceOrder` 未列出的来源排在其后，不会因观测较新而胜出。投影输出的 `attribution` 记录规则、次序、被拒候选与原因，`provenance` 记录渠道、来源地址、首次/最近领取时间与修订；`coverage.gaps` 是尚未采全的区间。
