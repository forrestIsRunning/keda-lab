# 一笔订单怎样改掉 Deployment 的副本数

这篇笔记跟着一笔订单走完。订单被 `LPUSH` 进 Docker 里的 Redis List `orders`。KEDA 不读订单内容，只看这个 list 的长度。长度大于 0 时，`keda-operator` 把 Deployment `order-worker` 的副本从 0 写成 1。随后 kube-controller-manager 里的 HPA 控制器按外部指标把副本写成 4。消费者自己 `BLPOP`。队列空了并过完冷却后，operator 把副本从 4 写成 0。

这次写入怎样经过认证、准入并进入 etcd，见 [kube-apiserver 如何接住一次写入](../kubernetes-lab/kube-apiserver/request-path.md)。HPA 和 Deployment 控制器都在 kube-controller-manager 里，循环形状见 [kube-controller-manager 如何把期望写回 API](../kubernetes-lab/kube-controller-manager/watch-and-write.md)。选节点见 [kube-scheduler 调度逻辑与扩展](../kubernetes-lab/kube-scheduler/logic-and-extensions.md)。节点上怎么把 Pod 跑起来，见 [kubelet 在节点上同步 Pod](../kubernetes-lab/kubelet/pod-sync.md)。Service 和 EndpointSlice 怎么变成转发规则，见 [kube-proxy 如何把 Service 变成转发规则](../kubernetes-lab/kube-proxy/service-rules.md)。

KEDA 源码基线是 tag `v2.21.0`，commit `626ded5d783`（2026-09-23）。HPA 控制器的行为对照本地 `kubernetes` 检出 `f322464da46`，和 kubernetes-lab 同一棵树。跑实验的集群是 kind `k8s-lab`，服务端 v1.36.1。KEDA 2.21 声明兼容 1.34–1.36。集群上看到的 `ScalingDisabled` 文案和这棵树里的字符串一致。

## 本地这一次

Redis 是 Docker 容器 `keda-redis`，镜像 `redis:7-alpine`，接在 kind 的 docker 网络上，地址 `172.20.0.4`。宿主机端口是 `16379`。集群里用一个没有 selector 的 Service `redis.keda-redis-demo.svc.cluster.local:6379`，Endpoints 指向这个地址。kube-proxy 消费的是对应的 EndpointSlice。

KEDA 用 Helm chart `kedacore/keda` 2.21.0 装在命名空间 `keda`。三个 Deployment 都在 `k8s-lab-worker` 上：

| Deployment | 镜像 | 端口 |
| --- | --- | --- |
| `keda-operator` | `ghcr.io/kedacore/keda:2.21.0` | Service `keda-operator` 的 9666 |
| `keda-operator-metrics-apiserver` | `ghcr.io/kedacore/keda-metrics-apiserver:2.21.0` | 443、8080 |
| `keda-admission-webhooks` | `ghcr.io/kedacore/keda-admission-webhooks:2.21.0` | 443 |

Lease `operator.keda.sh` 的持有者是当时的 operator Pod。`--leader-election-id` 的默认值就是这个名字。

ScaledObject 的参数是 `minReplicaCount: 0`、`maxReplicaCount: 4`、`listLength: "2"`、`activationListLength: "0"`、`pollingInterval: 5`、`cooldownPeriod: 15`、`initialCooldownPeriod: 0`。消费者每 `BLPOP` 到一条就睡 20 秒，避免队列在 HPA 采样前被吃空。

2026-10-10 08:02:22（+0800）向空队列 `LPUSH` 了 `order-1` 到 `order-8`。下面的时间是同一时区。

| 时刻 | 副本 spec | 就绪 | 队列长度 | ScaledObject Active | 这一拍谁写了 scale |
| --- | --- | --- | --- | --- | --- |
| 08:02:22 | 0 | 0 | 8，刚写入 | `ScalerNotActive` | 还没轮询到 |
| 08:02:26 | 1 |  |  |  | operator，`Successfully updated ScaleTarget`，0 到 1 |
| 08:02:28 | 1 | 1 | 7 | `ScalerActive` | 第一个 Pod 已经取走一条 |
| 08:02:33 | 4 | 4 | 4 | `ScalerActive` | HPA `SuccessfulRescale`，新大小 4 |
| 08:02:59 | 4 | 4 | 0 | `ScalerCooldown` | HPA 仍报 desired 4 |
| 08:03:06 | 0 |  | 0 |  | operator，`minReplicaCount`，4 到 0 |
| 08:03:40 | 0 | 0 | 0 | `ScalerNotActive` | HPA `ScalingActive=False`，原因 `ScalingDisabled` |

四个 Pod 的日志把 `order-1` 到 `order-8` 都打出来了。每个 Pod 两条。缩到 0 时 kubelet 发了 `Killing`。

## 三个进程各看一段

![operator 轮询 LLEN，自己把副本从 0 写成 1；HPA 经外部指标写成 N。Redis 在 kind 集群外面。](./assets/keda-architecture.svg)

`keda-operator` 是 controller-runtime 的控制器进程。选主成功之后它 reconcile `ScaledObject`，创建并持有 HPA，再按 `pollingInterval` 跑 scale loop。同一个进程在 `:9666` 起 gRPC Metrics Service。`pkg/metricsservice/server.go` 的 `NeedLeaderElection` 返回 true，所以只有 leader 听这个端口。副本数是 1 时，Service `keda-operator` 后面就是这个 Pod。

`keda-operator-metrics-apiserver` 注册 APIService `v1beta1.external.metrics.k8s.io`。HPA 向 apiserver 要外部指标时，聚合层把请求转到这个 Pod。它自己不连 Redis。`cmd/adapter/main.go` 里默认的 gRPC 地址是 `keda-operator.<namespace>.svc.cluster.local:9666`。`pkg/provider/provider.go` 的 `GetExternalMetric` 用标签 `scaledobject.keda.sh/name` 找出 ScaledObject，再调用 operator 的 `GetMetrics`。

`keda-admission-webhooks` 给 ValidatingWebhookConfiguration `keda-admission` 提供 `vscaledobject.kb.io` 等 webhook。这次 `kubectl apply` 的 ScaledObject 通过了它，随后才进入 etcd。

Redis scaler 是 operator 进程里的一个 Go 对象，带一个 `go-redis` 客户端。scale loop 和 gRPC `GetMetrics` 都调用这份缓存里的同一个 scaler。

安装还创建了这些 CRD：`scaledobjects.keda.sh`、`scaledjobs.keda.sh`、`triggerauthentications.keda.sh`、`clustertriggerauthentications.keda.sh`，以及 eventing 用的 CloudEventSource。这次只用了 `ScaledObject`。没有密码，所以没有 `TriggerAuthentication`。

## 订单只进 Redis

业务写入是：

```bash
redis-cli LPUSH orders order-1
```

消费者是 Deployment 里的 `redis:7-alpine`，循环 `BLPOP orders 5`，取到就睡 20 秒。KEDA 不在这条连接上。它不代理消息，也不知道订单正文。

Redis scaler 在 `NewRedisScaler` 里放了一段 Lua。key 不存在或类型是 list 时执行 `LLEN`。`GetMetricsAndActivity` 把这个长度交回去，激活条件是 `listLen > activationListLength`。这次 `activationListLength` 是 0，所以长度至少为 1 才算活跃。长度等于 0 不算。

指标名在 `GetMetricSpecForScaling` 里拼出来：`s<触发器下标>-redis-<listName>`。集群里的 HPA 写的是 `s0-redis-orders`。默认指标类型是 `AverageValue`，目标值是 `listLength`，这次是 2。

## ScaledObject 写成 HPA

`kubectl apply` 进 apiserver。webhook `vscaledobject.kb.io` 校验通过后，对象进 etcd。operator 的 `Reconcile` 看到它：

1. 盖上 finalizer `finalizer.keda.sh`。日志里同时有一句提示，希望 finalizer 带域名路径。
2. 确认 `scaleTargetRef` 指向的 Deployment 有 `/scale` 子资源。日志是 `Detected resource targeted for scaling`，资源 `apps/v1.Deployment`，名字 `order-worker`。
3. `Creating a new HPA`，名字 `keda-hpa-order-worker`。
4. 启动 scale loop。事件是 `Scaler redis is built` 和 `Started scalers watch`。

生成的 HPA 带 ownerReference，控制器是这个 ScaledObject。`GetHPAMinReplicas` 在 `minReplicaCount` 为 0 或未设置时返回常量 `defaultHPAMinReplicas`，值是 1。HPA 的 `minReplicas` 不能拿 ScaledObject 的 0 去填。集群上的 spec 是：

| 字段 | 值 |
| --- | --- |
| `scaleTargetRef` | `apps/v1` Deployment `order-worker` |
| `minReplicas` | 1 |
| `maxReplicas` | 4 |
| 指标 | External `s0-redis-orders`，`AverageValue` `"2"` |
| 选择器 | `scaledobject.keda.sh/name=order-worker` |

空队列时 ScaledObject 的条件是 `Ready=True`、`Active=False`（`ScalerNotActive`）、`Paused=False`。HPA 的 `ScalingActive=False`，原因 `ScalingDisabled`，文案是 `scaling is disabled since the replica count of the target is zero`。ScaledObject 把这个原因记进 `HPAActive=True`。`getHPAHealth` 看到 HPA 的 `ScalingDisabled` 时，把它当成缩到 0 之后的正常空闲，条件状态仍是 True。

刚创建 Service 的那一秒，EndpointSlice 已经在，kube-proxy 的规则还没跟上。operator 日志是 `dial tcp 10.96.206.94:6379: connect: connection refused`，探测 Pod 也是 connection refused。下一轮就能 `PING` 和 `LLEN`。这和 kube-proxy 笔记里的规则安装是同一段延迟。

## 队列变长时先写成 1

scale loop 在 `startScaleLoop`。它按 `pollingInterval` 调用 `checkScalers`，再进 `RequestScale`。

`RequestScale` 先读当前副本。触发器活跃，并且当前副本是 0 时，走 `scaleFromZeroOrIdle`。`minReplicaCount` 大于 0 就用它，否则写成 1。`updateScaleOnScaleTarget` 调 scale 子资源的 Update。

08:02:26 的日志：

```text
Successfully updated ScaleTarget
Original Replicas Count 0
New Replicas Count 1
```

事件是 `KEDAScaleTargetActivated`：`Scaled apps/v1.Deployment keda-redis-demo/order-worker from 0 to 1, triggered by s0-redis-orders`。

这一拍 HPA 还停在 `ScalingDisabled`。副本从 0 离开 0 的那一次写入来自 operator，来自 `scaleFromZeroOrIdle`。

Deployment 控制器看到 `spec.replicas` 变成 1，把 ReplicaSet `order-worker-7b4f879c9c` 从 0 扩到 1。ReplicaSet 创建 Pod。调度器把 Pod 绑到 `k8s-lab-worker`。kubelet 用节点上已有的 `redis:7-alpine` 把容器拉起来。08:02:28 这个 Pod 已经就绪，并且取走了一条，队列长度变成 7。

## HPA 再把 1 写成 4

副本大于 0 之后，`RequestScale` 不再改副本。它只更新 `LastActiveTime`，并把 `Active` 标成 `ScalerActive`。1 到 N 交给 HPA。

kind 的 kube-controller-manager 没有改 `--horizontal-pod-autoscaler-sync-period`。这份树的默认值是 15 秒，在 `pkg/controller/podautoscaler/config/v1alpha1/defaults.go`。

HPA 每一轮：

1. 读 Deployment 的 scale，拿到当前副本。
2. 向 apiserver 查询 `external.metrics.k8s.io/v1beta1` 的 `s0-redis-orders`，标签是 `scaledobject.keda.sh/name=order-worker`。
3. apiserver 把请求代理到 `keda-operator-metrics-apiserver`。
4. adapter 经 gRPC 调到 leader operator 的 `GetMetrics`。
5. `GetScaledObjectMetrics` 找到 scaler 缓存，再调 `GetMetricsAndActivity`。Redis scaler 再跑一次 Lua `LLEN`。
6. 算出期望副本，写回同一个 scale 子资源。

默认指标类型是 `AverageValue`，所以走 `GetExternalPerPodMetricReplicas`。期望副本是 `ceil(指标值 / 目标值)`。目标值是 2。`GenerateMetricInMili` 把长度收成 Quantity，HPA 用 `MilliValue` 除的时候毫单位会约掉，除数仍然是 list 长度和 `listLength`。队列长度为 8 或 7 时，`ceil(8/2)` 和 `ceil(7/2)` 都是 4，被 `maxReplicaCount` 卡住，也是 4。

容差默认 0.1。变化比例落在容差里就保持当前副本。从 1 拉到 4 时比例远在容差外。

08:02:33 的 HPA 事件：

```text
SuccessfulRescale
New size: 4
reason: external metric s0-redis-orders above target
```

Deployment 把同一个 ReplicaSet 从 1 扩到 4。三个新 Pod 都绑到 worker，几秒内就绪。此时队列长度已经是 4：四个 Pod 各取走一条，正在睡那 20 秒。

HPA 的缩容稳定窗口默认 5 分钟。队列随后掉到 4、3、0，按公式期望会下降，但窗口里还留着刚才“4”的建议。08:03:10 左右 HPA 的条件是 `ScaleDownStabilized`：`recent recommendations were higher than current one, applying the highest recent recommendation`。所以这一分钟里 HPA 没有把 4 收成 2 或 1。

## 消费者自己取订单

KEDA 把副本数写完就离开这条链路。四个容器各自连 `redis.keda-redis-demo.svc.cluster.local`，`BLPOP` 取出订单。日志里是 `processing order-8` 这样的行，八个编号都出现了。取走一条就睡 20 秒，所以第二轮取完时队列已经是 0，Pod 还在睡。

睡的过程中 operator 把副本写成 0。ReplicaSet 删除 Pod，kubelet 发出 `Killing`。正在睡的进程收到 `SIGTERM`。这次实验的处理时间只有 20 秒，grace period 够用。

## 队列空了以后写成 0

长度掉到 0 之后，`GetMetricsAndActivity` 的第二个返回值是 false，因为 `0 > 0` 不成立。`RequestScale` 进入不活跃分支。当前副本大于 0，并且 `minReplicaCount` 是 0，于是调用 `scaleToZeroOrIdle`。

冷却还没过时，条件是 `ScalerCooldown`。`cooldownPeriod` 这次设成 15 秒。默认值在 `scale_executor.go` 里是 `5 * 60` 秒，首次冷却 `defaultInitialCooldownPeriod` 是 0。08:02:59 采样还是 `ScalerCooldown`，副本仍是 4。

冷却结束，`updateScaleOnScaleTarget` 把副本写成 `minReplicaCount`。08:03:06 的日志：

```text
Successfully set ScaleTarget replicas count to ScaledObject minReplicaCount
Original Replicas Count 4
New Replicas Count 0
```

事件是 `KEDAScaleTargetDeactivated`：`Deactivated apps/v1.Deployment keda-redis-demo/order-worker from 4 to 0`。

副本变成 0 之后，HPA 下一轮在 `horizontal.go` 里看到 `currentReplicas == 0`。`minReplicas` 是 1，而且这次没有打开 HPA 自己的 scale-to-zero。`shouldComputeMetricsForZeroReplicas` 让这一轮停止改 scale，并把 `ScalingActive` 标成 False，原因 `ScalingDisabled`。08:03:40 的 HPA 已经是这个状态，`TARGETS` 显示 `<unknown>/2 (avg)`，`REPLICAS` 是 0。ScaledObject 的 `HPAActive` 回到 `ScalingDisabled`，状态仍是 True。

用户如果要停掉自动伸缩，用的是 ScaledObject 注解 `autoscaling.keda.sh/paused` 或 `autoscaling.keda.sh/paused-replicas`。`Reconcile` 看到它们会停掉 scale loop，并删掉 HPA。那是另一条路径。这次缩到 0 没有打这些注解。

## 这条链路停在哪里

KEDA 改的是带 `/scale` 子资源的对象的副本数。它不创建 Pod，不选节点，不安装 iptables 或 ipvs，也不从 Redis 里把消息取走。

HTTP 请求要先变成 Redis、队列或别的指标，才会进入这条链路。KEDA 没有一个按请求把 Pod 拉起来再把请求转进去的入口。那是别的项目的 activator。

一个 ScaledObject 可以挂多个 trigger。HPA 对每个外部指标各算一个期望副本，取最大的那个。这次只有 Redis 一个 trigger。

长任务如果不能接受缩容时被删掉，用 `ScaledJob`。它按事件创建 Job，做完就结束。这次没有建这个对象。

## 源码位置

| 位置 | 作用 |
| --- | --- |
| `cmd/operator/main.go` | 控制器进程、选主 id `operator.keda.sh`、gRPC 绑在 `:9666` |
| `cmd/adapter/main.go` | 外部指标适配器，默认连 `keda-operator` 的 9666 |
| `cmd/webhooks/main.go` | admission webhook 进程 |
| `controllers/keda/scaledobject_controller.go` | `Reconcile`、finalizer、确认 `/scale`、创建 HPA |
| `controllers/keda/hpa.go` | 组装 HPA spec |
| `apis/keda/v1alpha1/scaledobject_types.go` | `GetHPAMinReplicas`，0 映射成 1 |
| `pkg/scaling/scale_handler.go` | `startScaleLoop`、`GetScaledObjectMetrics` |
| `pkg/scaling/executor/scale_scaledobjects.go` | `RequestScale`、`scaleFromZeroOrIdle`、`scaleToZeroOrIdle`、`getHPAHealth` |
| `pkg/scaling/executor/scale_executor.go` | 默认冷却 5 分钟，首次冷却 0 |
| `pkg/metricsservice/server.go` | gRPC `GetMetrics`，只在 leader 上启动 |
| `pkg/provider/provider.go` | 把 HPA 的外部指标查询转成 gRPC |
| `pkg/scalers/redis_scaler.go` | Lua `LLEN`，指标名 `s0-redis-<list>`，`listLen > activationListLength` |
| `pkg/scalers/scaler.go` | 默认 `AverageValue`，`GenerateMetricNameWithIndex` |
| `pkg/controller/podautoscaler/horizontal.go` | 副本为 0 时写 `ScalingDisabled`；缩容稳定窗口 |
| `pkg/controller/podautoscaler/replica_calculator.go` | `GetExternalPerPodMetricReplicas`，`ceil(指标 / 目标)` |
| `pkg/controller/podautoscaler/config/v1alpha1/defaults.go` | 同步周期 15 秒，缩容稳定窗口 5 分钟 |

上面 Kubernetes 的三条在本地 `kubernetes` 检出里，不在 KEDA 仓库里。
