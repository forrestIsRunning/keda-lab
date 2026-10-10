# keda-lab

公开仓库：[forrestIsRunning/keda-lab](https://github.com/forrestIsRunning/keda-lab)

本地 kind 集群 `k8s-lab` 上的 KEDA 笔记。集群和 Kubernetes 组件笔记在 [kubernetes-lab](../kubernetes-lab/README.md)。一篇笔记只讲一条主线。

| 笔记 | 内容 |
| --- | --- |
| [redis-list-path.md](redis-list-path.md) | 一笔订单进 Redis List 之后，KEDA 怎样把 Deployment 副本从 0 写成 1，再交给 HPA 写成 N，冷却后写回 0。架构图嵌在这篇笔记里 |

清单在 [manifests/keda-redis-demo.yaml](manifests/keda-redis-demo.yaml)。`manifests/apply.sh` 用 Docker 容器 `keda-redis` 在 kind 网络上的地址填 Endpoints，再应用到上下文 `kind-k8s-lab`。脚本不切换当前 kubectl 上下文。

KEDA 2.21.0 装在命名空间 `keda`。业务对象在 `keda-redis-demo`。kind 节点的 `HTTP_PROXY` 指向容器里不存在的 `127.0.0.1:1087`，所以三个镜像是在宿主机 Docker 拉取后用 `ctr images import` 导入的，Deployment 的 `imagePullPolicy` 改成了 `IfNotPresent`。`helm upgrade` 会把这个字段改回去。
