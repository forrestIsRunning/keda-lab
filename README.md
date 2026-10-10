# keda-lab

kind 集群 `k8s-lab` 上的 KEDA 2.21.0 实验。一条主线：Redis List 变长之后，Deployment 副本怎样从 0 写成 1，再写成 N，再写回 0。

GitHub：https://github.com/forrestIsRunning/keda-lab 。本地目录：`~/Projects/learning/keda-lab`。

| 笔记 | 本机跑到哪 |
|---|---|
| [redis-list-path.md](./redis-list-path.md) | 2026-10-10 08:02，8 笔订单，副本 0 → 1 → 4 → 0。架构图在这篇里 |

这套 kind 集群同时跑着 [sandbox-lab](https://github.com/forrestIsRunning/sandbox-lab)。context 仍是 `kind-k8s-lab`。不要新建集群，也不要 `kubectl config use-context`。下面的命令都带 `--context` 或 `--kube-context`。

## 版本

| 组件 | 版本 |
|---|---|
| KEDA Helm chart | `kedacore/keda` 2.21.0 |
| 三个镜像 | `ghcr.io/kedacore/keda:2.21.0`、`keda-metrics-apiserver:2.21.0`、`keda-admission-webhooks:2.21.0` |
| 集群 | kind `k8s-lab`，服务端 v1.36.1。KEDA 2.21 声明兼容 1.34–1.36 |
| Redis | 容器 `keda-redis`，镜像 `redis:7-alpine`，kind 网络 |

KEDA 源码不在本仓库。这次对照的检出是 `/tmp/keda-v2.21.0`，tag `v2.21.0`，commit `626ded5d783`。HPA 行为对照 `/Users/xiaoxia/Projects/learning/kubernetes`，commit `f322464da46`。组件笔记在本地 `~/Projects/learning/kubernetes-lab`，对应私有仓库 `forrestIsRunning/kubernetes-lab`。

## 前置条件

- Docker Desktop 正在运行。本机 `docker` 不在 `PATH` 上，`/usr/local/bin/docker` 是断掉的 OrbStack 符号链接。下面用 Docker Desktop 自带的二进制。
- kind 集群 `k8s-lab` 的两个节点已经 Ready：`k8s-lab-control-plane`、`k8s-lab-worker`。
- `kubectl`、`helm`。

```bash
export DOCKER=/Applications/Docker.app/Contents/Resources/bin/docker
"$DOCKER" info >/dev/null
kubectl --context kind-k8s-lab get nodes
```

## 安装 KEDA

```bash
helm repo add kedacore https://kedacore.github.io/charts
helm repo update kedacore
helm install keda kedacore/keda \
  --kube-context kind-k8s-lab \
  --namespace keda --create-namespace \
  --version 2.21.0
```

这次安装是 2026-10-10 07:50，revision 1。三个 Deployment 都在 `k8s-lab-worker` 上。

### 镜像进不了节点

kind 节点的 `HTTP_PROXY` / `HTTPS_PROXY` 指向 `http://127.0.0.1:1087`。这个代理在 Mac 上，不在 kind 容器里。kubelet 拉 `ghcr.io` 会报 `proxyconnect tcp: dial tcp 127.0.0.1:1087: connect: connection refused`。

这次没有改节点里的代理。宿主机 Docker 能拉镜像。`kind load docker-image` 没有成功：OCI index 导入时 `ctr` 报 content digest not found。用的是下面这条。

```bash
export DOCKER=/Applications/Docker.app/Contents/Resources/bin/docker
for image in \
  ghcr.io/kedacore/keda:2.21.0 \
  ghcr.io/kedacore/keda-metrics-apiserver:2.21.0 \
  ghcr.io/kedacore/keda-admission-webhooks:2.21.0
do
  "$DOCKER" pull "$image"
  "$DOCKER" save "$image" | "$DOCKER" exec -i k8s-lab-worker ctr --namespace=k8s.io images import -
  "$DOCKER" save "$image" | "$DOCKER" exec -i k8s-lab-control-plane ctr --namespace=k8s.io images import -
done
```

chart 把 `imagePullPolicy` 设成 `Always`。节点上已经导入的镜像会被忽略，Pod 仍去外网拉。三个 Deployment 要改成 `IfNotPresent`：

```bash
for deploy in keda-operator keda-operator-metrics-apiserver keda-admission-webhooks; do
  kubectl --context kind-k8s-lab -n keda patch deploy "$deploy" --type strategic \
    -p '{"spec":{"template":{"spec":{"containers":[{"name":"'"$deploy"'","imagePullPolicy":"IfNotPresent"}]}}}}'
done
```

`keda-admission-webhooks` 的容器名就是 Deployment 名。`helm upgrade` 会把 `imagePullPolicy` 改回 `Always`。

装好后应看到：

```bash
kubectl --context kind-k8s-lab -n keda get deploy
kubectl --context kind-k8s-lab get apiservice v1beta1.external.metrics.k8s.io
```

APIService 的 Available 应为 True，后端 Service 是 `keda-operator-metrics-apiserver`。

## Redis 和订单

Redis 不进集群。容器接在 docker 网络 `kind` 上，宿主机端口 `16379`。

```bash
export DOCKER=/Applications/Docker.app/Contents/Resources/bin/docker
"$DOCKER" run -d --name keda-redis --network kind -p 16379:6379 redis:7-alpine
manifests/apply.sh
```

`manifests/apply.sh` 用 `docker inspect` 读 `keda-redis` 在 kind 网络上的地址，填进没有 selector 的 Service `redis` 的 Endpoints，再 `kubectl --context kind-k8s-lab apply`。这次地址是 `172.20.0.4`。脚本不改当前 context。

2026-10-10 08:02:22（+0800）推了 8 笔订单：

```bash
export DOCKER=/Applications/Docker.app/Contents/Resources/bin/docker
"$DOCKER" exec keda-redis redis-cli LPUSH orders \
  order-1 order-2 order-3 order-4 order-5 order-6 order-7 order-8
```

随后副本走到 4，再回到 0。时间线和每次是谁写的 scale，见 [redis-list-path.md](./redis-list-path.md)。

## 不要动

- 不要新建 kind 集群，不要 `kubectl config use-context`。
- 不要改这些命名空间：`agent-sandbox-demo`、`agent-sandbox-system`、`cubesandbox-system`、`opensandbox`、`opensandbox-system`。
- 不要装 KEDA HTTP add-on。这次没有按请求把流量转进 Pod 的入口。
- 不要为了这次实验去改 kind 节点里的 `HTTP_PROXY`。

## 不在本仓库

- KEDA 源码。对照 tag `v2.21.0`。
- `kubernetes` 源码。HPA 的 `ScalingDisabled` 和缩容稳定窗口在那棵树里。
- `kubernetes-lab` 的组件笔记。本地阅读，GitHub 上那份仓库是私有的。
