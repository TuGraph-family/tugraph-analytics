# 安装minikube

这里以minikube为例，单机模拟K8S集群。如果已有K8S集群可以直接使用，跳过该部分。


下载安装minikube
```
# arm架构
curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-darwin-arm64
sudo install minikube-darwin-arm64 /usr/local/bin/minikube

# x86架构
curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-darwin-amd64
sudo install minikube-darwin-amd64 /usr/local/bin/minikube
```

启动minikube和dashboard
```
# 以docker为driver启动minikube
minikube start --driver=docker --ports=32761:32761 —image-mirror-country='cn'
# 启动minikube dashboard，会自动在浏览器中打开dashboard页面，如未打开复制终端给出的dashboard地址到浏览器打开
minikube dashboard
```

**注意：**
注意不要关闭dashboard所属的terminal进程，后续操作请另起终端进行，否则api server进程会退出

如果希望在本地minikube环境使用GeaFlow，请确保minikube正常启动，GeaFlow引擎镜像会自动构建到minikube环境，否则构建到本地Docker环境，需要手工自行push到镜像仓库使用。
```shell
# confirm host、kubelet、apiserver is running
minikube status
```