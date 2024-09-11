# Install Minikube

Here we will use minikube as an example to simulate a K8S cluster on a single machine. 

If you already have a K8S cluster, you can skip this part and use it directly.

Download and install minikube.
```
# ARM architecture
curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-darwin-arm64
sudo install minikube-darwin-arm64 /usr/local/bin/minikube

# x86 architecture
curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-darwin-amd64
sudo install minikube-darwin-amd64 /usr/local/bin/minikube
```

Start minikube and dashboard.
```
# Start minikube with docker as the driver
minikube start --driver=docker --ports=32761:32761 —image-mirror-country='cn'
# Starting minikube dashboard will automatically open the dashboard page in the browser. 
# If it doesn't open, copy the dashboard address provided in the terminal and open it in the browser.
minikube dashboard
```

**Note:**
Do not to close the terminal process that the dashboard belongs to. For subsequent operations,
please start a new terminal. Otherwise, the API server process will exit.

If you want to use GeaFlow on the local minikube environment, please make sure that minikube is running properly. GeaFlow engine image will be automatically built into the minikube environment. Otherwise, it will be built into the local Docker environment and you need to manually push it to the image repository for use.

```shell
# confirm host、kubelet、apiserver is running
minikube status
```