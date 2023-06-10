## Build Images

Download GeaFlow source code, build GeaFlow engine image and GeaFlow Console image.

```shell
git clone https://github.com/TuGraph-family/tugraph-analytics.git
cd tugraph-analytics/
bash ./build.sh --all
```
**Note:**
If you want to use GeaFlow on the local minikube environment, please make sure that minikube is running properly. GeaFlow engine image will be automatically built into the minikube environment. Otherwise, it will be built into the local Docker environment and you need to manually push it to the image repository for use.

```shell
# confirm host、kubelet、apiserver is running
minikube status
```

After the image compilation is successful, use the following command to view the images:
```shell
# check geaflow image
eval $(minikube docker-env)
docker images

# check geaflow-console image
eval $(minikube docker-env --unset)
docker images
```
The default GeaFlow platform image name is "geaflow-console:0.1", and the engine image name is "geaflow:0.1". After starting the platform image "geaflow-console:0.1", you can access the GeaFlow console through a web browser to create and submit GeaFlow jobs. The GeaFlow jobs are run based on the built engine image "geaflow:0.1" by default.

## Start Containers

Start GeaFlow Console platform service locally. (Replace ${your.host.name} with your machine's public IP address)

```shell
docker run -d --name geaflow-console -p 8080:8080 -p 8888:8888 -p 3306:3306 -p 6379:6379 -p 8086:8086 -e geaflow.host=${your.host.name} geaflow-console:0.1
```

The container starts in "local" mode by default, and local MySQL, Redis, and InfluxDB are launched by default.

```properties
# /opt/geaflow/config/application.properties
geaflow.deploy.mode=local
geaflow.host=${your.host.name}
geaflow.web.port=8888
geaflow.gateway.port=8080
geaflow.web.url=http://${geaflow.host}:${geaflow.web.port}
geaflow.gateway.url=http://${geaflow.host}:${geaflow.gateway.port}

# Datasource
spring.datasource.driver-class-name=com.mysql.jdbc.Driver
spring.datasource.url=jdbc:mysql://${geaflow.host}:3306/geaflow?useUnicode=true&characterEncoding=utf8
spring.datasource.username=geaflow
spring.datasource.password=geaflow
```

Enter the container and wait for the GeaFlow web process to start. After that, access [localhost:8888](localhost:8888) to enter the GeaFlow Console platform page.

```shell
> docker exec -it geaflow-console tailf /tmp/logs/geaflow/app-default.log

# wait the logs below and open url http://localhost:8888
GeaflowApplication:61   - Started GeaflowApplication in 11.437 seconds (JVM running for 13.475)
```

If you want to start the container in "cluster" mode, you need to adjust the datasource configuration to point to an external data source and set a unified service URL for external access. The container supports environment variable injection of datasource configuration and service URL. For example:

```shell
docker run -d --name geaflow-console -p 8080:8080 -p 8888:8888 \
-e geaflow.deploy.mode="cluster" \
-e spring.datasource.url=${your.datasource.url} \
-e spring.datasource.username=${your.datasource.username} \
-e spring.datasource.password=${your.datasource.password} \
-e geaflow.web.url=${your.geaflow.web.url} \
-e geaflow.gateway.url=${your.geaflow.gateway.url} \
geaflow-conosle:1.0
```

## Register and Login
The first registered user will be set as the administrator by default. Log in as an administrator and use the "One-click Installation" function to start system initialization.

![install_welcome](../../static/img/install_welcome.png)

## System Initialization
When the administrator logs into the GeaFlow system for the first time, the one-click installation process will be triggered to prepare the system for initialization.

### Cluster Configuration
Configure the runtime cluster for GeaFlow jobs, and it is recommended to use Kubernetes. In local mode, the default proxy address is ${your.host.name}:8000. Please make sure that minikube has been started locally and the proxy address has been set. If you set the address of the K8S cluster, please ensure that the connectivity of the address is normal.

![install_cluster_config](../../static/img/install_cluster_config.png)

### Runtime Configuration
Configure the storage of runtime metadata for GeaFlow jobs. Runtime metadata includes information such as the job's Pipeline, Cycle, checkpoint, and exceptions. It is recommended to use MySQL.

Configure the storage of HA metadata for GeaFlow job runtime. HA metadata includes information such as Master, Driver, Container, and other main components. It is recommended to use Redis.

Configure the storage of metric data for GeaFlow job runtime, which is used for job metric monitoring. It is recommended to use InfluxDB.

![undefined](../../static/img/install_meta_config.png)

In local mode, when the docker container starts, MySQL, Redis, and InfluxDB services will be automatically pulled up by default.

### Data Storage Configuration
Configure the persistent storage of GeaFlow job, graph, and table data, and it is recommended to use HDFS. In local mode, the default is the disk inside the container.

![install_storage_config](../../static/img/install_storage_config.png)

### File Storage Configuration
Configure the persistent storage of GeaFlow engine JAR and user JAR files, and it is recommended to use HDFS. In local mode, the default is the disk inside the container.

![install_jar_config](../../static/img/install_jar_config.png)

After the installation is successful, **the administrator will automatically switch to the default instance under the default tenant**, and you can directly create and publish graph computation tasks at this time.

## Work Mode
GeaFlow console supports tenant isolation and supports system mode perspective and tenant mode perspective to use product features.

**Note:**
The user icon menu in the upper right corner of the page provides a quick mode switching entry.

### System Mode
When the user logs in as an administrator, they will enter the system mode. At this time, you can perform system operations such as one-click installation and system management.

![install_system_mode](../../static/img/install_system_mode.png)

In system mode, the administrator can manage information such as clusters, GeaFlow engine versions, files, users, tenants, etc.

### Tenant Mode
After normal user login, they will enter tenant mode. At this time, they can perform graph computing development and maintenance operations.

![install_tenant_mode](../../static/img/install_tenant_mode.png)

In tenant mode, users can create development resources such as instances, graphs, tables, and graph computing tasks. They can also publish graph computing tasks, submit graph computing jobs, and perform other related operations.

## Task Management
Add a graph computing task and describe the business logic of graph computing using SQL+GQL.

![install_task_manager](../../static/img/install_task_manager.png)

After creating a task, click on "publish" to enter the job operation and maintenance interface.

## Task Maintenance
Before submitting a job, you can also adjust the default task parameters and cluster parameters to facilitate adjustments to job behavior.

![install_job_op](../../static/img/install_job_op.png)

Access other tabs on the job details page to view information about job runtime, metrics, containers, exceptions, logs, and more.











