# Quick Start(Running in GeaFlow Console)
## Prepare
1. Install Docker and adjust Docker service resource settings (Dashboard-Settings-Resources), then start Docker service:

![docker_pref](../static/img/docker_pref.png)

2. Pull GeaFlow Image

Run the following command to pull the remote geaflow console image:

For x86 architecture pull x86 image:
```shell
docker pull tugraph/geaflow-console:0.1
```

If it is arm architecture, pull the arm image:
```shell
docker pull tugraph/geaflow-console-arm:0.1
```

If the pull fails due to network problems, you can also run the following command to directly build the local image 
(before building the image, start the docker container, and the build the script to build the image of the 
corresponding type based on the machine type):

```shell
git clone https://github.com/TuGraph-family/tugraph-analytics.git
cd tugraph-analytics/
bash ./build.sh --all

```

The entire compilation process may take some time, please be patient. After the image compilation is successful, use 
the following command to view the image.
```shell
docker images
```
The name of the remotely pulled image is: **tugraph/geaflow-console:0.1**(x86 architecture) or **tugraph/ 
geaflow-console-arm :0.1**(arm architecture)
. The name of the local image is **geaflow-console:0.1**. You only need to select one method to build the image.

## Running Job in Docker
Below is an introduction on running the flow graph job mentioned in Local Mode Execution inside a docker container.


1. Start the GeaFlow Console service locally.


* For the Remote Image:

**x86 architecture**
```shell
docker run -d --name geaflow-console -p 8080:8080 -p 8888:8888 tugraph/geaflow-console:0.1
```

**arm Architecture**
```
docker run -d --name geaflow-console -p 8080:8080 -p 8888:8888 tugraph/geaflow-console-arm:0.1
```

* For the Local Image
```shell
docker run -d --name geaflow-console -p 8080:8080 -p 8888:8888 geaflow-console:0.1
```

**Note**: The tag name of the remote image is different from that of the local build image, and the startup 
command is different.

Enter the container and wait for the Java process to start. After that, access [localhost:8888](http://localhost:8888) to enter the GeaFlow Console platform page.

```shell
> docker exec -it geaflow-console tailf /tmp/logs/geaflow/app-default.log

# wait the logs below and open url http://localhost:8888
GeaflowApplication:61   - Started GeaflowApplication in 11.437 seconds (JVM running for 13.475)
```

2. Register Account

The first registered user will be set as the default administrator. Log in as an administrator and use the one-click installation feature to start system initialization.

![install_welcome](../static/img/install_welcome_en.png)


3. Config GeaFlow

GeaFlow requires configuration of runtime environment settings during its initial run, including cluster settings, runtime settings, data storage settings, and file storage settings.

3.1 Cluster Config

Click "Next" and use the default Container mode, which is local container mode.

![install_container](../static/img/install_container_en.png)

3.2 Runtime Config

For local runtime mode, you can skip this step and use the default system settings by clicking "Next" directly.

![install_conainer_meta_config.png](../static/img/install_container_meta_config_en.png)

3.3 Storage Config

Select the graph data storage location. For local mode, select LOCAL and enter a local directory. If you don't need to fill it out, click "Next" directly.

![install_storage_config](../static/img/install_storage_config_en.png)

3.4 File Config

This configuration is for the persistent storage of GeaFlow engine JAR and user JAR files, such as in HDFS. For local runtime mode, it is the same as the data storage configuration, so select LOCAL mode and enter a local directory. If you don't need to fill it out, click "Next" directly.

![install_jar_config](../static/img/install_jar_config_en.png)
After completing the configuration, click the one-click installation button. After successful installation, the administrator will automatically switch to the default instance under the personal tenant and can directly create and publish graph computing tasks.

4. Create Job

Go to the `DEVELOPMENT` page, select the `Jobs` tab on the left, click the "Add" button in the upper right corner, and create a new DSL job.

![create_job](../static/img/create_job_en.png)
Fill in the job name, task description, and DSL content. The DSL content is the same as described in the local runtime job section earlier, just modify the DSL and replace tbl_source and tbl_result tables with ${your.host.ip}.

```sql
set geaflow.dsl.window.size = 1;
set geaflow.dsl.ignore.exception = true;

CREATE GRAPH IF NOT EXISTS dy_modern (
  Vertex person (
    id bigint ID,
    name varchar
  ),
  Edge knows (
    srcId bigint SOURCE ID,
    targetId bigint DESTINATION ID,
    weight double
  )
) WITH (
  storeType='rocksdb',
  shardCount = 1
);

CREATE TABLE IF NOT EXISTS tbl_source (
  text varchar
) WITH (
  type='socket',
  `geaflow.dsl.column.separator` = '#',
  `geaflow.dsl.socket.host` = '${your.host.ip}',
  `geaflow.dsl.socket.port` = 9003
);

CREATE TABLE IF NOT EXISTS tbl_result (
  a_id bigint,
  b_id bigint,
  c_id bigint,
  d_id bigint,
  a1_id bigint
) WITH (
  type='socket',
    `geaflow.dsl.column.separator` = ',',
    `geaflow.dsl.socket.host` = '${your.host.ip}',
    `geaflow.dsl.socket.port` = 9003
);

USE GRAPH dy_modern;

INSERT INTO dy_modern.person(id, name)
SELECT
cast(split_ex(t1, ',', 0) as bigint),
split_ex(t1, ',', 1)
FROM (
  Select trim(substr(text, 2)) as t1
  FROM tbl_source
  WHERE substr(text, 1, 1) = '.'
);

INSERT INTO dy_modern.knows
SELECT
 cast(split_ex(t1, ',', 0) as bigint),
 cast(split_ex(t1, ',', 1) as bigint),
 cast(split_ex(t1, ',', 2) as double)
FROM (
  Select trim(substr(text, 2)) as t1
  FROM tbl_source
  WHERE substr(text, 1, 1) = '-'
);

INSERT INTO tbl_result
SELECT DISTINCT
  a_id,
  b_id,
  c_id,
  d_id,
  a1_id
FROM (
  MATCH (a:person) -[:knows]->(b:person) -[:knows]-> (c:person)
   -[:knows]-> (d:person) -> (a:person)
  RETURN a.id as a_id, b.id as b_id, c.id as c_id, d.id as d_id, a.id as a1_id
);
```

![add_dsl_job](../static/img/add_dsl_job_en.png)
After creating the job, click the "Publish" button to publish the job. 

![add_dsl_job](../static/img/job_list_en.png)

Then go to the Job Management page and click the "Submit" button to submit the taslk for execution.

![task_detail](../static/img/task_detail_en.png)

5. Start the socket service and input data

Go to the GeaFlow project path and execute the following command to start the socket service.

```shell
bin/socket.sh
```
After the socket service is started, enter the vertex-edge data, and the calculation result will be displayed in real-time on the screen:

```
. 1,jim
. 2,kate
. 3,lily
. 4,lucy
. 5,brown
. 6,jack
. 7,jackson
- 1,2,0.2
- 2,3,0.3
- 3,4,0.2
- 4,1,0.1
- 4,5,0.1
- 5,1,0.2
- 5,6,0.1
- 6,7,0.1
```

![ide_socket_server](../static/img/ide_socket_server.png) 

## K8S Deployment
GeaFlow supports K8S deployment. For details about the deployment mode, see the document: [K8S Deployment](deploy/install_guide.md).