# base image
FROM docker.io/library/centos:7@sha256:9d4bcbbb213dfd745b58be38b13b996ebb5ac315fe75711bd618426a630e0987

VOLUME /tmp

ENV GEAFLOW_INSTALL_PATH=/opt
ENV GEAFLOW_HOME=$GEAFLOW_INSTALL_PATH/geaflow

ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# base packages
RUN yum -y install wget ps vim curl
RUN wget --no-check-certificate http://repos.fedorapeople.org/repos/dchen/apache-maven/epel-apache-maven.repo -O /etc/yum.repos.d/epel-apache-maven.repo

# jdk
RUN yum -y install java-1.8.0-openjdk java-1.8.0-openjdk-devel \
    && echo "export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk" >> /etc/profile \
    && echo "export JRE_HOME=\${JAVA_HOME}/jre" >> /etc/profile \
    && echo "export CLASSPATH=.:\${JAVA_HOME}/lib:\${JRE_HOME}/lib:\$CLASSPATH" >> /etc/profile \
    && echo "export JAVA_PATH=\${JAVA_HOME}/bin:\${JRE_HOME}/bin" >> /etc/profile \
    && echo "export PATH=\$PATH:\${JAVA_PATH}" >> /etc/profile \
    && source /etc/profile

# mysql
RUN rpm -Uvh https://dev.mysql.com/get/mysql80-community-release-el7-7.noarch.rpm \
    && rpm --import https://repo.mysql.com/RPM-GPG-KEY-mysql-2022 \
    && yum -y install mysql-community-server \
    && mysqld --initialize-insecure --user=mysql

# redis
RUN yum install -y epel-release \
    && yum install -y redis

# influxdb
RUN echo '[influxdata]' >> /etc/yum.repos.d/influxdata.repo \
    && echo 'name = InfluxData Repository - Stable' >> /etc/yum.repos.d/influxdata.repo \
    && echo 'baseurl = https://repos.influxdata.com/stable/\$basearch/main' >> /etc/yum.repos.d/influxdata.repo \
    && echo 'enabled = 1' >> /etc/yum.repos.d/influxdata.repo \
    && echo 'gpgcheck = 1' >> /etc/yum.repos.d/influxdata.repo \
    && echo 'gpgkey = https://repos.influxdata.com/influxdata-archive_compat.key' >> /etc/yum.repos.d/influxdata.repo \
    && yum install -y influxdb2 \
    && mkdir /usr/lib/influxdb2-client-2.7.3-linux-amd64 \
    && cd /usr/lib/influxdb2-client-2.7.3-linux-amd64 \
    && wget https://dl.influxdata.com/influxdb/releases/influxdb2-client-2.7.3-linux-amd64.tar.gz \
    && tar xvzf influxdb2-client-2.7.3-linux-amd64.tar.gz \
    && cp influx /usr/local/bin/ \
    && rm -rf /usr/lib/influxdb2-client-2.7.3-linux-amd64

# copy console jar
WORKDIR $GEAFLOW_HOME/
COPY geaflow-console/target/boot/geaflow-console-bootstrap-*-executable.jar \
    $GEAFLOW_HOME/boot/geaflow-console-bootstrap.jar
COPY geaflow-console/target/config $GEAFLOW_HOME/config
COPY geaflow-console/docker $GEAFLOW_HOME/

# copy geaflow jar
COPY geaflow/geaflow-deploy/geaflow-assembly/target/geaflow-assembly-*[!-sources].jar \
    $GEAFLOW_HOME/files/geaflow.jar

EXPOSE 8888 3306 6379 8086 2181

ENTRYPOINT ["bash", "/opt/geaflow/bin/start-process.sh"]
