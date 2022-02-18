# Uber Remote Shuffle Service (RSS) - Kubernetes Version

Uber Remote Shuffle Service provides the capability for Apache Spark applications to store shuffle data 
on remote servers. See more details on Spark community document: 
[[SPARK-25299][DISCUSSION] Improving Spark Shuffle Reliability](https://docs.google.com/document/d/1uCkzGGVG17oGC6BJ75TpzLAZNorvrAU3FRd2X-rVHSM/edit?ts=5e3c57b8).

The high level design for Remote Shuffle Service could be found [here](https://github.com/uber/RemoteShuffleService/blob/master/docs/server-high-level-design.md).

Please contact us (**remoteshuffleservice@googlegroups.com**) for any question or feedback.

## Quick Start: Run Spark Application With Pre-Built Images

### Deploy Remote Shuffle Service using Helm

Run following command under root directory of this project:

```
helm install remote-shuffle-service charts/remote-shuffle-service --namespace remote-shuffle-service --create-namespace
```

### Run Spark Application With Remote Shuffle Service and Dynamic Allocation

Use following pre-built Spark images with embedded Remote Shuffle Service client jar file:

```
ghcr.io/datapunchorg/spark:spark-3.2.1-1643336295
ghcr.io/datapunchorg/spark:pyspark-3.2.1-1643336295
```

Add configure to your Spark application like following, keep string like `rss-%s` 
inside value for `spark.shuffle.rss.serverSequence.connectionString`, since `RssShuffleManager`
will use that to format connection string for different RSS server instances:

```
spark.shuffle.manager=org.apache.spark.shuffle.RssShuffleManager
spark.shuffle.rss.serviceRegistry.type=serverSequence
spark.shuffle.rss.serverSequence.connectionString=rss-%s.rss.remote-shuffle-service.svc.cluster.local:9338
spark.shuffle.rss.serverSequence.startIndex=0
spark.shuffle.rss.serverSequence.endIndex=1
spark.serializer=org.apache.spark.serializer.KryoSerializer
spark.dynamicAllocation.enabled=true
spark.dynamicAllocation.shuffleTracking.enabled=true
spark.dynamicAllocation.shuffleTracking.timeout=1
```

Now you can run your application in your own Spark Kubernetes environment. Please note the value for 
"spark.shuffle.rss.serverSequence.connectionString" contains string like "rss-%s". This is intended because 
RssShuffleManager will use it to generate actual connection string like rss-0.xxx and rss-1.xxx.

If you do not have your own environment to run Spark, please see [Punch Project](https://github.com/datapunchorg/punch),
which provides a one-click tool to create a Spark environment on AWS.

## Build Remote Shuffle Service By Yourself

Make sure JDK and maven are installed on your machine.

### Build Server Jar File

- Run following command inside this project directory: 

```
mvn clean package -Pserver -DskipTests -Dmaven.javadoc.skip=true
```

This command creates `remote-shuffle-service-server-xxx.jar` file under `target` directory.

### Build Server Docker Image

```
rm target/original-remote-shuffle-service-*.jar
rm target/remote-shuffle-service-*-sources.jar
mv target/remote-shuffle-service-server-*.jar target/remote-shuffle-service-server.jar
docker build -t remote-shuffle-service-server:0.0.10 .
docker images
```

The upper commands will create and list docker image for Remote Shuffle Service.

### Build Spark Image with Client Jar File

To run Spark with Remote Shuffle Service on Kubernetes. Spark image must have the Remote Shuffle Service client jar file inside SPARK_HOME/jars folder.

You could add following dependency in Spark pom.xml file and build Spark distribution and image.

```
    <dependency>
      <groupId>org.datapunch</groupId>
      <artifactId>remote-shuffle-service-client</artifactId>
      <version>0.0.10</version>
      <scope>compile</scope>
    </dependency>
```

You could also build the Remote Shuffle Service client jar file by yourself, using command like following:

```
mvn clean package -Pclient -DskipTests -Dmaven.javadoc.skip=true
```

With Remote Shuffle Service client jar, there are two steps to build Spark image:

1. Build Spark distribution, see: [Building Spark](https://spark.apache.org/docs/latest/building-spark.html), e.g.

```
./dev/make-distribution.sh --name spark-with-remote-shuffle-service-client --pip --tgz -Phive -Phive-thriftserver -Pkubernetes -Phadoop-3.2 -Phadoop-cloud
```

2. Build Spark docker image: unzip the Spark distribution tgz file, run commands like:

```
./bin/docker-image-tool.sh -t spark-with-remote-shuffle-service build
./bin/docker-image-tool.sh -t spark-with-remote-shuffle-service -p kubernetes/dockerfiles/spark/bindings/python/Dockerfile build
```

The first command creates Java Spark image, the second command creates Python Spark image.
