# Uber Remote Shuffle Service (RSS)

Uber Remote Shuffle Service provides the capability for Apache Spark applications to store shuffle data 
on remote servers. See more details on Spark community document: 
[[SPARK-25299][DISCUSSION] Improving Spark Shuffle Reliability](https://docs.google.com/document/d/1uCkzGGVG17oGC6BJ75TpzLAZNorvrAU3FRd2X-rVHSM/edit?ts=5e3c57b8).

## How to Build

Make sure JDK 8+ and maven is installed on your machine.

### Build RSS Server

- Run: 

``
mvn clean package -Pserver -DskipTests
``

This command creates **remote-shuffle-service-xxx-server.jar** file for RSS server, e.g. target/remote-shuffle-service-0.0.8-server.jar.

### Build RSS Client

- Run: 

``
mvn clean package -Pclient -DskipTests
``

This command creates **remote-shuffle-service-xxx-client.jar** file for RSS server, e.g. target/remote-shuffle-service-0.0.8-client.jar.

## How to Run

### Run RSS Server

- Pick up a server in your environment, e.g. `server1`. Run RSS server jar file (**remote-shuffle-service-xxx-server.jar**) as a Java application, for example,

``
java -Dlog4j.configuration=log4j-rss-prod.properties -cp target/remote-shuffle-service-0.0.8-server.jar com.uber.rss.StreamServer -port 12222 -serviceRegistry standalone -dataCenter dc1
``

### Run Spark application with RSS Client

- Upload client jar file (**remote-shuffle-service-xxx-client.jar**) to your HDFS, e.g. `hdfs:///file/path/remote-shuffle-service-0.0.8-client.jar`

- Add configure to your Spark application like following (you need to adjust the values based on your environment):

``
spark.jars=hdfs:///file/path/remote-shuffle-service-0.0.8-client.jar
spark.executor.extraClassPath=remote-shuffle-service-0.0.8-client.jar
spark.shuffle.manager=org.apache.spark.shuffle.RssShuffleManager
spark.shuffle.rss.serviceRegistry.type=standalone
spark.shuffle.rss.serviceRegistry.server=server1:12222
spark.shuffle.rss.dataCenter=dc1
``

- Run your Spark application
