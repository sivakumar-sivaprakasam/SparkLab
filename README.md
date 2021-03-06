# Apache Spark Core & SQL Lab

## Prerequisites 

### Install OpenJDK 11

To install OpenJDK11, first update the package index

```
sudo apt update
```

Next, install default Java OpenJDK package with

```
sudo apt install default-jdk
```

Verify the java installation

```
java -version
```

The output will be something similar like this

```
openjdk version "11.0.2" 2019-01-15
OpenJDK Runtime Environment (build 11.0.2+9-Ubuntu-3ubuntu118.04.3)
OpenJDK 64-Bit Server VM (build 11.0.2+9-Ubuntu-3ubuntu118.04.3, mixed mode, sharing)
```

### Configure JAVA_HOME

To create JAVA_HOME environment variable, open `/etc/environment` file:

```
sudo nano /etc/environment
```

Add the following line at the end of the file:

```
JAVA_HOME="/usr/lib/jvm/java-11-open-jdk-amd64"
```

### Download & Install Maven

To install Maven, first update the package index

```
sudo apt update
```

Next, install Maven package with

```
sudo apt install maven
```

Verify the maven installation

```
mvn -version
```

The output will be something similar like this

```
Apache Maven 3.6.3
Maven home: /usr/share/maven
Java version: 11.0.7, vendor: Ubuntu, runtime: /usr/lib/jvm/java-11-openjdk-amd64
Default locale: en_US, platform encoding: UTF-8
OS name: "linux", version: "5.4.0-26-generic", arch: "amd64", family: "unix"
```

### Configure M2_HOME

To create M2_HOME environment variable, open `/etc/environment` file:

```
sudo nano /etc/environment
```

Add the following line at the end of the file:

```
M2_HOME="/usr/share/maven"
```

### Download & Install Spark 3.2.0

Create a directory in `/home/ubuntu/` called `downloads` to keep your downloads

```
mkdir -p /home/ubuntu/downloads
```

Use `curl` to download the Apache Spark 3.2.0 binaries:

```
curl "https://dlcdn.apache.org/spark/spark-3.2.0/spark-3.2.0-bin-hadoop3.2.tgz" -o /home/ubuntu/downloads/spark.tgz
```

Create a directory named `spark` and change to this directory. This will be the base directory for spark

```
mkdir -p /home/ubuntu/spark && cd /home/ubuntu/spark
```

Extract the downloaded archive using `tar` command:

```
tar -xvzf /home/ubuntu/downloads/spark.tgz --strip 1
```

### Download & Install Eclipse IDE

Launch terminal and run following command

```
sudo snap install --classic eclipse
```

### Download & Install IntelliJ IDEA

Launch terminal and run following command

```
sudo snap install intellij-idea-community --classic
```

### Download & Install Google Chrome Browser

Launch terminal and run following command

```
wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
```

To install Chrome browser, run following command

```
sudo apt install ./google-chrome-stable_current_amd64.deb
```

## Spark Lab exercises using Spark-Shell (Scala)

### Launch Spark-Shell

Open terminal and cd to SPARK_INSTALLATION_FOLDER/bin and type the below command

```
./spark-shell
```

You could notice the above command created Spark Context, Spark Content Web UI & also Spark Session objects

```
Spark context Web UI available at http://192.168.1.83:4040
Spark context available as 'sc' (master = local[*], app id = local-1637652156569).
Spark session available as 'spark'.
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 3.2.0
      /_/
         
Using Scala version 2.12.15 (OpenJDK 64-Bit Server VM, Java 11.0.11)
```

You can also launch browser to access the Spark Web UI @ http://localhost:4040/

### Exercise 1 - Using SparkContext, create an RDD

In the `scala>` prompt, run the below commands to create an RDD

```
scala> var r = sc.parallelize(Seq(1, 2, 3, 4, 5))
r: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[0] at parallelize at <console>:23


```

The above command created an RDD. Now, examine the RDD to check the partition size

```
scala> r.partitions.size
res0: Int = 4
``` 

Depends on the configuration (number of vCPU) of the VM, you will see the partition size. 

### Exercise 2 - RDD Repartition

To change the default partition it can be done in 2 ways. One is at the time of creating RDD or call Repartition method on RDD

#### Calling repartition method

```
scala> var r2 = r.repartition(1)
r2: org.apache.spark.rdd.RDD[Int] = MapPartitionsRDD[4] at repartition at <console>:23

scala> r2.partitions.size
res1: Int = 1

scala> 
```

#### Specifying partition size at the time of creating RDD

```
scala> var r = sc.parallelize(Seq(1, 2, 3, 4, 5), 1)
r: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[5] at parallelize at <console>:23

scala> r.partitions.size
res2: Int = 1

scala> 

```

### Exercise 3 - RDD Transformation & Action

Let's apply some basic transformation on an RDD. In the below example, we're just multiplying each element by 2 and then calling action `foreach` on it.

Remember RDD transformations are lazy (i.e., execution takes place only when an Action is called on it). You can check in the browser to see effect

```
scala> var r2 = r.map(x => x * 2)
r2: org.apache.spark.rdd.RDD[Int] = MapPartitionsRDD[6] at map at <console>:23

scala> r2.foreach(x => println(x))
2
4
6
8
10

scala> 

```

### Exercise 4 - RDD Transformation & Action

Let's explore some other set of transformation on an RDD. In the below example, we're filtering EVEN numbers from the list and then calling action `reduce` on it.

```
scala> var r = sc.parallelize(Seq.range(1, 100))
r: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[14] at parallelize at <console>:23

scala> var r2 = r.filter(x => x % 2 == 0)
r2: org.apache.spark.rdd.RDD[Int] = MapPartitionsRDD[15] at filter at <console>:23

scala> r2.reduce((x, y) => x + y)
res7: Int = 2450

scala> 
```

### Exercise 5 - Working with PairRDD (Key/Value)

In this exercise, lets create PairRDD from an array of String, split them by white space, put them in a PairRDD and then finally call `reduceByKey` action on it

```
scala> var r = sc.parallelize(Seq("Hello World", "Hello Singapore"))
r: org.apache.spark.rdd.RDD[String] = ParallelCollectionRDD[16] at parallelize at <console>:23

scala> var r2 = r.flatMap(x => x.split(" ")).map(x => (x, 1))
r2: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[19] at map at <console>:23

scala> var r3 = r2.reduceByKey((x, y) => x + y)
r3: org.apache.spark.rdd.RDD[(String, Int)] = ShuffledRDD[21] at reduceByKey at <console>:23

scala> r3.foreach(x => println(x))
(Hello,2)
(World,1)
(Singapore,1)

scala> 

```

### Exercise 6 - Wide Transformation & Actions on RDD

In this exercise, we're going to load 2 data files and then performing wide transformation & actions on it

Data files are kept at `/home/ubuntu/data/` folder

orders.txt - it is a comma separated file with this schema (Order Id, Order Date, Customer Id, Order Status)
order_items.txt - also a comma separated file with this schema (Order Item Id, Order Id, Product Id, Quantity, Total Price, Unit Price)

We're loading data files using `textfile` method and then splitting each line with comma (","), filter only COMPLETE order status, join them with corresponding order_items and find average sales by date, sort them by order date, take only top 5 sales


```
val ordersRDD = sc.textFile("/home/ubuntu/data/orders.txt");
val ordersMap = ordersRDD.map(x => x.split(",")).filter(x => x(3) == "COMPLETE").map(x => (x(0).toInt, x(1)));

val ordersItemsRDD = sc.textFile("/home/ubuntu/data/order_items.txt");
val orderItemsMap = ordersItemsRDD.map(x => x.split(",")).map(x => (x(1).toInt, x(4).toFloat));

val ordersJoinedMap = ordersMap.join(orderItemsMap);

val orderSalesMap = ordersJoinedMap.map(x => (x._2._1, x._2._2));

val salesPerDay = orderSalesMap.aggregateByKey((0.0, 0))((acc, revenue) => (acc._1 + revenue, acc._2 + 1), (total1, total2) => (total1._1 + total2._1, total1._2 + total2._2));

val avgSalesPerDay = salesPerDay.map(x => (x._1, (x._2._2, (x._2._1, x._2._1/x._2._2)))).sortByKey();

avgSalesPerDay.take(5).foreach(println);

```

The above code will produce following output

```
scala> avgSalesPerDay.take(5).foreach(x => println(x))
(2013-07-25,(96,(20030.320388793945,208.6491707166036)))
(2013-07-26,(220,(42165.8807926178,191.6630945118991)))
(2013-07-27,(168,(33156.210554122925,197.3583961554936)))
(2013-07-28,(137,(27012.910556793213,197.17452961162928)))
(2013-07-29,(232,(45898.65076828003,197.8390119322415)))

scala> 

```

### Exercise 7 - Spark SQL DataFrame (Create it from RDD)

We're going to perform the same example done on exercise 6 but using DataFrame

```
val ordersRDD = sc.textFile("/home/ubuntu/data/orders.txt")
val ordersMap = ordersRDD.map(x => x.split(",")).filter(x => x(3) == "COMPLETE").map(x => (x(0).toInt, x(1)))

val ordersItemsRDD = sc.textFile("/home/ubuntu/data/order_items.txt")
val orderItemsMap = ordersItemsRDD.map(x => x.split(",")).map(x => (x(1).toInt, x(4).toFloat))

val orderDF = ordersMap.toDF("order_id", "order_date")

val orderItemsDF = orderItemsMap.toDF("order_id", "order_total")

orderDF.createOrReplaceTempView("orders")

orderItemsDF.createOrReplaceTempView("order_items")

val avgSalesPerDay = spark.sql("SELECT o.order_date, COUNT(o.*) total_sales, SUM(oi.order_total) total_sales_amount, SUM(oi.order_total)/COUNT(o.*) avg_sales_amount FROM orders o JOIN order_items oi ON o.order_id=oi.order_id GROUP BY o.order_date ORDER BY o.order_date")

avgSalesPerDay.show(5)

```

The above code will produce following output

```
scala> avgSalesPerDay.show(5)
+----------+-----------+------------------+------------------+
|order_date|total_sales|total_sales_amount|  avg_sales_amount|
+----------+-----------+------------------+------------------+
|2013-07-25|         96|20030.320388793945| 208.6491707166036|
|2013-07-26|        220|  42165.8807926178| 191.6630945118991|
|2013-07-27|        168|33156.210554122925| 197.3583961554936|
|2013-07-28|        137|27012.910556793213|197.17452961162928|
|2013-07-29|        232| 45898.65076828003| 197.8390119322415|
+----------+-----------+------------------+------------------+
only showing top 5 rows

```

### Exercise 8 - Spark SQL DataFrame (using CSV)

We're going to perform the same example done on exercise 6 but using CSV API

```
val orderDF = spark.read.option("header", "false").option("inferSchema", "true").option("delimiter", ",").csv("/home/ubuntu/data/orders.txt").filter("_c3='COMPLETE'").selectExpr("_c0 as order_id", "_c1 as order_date")

val orderItemsDF = spark.read.option("header", "false").option("inferSchema", "true").option("delimiter", ",").csv("/home/ubuntu/data/order_items.txt").selectExpr("_c1 as order_id", "_c4 as order_total")

orderDF.createOrReplaceTempView("orders")

orderItemsDF.createOrReplaceTempView("order_items")

val avgSalesPerDay = spark.sql("SELECT o.order_date, COUNT(o.*) total_sales, SUM(oi.order_total) total_sales_amount, SUM(oi.order_total)/COUNT(o.*) avg_sales_amount FROM orders o JOIN order_items oi ON o.order_id=oi.order_id GROUP BY o.order_date ORDER BY o.order_date")

avgSalesPerDay.show(5)

```

The above code will produce following output

```
scala> avgSalesPerDay.show(5)
+----------+-----------+------------------+------------------+                  
|order_date|total_sales|total_sales_amount|  avg_sales_amount|
+----------+-----------+------------------+------------------+
|2013-07-25|         96|20030.319999999992| 208.6491666666666|
|2013-07-26|        220|          42165.88| 191.6630909090909|
|2013-07-27|        168|          33156.21|197.35839285714286|
|2013-07-28|        137| 27012.91000000001|197.17452554744534|
|2013-07-29|        232| 45898.65000000001| 197.8390086206897|
+----------+-----------+------------------+------------------+
only showing top 5 rows

```

## Spark Lab exercises using Java

Clone this project `https://github.com/sskumar77/SparkLab.git` for the below 2 exercises

### Exercise 9 - Spark RDD

In this example, we'll be loading ~2GB unstructured file which contains random alphanumeric data. We'll replace the alphabets with empty string and then count the occurence of each numbers.

Data file to be used is `/home/ubuntu/data/sample.data` and it is already kept in there

Finally, filtering out selected numbers and also save the output of RDD to folder `/home/ubuntu/data/sample.data/out`

Partially implemented solution can be found at  `com.spark.lab.spark_exercises.rdd.partial.RDDExample`

Full solution can be found at `com.spark.lab.spark_exercises.rdd.solution.RDDExample`

### Exercise 10 - Spark DataSet

In this example, we'll be analysis Singapore HDB's Resale Flat Prices data set (https://data.gov.sg/dataset/resale-flat-prices). 

Dataset is already downloaded and kept at `/home/ubuntu/data/hdb/`

We're going to perform following analysis

1. Get total number of Flats per Town (grouped by Flat Type)
2. Get list of Towns where the flats are built on or after 2000
3. Get top 10 resale flats (resale price $)

Partially implemented solution can be found at  `com.spark.lab.spark_exercises.ds.partial.DSExample`

Full solution can be found at `com.spark.lab.spark_exercises.ds.solution.DSExample`

## Spark Streaming Lab exercises using Spark Shell (Scala)

### Launch Spark-Shell

Open terminal and cd to SPARK_INSTALLATION_FOLDER/bin and type the below command

```
./spark-shell
```

### Exercise 11 - Word count using Spark Streaming 

It is a simple word count example using Socket as streaming source. On the `scala>` prompt, run the following commands:

```
val streamDF = spark.readStream.format("socket").option("host", "localhost").option("port", "1234").load();

val wordDF = streamDF.select(explode(split(streamDF("value"), " ")).alias("word"));

val count = wordDF.groupBy("word").count();

val query = count.writeStream.format("console").outputMode("complete").start().awaitTermination();
```

You've noticed, the above code doesn't produce any output. The above code listen to port `localhost:1234`. As soon as message starts flowing from the Socket (host/port), it will perform aggregation of data and show the output in console.

We're going to make use of Linux's `Netcat` utility to produce messages. Launch another terminal window and run following commands:

```
nc -l -p 1234

Hello World
Hello Singapore
```

As soon as you start producing data in streaming data source at localhost:1234, on the spark-shell, you could see the results gets printed on the console window. 

## Spark Streaming Lab exercises using Java

Clone this project `https://github.com/sskumar77/SparkLab.git` for the below 2 exercises

### Exercise 12 - Word count example using JavaStreamingContext

A simple word count exercise implemented using Java. This program listens to socket stream at localhost:1234. As soon as data start to flow in, it will perform aggregation and produces the results in console.

Use same `Netcat` utility as seen in previous exercise to input stream data

Partially implemented solution can be found at  `com.spark.lab.spark_exercises.streaming.partial.WordCountExample`

Full solution can be found at `com.spark.lab.spark_exercises.streaming.solution.WordCountExample`

### Exercise 13 - Structured data streaming using Spark SQL

In this exercise, we're going to listen to a directory where SG Air Temp public datasets (we'll use API to extract near real-time datasets) keep getting dropped every 5 seconds. Stream Processor listen to this directory and performs aggregation with old & newly generated data by weather stations (key: station_id) and display the results in console output.

SG Air Temp data producer program is kept at `com.spark.lab.spark_exercises.streaming.SGAirTempDataProducer`

For this stream data processor to run, you need to pass data folder as an argument.

Partially implemented solution can be found at  `com.spark.lab.spark_exercises.streaming.partial.SGAirTempDataProcessor`

Full solution can be found at `com.spark.lab.spark_exercises.streaming.solution.SGAirTempDataProcessor`

## Spark Cluster Setup

In order to setup Spark cluster, we are going to use Spark's CLI. We first need to setup Spark Master & then followed by worker(s)

Launch terminal and run following commands

```
cd /home/ubuntu/spark/sbin

nohup ./start-master.sh &
```

The above command will start Spark Master. You can access Spark Master by launching browser and go to `http://localhost:8080/`

Once Spark Master is started, we can now attach worker(s) to Spark Master. In this lab, we've single master/worker setup. But in your applicative environment, you may have many worker nodes to form Spark cluster. 

*** Remember you must install Spark on all your worker nodes. ***

Launch terminal and run following commands. You can get Spark Master URL from `http://localhost:8080` (note down URL showing on top of the page). Normally it starts with `spark://{host name}:{port number}`

```
cd /home/ubuntu/spark/sbin

nohup ./start-worker.sh {spark master URL} &
```

You can repeat the above step on all your worker nodes. To confirm if all worker nodes are joined to your cluster, access Spark Master and check `Workers` section

## Submitting Java application to Spark Cluster

Clone this project `https://github.com/sskumar77/SparkLab.git` for the below exercises

#### Exercise 14 - Word count (RDD) example

Let us build SparkLab application. Launch Eclipse IDE, either import or git clone `https://github.com/sskumar77/SparkLab.git`. And then, right-click your project, `Run > Maven Install`

Once maven build is successful, it will create Uber jar in your project's target folder.

##### Submitting application to Spark Cluster using spark-submit CLI

Launch terminal window, `cd` to your project's target folder and then run following command to launch word count program to spark cluster

```
cd /home/ubuntu/spark/bin

./spark-submit --class com.spark.lab.spark_exercises.rdd.solution.RDDExample --master <spark_master_url> --deploy-mode cluster --supervise <project_target_folder>/spark-lab-jar-with-dependencies.jar <input_data_file> <output_folder>
```

As soon as you see the message `Driver successfully submitted as <driver_id>`, you can launch browser and access spark-cluster to see the progress of your payload submitted to Spark Cluster

#### Submitting other exercises on Spark Cluster

Based on the above steps, please try to submit other solutions we had seen before on Spark Cluster and see the execution

## Spark Integration with Kafka

### Download & Install Confluent Kafka

Download Confluent Kafka (community edition) version 6.2.1 (http://packages.confluent.io/archive/6.2/confluent-community-6.2.0.tar.gz)

Launch terminal and run following commands to download & install Confluent Kafka

```
mkdir -p /home/ubuntu/kafka && cd /home/ubuntu/kafka
curl "http://packages.confluent.io/archive/6.2/confluent-community-6.2.0.tar.gz" -o kafka.tar.gz
tar -xvzf kafka.tar.gz  --strip 1
```

#### Start Zookeeper

To start Zookeeper, launch new terminal & run following commands

```
cd /home/ubuntu/kafka/bin
nohup ./zookeeper-server-start ../etc/kafka/zookeeper.properties &
```

To verify Zookeeper started properly, launch new terminal & run following command

```
jps
```

You should see `QuorumPeerMain` as one of a java process

#### Start Kafka Broker

To start Kafka broker, launch new terminal and run following commands

```
cd /home/ubuntu/kafka/bin
nohup ./kafka-server-start ../etc/kafka/server.properties &
```

To verify Kafka broker started properly, launch new terminal & run following command

```
jps
```

You should see `Kafka` as one of a java process

#### Create Kafka topics

For the below lab exercises (15 & 16), we need to create Kafka topics to send/receive data to/from spark. Launch terminal window and run following commands

```
cd /home/ubuntu/kafka/bin

./kafka-topics --bootstrap-server localhost:9092 --create --topic word_count --partitions 1 --replication-factor 1

./kafka-topics --bootstrap-server localhost:9092 --create --topic sg_temp --partitions 1 --replication-factor 1

```

### Spark Integration with Kafka Lab Exercise (Using Java)

#### Exercise 15 - Word count example (Spark as Kafka Data Consumer)

A simple word count exercise implemented using Java. This program listens to Kafka topic `word_count`. As soon as data start to flow in, it will perform aggregation and produces the results in console.

To use Kafka console producer to produce data, launch terminal window & run following command. Once you see the prompt sign `>`, start to type messages

```
cd /home/ubuntu/kafka/bin

./kafka-console-producer --bootstrap-server localhost:9092 --topic word_count

> Hello World
> Hello Singapore

```

Full solution can be found at `com.spark.lab.spark_exercises.kafka.SparkKafkaConsumer`

To submit this application on Spark cluster, please use following commands

```
cd /home/ubuntu/spark/bin

./spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 --class com.spark.lab.spark_exercises.kafka.SparkKafkaConsumer --master <spark_master_url> --deploy-mode cluster --supervise <full_path_of_project_target_folder>/spark-lab-jar-with-dependencies.jar
```

To verify the results, we'll be accessing Spark Cluster at `http://localhost:8080` access the running driver application and check `stdout` log.

#### Exercise 16 - SG Air Temp Data Analyser (Spark as Kafka Data Producer)

Based on the experience got from exercise 15, can you please come up with your own solution to push aggregated results (refer solution from Exercise 13), to kafka topic `sg_temp`?

Once your solution is ready, submit it to Spark cluster. 

To verify the results, we'll be using Kafka console consumer CLI.

To consume data in Kafka, we can use console consumer CLI, launch terminal window & run following command. As soon as the data start to be produced by Spark, you will start to see messages on the console

```
cd /home/ubuntu/kafka/bin

./kafka-console-consumer --bootstrap-server localhost:9092 --topic sg_temp

```






