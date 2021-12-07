package com.spark.lab.spark_exercises.kafka;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.streaming.Durations;

public class SparkKafkaConsumer {
	public static void main(String[] args) throws InterruptedException, StreamingQueryException, TimeoutException {

		SparkSession spark = SparkSession.builder().appName("Spark Kafka Consumer").master("local[*]").getOrCreate();
		Dataset<Row> df = spark.readStream()
				.format("kafka")
				.option("kafka.bootstrap.servers", "localhost:9092")
				.option("subscribe", "word_count")
				.load()
				.selectExpr("CAST(key AS STRING) as rec_key", "CAST(value AS STRING) as rec_msg");
		
		Map<String, String> aggExpr = new HashMap<>();
		aggExpr.put("rec_msg", "count");
		Dataset<Row> aggDF = df.groupBy("rec_msg").agg(aggExpr);

		aggDF.writeStream().outputMode("complete").format("console")
				.trigger(Trigger.ProcessingTime(Durations.seconds(30).milliseconds()))
				.start()
				.awaitTermination();
	}
}
