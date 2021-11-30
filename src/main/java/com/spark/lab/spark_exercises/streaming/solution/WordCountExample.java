package com.spark.lab.spark_exercises.streaming.solution;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class WordCountExample {
	private static final int PORT = 1234;
	private static final String HOST_NAME = "localhost";

	public static void main(String[] args) throws InterruptedException {
		SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("Word Count");
		// Creating JavaStreamingContext object with duration of 5 seconds
		JavaStreamingContext jsc = new JavaStreamingContext(sparkConf, Durations.seconds(5));
		
		// Listen to port 1234 on localhost
		JavaReceiverInputDStream<String> streamLine = jsc.socketTextStream(HOST_NAME, PORT);
		
		JavaDStream<String> streamWords = streamLine.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
		
		JavaPairDStream<String, Integer> pairStream = streamWords.mapToPair(x -> new Tuple2(x, 1));
		JavaPairDStream<String, Integer> counts = pairStream.reduceByKey((x, y) -> x + y);
		
		counts.print();
		
		// Starting Streaming Context listener
		jsc.start();
		
		// Shutdown hook
		jsc.awaitTermination();
	}
}
