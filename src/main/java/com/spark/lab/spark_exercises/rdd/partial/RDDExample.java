package com.spark.lab.spark_exercises.rdd.partial;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

public class RDDExample {

	public static void main(String[] args) {
		if (args.length != 1) {
			throw new IllegalArgumentException();
		}
		String input_file = args[0];
		String output_file = input_file + "/out";
		try {
			Files.deleteIfExists(Paths.get(output_file));
		} catch (IOException e) {
			e.printStackTrace();
		}

		// Creating Spark Conf object
		// setMaster(local[*]) -- will run Spark on Local JVM mode
		// setMaster(Master URL) -- will run Spark on Cluster Mode
		SparkConf sparkConf = new SparkConf().setAppName("Demo").setMaster("local[*]");

		// Creating JavaSparkContext object from SparkConf
		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		// Reading the input file
		// This reads with default partition
		JavaRDD<String> inputRDD = sc.textFile(input_file);
		
		// TODO: Try to repartition the RDD
		
		

		// Map all non-numbers to empty string
		JavaRDD<String> mappedRDD = inputRDD.map(x -> x.replaceAll("[^0-9]", " "));

		// Flatten the list of string & filter all non-empty string
		JavaRDD<String> filteredRDD = mappedRDD.flatMap(x -> Arrays.asList(x.split(" ")).iterator())
				.filter(x -> x.toString().trim() != "");

		// For every numbered items, assign it with value 1
		JavaPairRDD<String, Integer> pairRDD = filteredRDD.mapToPair(x -> new Tuple2<String, Integer>(x.toString(), 1));

		// Perform aggregate operation on it
		JavaPairRDD<String, Integer> resultedRDD = pairRDD.reduceByKey((x, y) -> x + y);

		// TODO: Print the size of final result RDD
		

		// TODO: Filter RDD where key = 10 and print the key and value
		
		

		// TODO: To improve performance, we're going to persist the RDD
		
		

		// TODO: Filter RDD where key = 10 or 20 and print the key and value
		
		

		// TODO: Save result in a file

		sc.stop();
		sc.close();
	}

}