package com.spark.lab.spark_exercises.rdd.partial;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class RDDExample {

	public static void main(String[] args) {
		if (args.length != 2) {
			throw new IllegalArgumentException("Expected 2 arguments. 1) Input data file 2) Folder to store the output file");
		}
		String input_file = args[0];
		String output_file = args[1];
		try {
		    Files.walk(Paths.get(output_file))
		      .sorted(Comparator.reverseOrder())
		      .map(Path::toFile)
		      .forEach(File::delete);
		    
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
