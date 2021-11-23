package com.spark.lab.spark_exercises.ds.partial;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DSExample {
	public static void main(String[] args) {
		if (args.length != 1) {
			throw new IllegalArgumentException();
		}
		String input_file = args[0];

		// Creating Spark Conf object
		// setMaster(local[*]) -- will run Spark on Local JVM mode
		// setMaster(Master URL) -- will run Spark on Cluster Mode
		SparkSession spark = SparkSession.builder().appName("HDB Analyser").master("local[*]").getOrCreate();

		// Reading the input files using CSV API
		// Use encoders to convert it to Java Bean
		Dataset<Row> ds = spark.read()
				.option("delimiter", ",")
				.option("header", "true")
				.option("inferSchema", "false")
				.csv(input_file);

		// Register the Dataset with temp view
		ds.createOrReplaceTempView("hdb_data");
		
		// Querying on the view
		// Select list of Town, Flat Type
		
		// Select list of Town, Flat Types built on or after year 2000
		
		// Select top 10 flats in terms of its resale price

		spark.close();
		spark.stop();
	}
}