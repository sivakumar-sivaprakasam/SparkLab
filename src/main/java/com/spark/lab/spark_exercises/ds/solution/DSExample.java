package com.spark.lab.spark_exercises.ds.solution;

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
		Dataset<Row> ds = spark.read().option("delimiter",  ",").option("header", true).option("inferSchema", true).csv(input_file);
		ds.printSchema();
		ds.createOrReplaceTempView("hdb_data");
		ds.show();
		
		spark.close();
		spark.stop();
	}
}
