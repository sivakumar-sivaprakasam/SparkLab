package com.spark.lab.spark_exercises.ds.solution;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;

import com.spark.lab.spark_exercises.ds.FlatResalePrice;

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
		Dataset<FlatResalePrice> ds = spark.read()
				.option("delimiter", ",")
				.option("header", "true")
				.option("inferSchema", "false")
				.csv(input_file)
				.as(ExpressionEncoder.javaBean(FlatResalePrice.class));

		// Register the Dataset with temp view
		ds.createOrReplaceTempView("hdb_data");
		
		// Querying on the view
		// Select list of Town, Flat Type
		spark.sql("SELECT town, flat_type, COUNT(*) total_flats FROM hdb_data GROUP BY town, flat_type ORDER BY town, flat_type").show();
		
		// Select list of Town, Flat Types built on or after year 2000
		spark.sql("SELECT town, flat_type, COUNT(*) total_flats FROM hdb_data WHERE year(month) >= 2000 GROUP BY town, flat_type ORDER BY town, flat_type").show();
		
		// Select top 10 flats in terms of its resale price
		spark.sql("SELECT * FROM hdb_data ORDER BY INT(resale_price) desc LIMIT 10").show();

		spark.close();
		spark.stop();
	}
}