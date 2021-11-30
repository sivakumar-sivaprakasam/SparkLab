package com.spark.lab.spark_exercises.streaming.solution;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;

public class SGAirTempDataProcessor {
	public static void main(String[] args) throws StreamingQueryException, TimeoutException {
		if (args.length != 1) {
			throw new IllegalArgumentException("Please enter the folder to read input files");
		}
		SparkSession spark = SparkSession.builder().master("local[*]").appName("SG Air Temp Processor").getOrCreate();

		StructType schema = new StructType(
				new StructField[] { new StructField("station_id", DataTypes.StringType, false, Metadata.empty()),
						new StructField("value", DataTypes.FloatType, false, Metadata.empty()) });

		Dataset<Row> airDF = spark.readStream().format("json").schema(schema).option("latestFirst", "true")
				.option("cleanSource", "delete").load(args[0]);

		Map<String, String> aggExpr = new HashMap<>();
		aggExpr.put("value", "avg");
		aggExpr.put("station_id", "count");
		Dataset<Row> aggDF = airDF.groupBy("station_id").agg(aggExpr);
		aggDF.writeStream().outputMode("complete").format("console")
				.trigger(Trigger.ProcessingTime(Durations.seconds(30).milliseconds())).start().awaitTermination();

		// Alternatively we can use Spark SQL queries as below
//		aggDF.writeStream().queryName("sg_air_temp").outputMode("complete").format("console")
//				.trigger(Trigger.ProcessingTime(Durations.seconds(30).milliseconds())).start().awaitTermination();
//		spark.sql("SELECT * FROM sg_air_temp ORDER BY station_id").show();
	}
}
