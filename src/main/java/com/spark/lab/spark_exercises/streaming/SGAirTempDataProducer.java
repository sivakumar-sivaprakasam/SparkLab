package com.spark.lab.spark_exercises.streaming;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.apache.spark.streaming.Durations;
import org.json.JSONArray;
import org.json.JSONObject;

public class SGAirTempDataProducer {
	private static final String SG_API_URL = "https://api.data.gov.sg/v1/environment/air-temperature?date_time=%s";
	private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("YYYY-MM-dd'T'HH:mm:ss");
	private static final DateTimeFormatter dtfFile = DateTimeFormatter.ofPattern("YYYYMMddHHmmss");

	public static void main(String[] args) throws Exception {
		if (args.length != 1) {
			throw new IllegalArgumentException("Please enter the folder to save the API output");
		}
		try {
			while (true) {
				String fileNameToSave = args[0] + java.io.File.separatorChar + dtfFile.format(LocalDateTime.now()) + ".json";
				URL url = new URL(String.format(SG_API_URL, dtf.format(LocalDateTime.now())));
				HttpURLConnection conn = (HttpURLConnection) url.openConnection();
				conn.setRequestMethod("GET");
				conn.setRequestProperty("Accept", "application/json");
				if (conn.getResponseCode() != 200) {
					throw new RuntimeException("Failed : HTTP Error code : " + conn.getResponseCode());
				}
				try (InputStream in = conn.getInputStream(); BufferedWriter fos = new BufferedWriter(new FileWriter(fileNameToSave))) {
					JSONObject json = new JSONObject(new String(in.readAllBytes()));
					JSONArray items = json.getJSONArray("items");
					fos.write(items.getJSONObject(0).getJSONArray("readings").toString());
				}

				// Wait for 5 seconds
				Thread.sleep(Durations.seconds(5).milliseconds());
			}
		} catch (Exception e) {
			throw e;
		}
	}
}
