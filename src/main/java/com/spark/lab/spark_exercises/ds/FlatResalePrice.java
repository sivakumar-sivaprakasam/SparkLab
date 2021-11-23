package com.spark.lab.spark_exercises.ds;

import java.io.Serializable;
import java.util.Date;

public class FlatResalePrice implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 8283202466058510174L;

	Date month;
	String town;
	String flat_type;
	String block;
	String street_name;
	String storey_range;
	int floor_area_sqm;
	String flat_model;
	Date lease_commence_date;
	String remaining_lease;
	int resale_price;

	public Date getMonth() {
		return month;
	}

	public void setMonth(Date month) {
		this.month = month;
	}

	public String getTown() {
		return town;
	}

	public void setTown(String town) {
		this.town = town;
	}

	public String getFlat_type() {
		return flat_type;
	}

	public void setFlat_type(String flat_type) {
		this.flat_type = flat_type;
	}

	public String getBlock() {
		return block;
	}

	public void setBlock(String block) {
		this.block = block;
	}

	public String getStreet_name() {
		return street_name;
	}

	public void setStreet_name(String street_name) {
		this.street_name = street_name;
	}

	public String getStorey_range() {
		return storey_range;
	}

	public void setStorey_range(String storey_range) {
		this.storey_range = storey_range;
	}

	public int getFloor_area_sqm() {
		return floor_area_sqm;
	}

	public void setFloor_area_sqm(int floor_area_sqm) {
		this.floor_area_sqm = floor_area_sqm;
	}

	public String getFlat_model() {
		return flat_model;
	}

	public void setFlat_model(String flat_model) {
		this.flat_model = flat_model;
	}

	public Date getLease_commence_date() {
		return lease_commence_date;
	}

	public void setLease_commence_date(Date lease_commence_date) {
		this.lease_commence_date = lease_commence_date;
	}

	public String getRemaining_lease() {
		return remaining_lease;
	}

	public void setRemaining_lease(String remaining_lease) {
		this.remaining_lease = remaining_lease;
	}

	public int getResale_price() {
		return resale_price;
	}

	public void setResale_price(int resale_price) {
		this.resale_price = resale_price;
	}

}
