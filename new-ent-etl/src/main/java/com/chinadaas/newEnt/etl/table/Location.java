package com.chinadaas.newEnt.etl.table;

import java.util.List;

/**
 * @author ShiLin
 * @since 2017-05-18
 */
public class Location {

	private String status;

	private String info;

	private String infocode;

	private int count;

	private List<Geo> geocodes;

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getInfo() {
		return info;
	}

	public void setInfo(String info) {
		this.info = info;
	}

	public String getInfocode() {
		return infocode;
	}

	public void setInfocode(String infocode) {
		this.infocode = infocode;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public List<Geo> getGeocodes() {
		return geocodes;
	}

	public void setGeocodes(List<Geo> geocodes) {
		this.geocodes = geocodes;
	}
}
