package com.chinadaas.common.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DataFormatConvertUtil {
	
	private static final boolean DEBUG_FLAG = true;
	
	/************ add data to hdfs **************/
	static public void writeData(DataOutput output, Date dt) throws IOException {
		if (dt == null) {
			output.writeLong(0);
		} else {
			output.writeLong(dt.getTime());
		}
	}
	static public void writeData(DataOutput output, String value) throws IOException {
		if (value == null) {
			output.writeUTF("");
		} else {
			output.writeUTF(value);
		}
	}
	static public void writeData(DataOutput output, double value) throws IOException {
		output.writeDouble(value);
	}
	static public void writeData(DataOutput output, int value) throws IOException {
		output.writeInt(value);
	}
	static public void writeData(DataOutput output, long value) throws IOException {
		output.writeLong(value);
	}
	
	/************ read data from hdfs **************/
	static public Date readDate(DataInput input) throws IOException {
		long v = input.readLong();
		if (v == 0) {
			return null;
		} else {
			return new Date(v);
		}
	}
	
	/************ date & string **************/
	static private final String CHINADAAS_DATAFORMAT = "yyyy-MM-dd hh:mm:ss";
	static public String date2String(Date date) {
		if (date != null) {
			SimpleDateFormat df = new SimpleDateFormat(CHINADAAS_DATAFORMAT);
			return df.format(date);
		} else {
			return null;
		}
	}
	static public Date string2Date(String str) {
		if (str != null) {
			SimpleDateFormat df = new SimpleDateFormat(CHINADAAS_DATAFORMAT);
			df.setLenient(false);
			try {
				return df.parse(str);
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
		return null;
	}
	
	static public int string2int(String str) {
		if (str != null && str.length() > 0) {
			try {
				return Integer.valueOf(str);
			} catch (Exception e) {
			}
		}
		return 0;
	}
	

	public static void deletePath(String output) {
		Configuration conf = new Configuration();
		FileSystem filesystem;
		try {
			filesystem = FileSystem.get(conf);
			if (filesystem.exists(new Path(output))) {
				filesystem.delete(new Path(output), true);
			}		
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


	public static void mkdir(String output) {
		Configuration conf = new Configuration();
		FileSystem filesystem;
		try {
			filesystem = FileSystem.get(conf);
			if (!filesystem.exists(new Path(output))) {
				filesystem.mkdirs(new Path(output));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static boolean isExistsPath(String output) {
		boolean ret = false;
		Configuration conf = new Configuration();
		FileSystem filesystem;
		try {
			filesystem =  FileSystem.get(conf);
			if (filesystem.exists(new Path(output))) {
				ret = true;
			}	
		} catch (IOException e) {
			e.printStackTrace();
		}
		return ret;
	}

	public static boolean isValidDate(String sDate) {
		if(sDate==null){
			return false;
		}
		DateFormat formatter = new SimpleDateFormat("yyyyMMdd");
		try{
			Date date = (Date)formatter.parse(sDate);
			return sDate.equals(formatter.format(date));
		}catch(Exception e){
			return false;
		}
	}

	public static void main(String[] args){
		String date="20170801";

	}

	
}