package com.chinadaas.association.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import com.chinadaas.association.common.CommonConfig;
import com.chinadaas.association.common.DatabaseValues;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
	static private final String TAIKANG_DATAFORMAT = "yyyy-MM-dd hh:mm:ss";
	static public String date2String(Date date) {
		if (date != null) {
			SimpleDateFormat df = new SimpleDateFormat(TAIKANG_DATAFORMAT);
			return df.format(date);
		} else {
			return null;
		}
	}
	static public Date string2Date(String str) {
		if (str != null) {
			SimpleDateFormat df = new SimpleDateFormat(TAIKANG_DATAFORMAT);
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
	
	public static String getSchema() {
		return CommonConfig.getValue(DatabaseValues.CHINADAAS_HIVE_ASSOCIATION_SCHEMA);
	}

	
}