package com.chinadaas.common.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class TimeUtil {

    private static SimpleDateFormat dfdays = new SimpleDateFormat("yyyy-MM-dd");

	public static String getNowStr() {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");// 设置日期格式
		return df.format(new Date());
	}


	public static String getNowStrYMD() {
		SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");// 设置日期格式
		return df.format(new Date());
	}

	public static String getNowYMDStr() {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");// 设置日期格式
		return df.format(new Date());
	}
	
	public static Date string2Time(String str) {
		if (str != null) {
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			df.setLenient(false);
			try {
				return df.parse(str);
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
		return null;
	}
	
	public static Date string2YMDHMSDate(String str) {
		if (str != null) {
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			df.setLenient(false);
			try {
				return df.parse(str);
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
		return null;
	}
	
	public static Date string2YMDDate(String str) {
		if (str != null) {
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
			df.setLenient(false);
			try {
				return df.parse(str);
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
		return null;
	}
/**
 * 得到当前月
 * @return
 */

public static int getMonth(){
	Calendar now = Calendar.getInstance();
	return  now.get(Calendar.MONTH) + 1;

}
	/**
	 * 得到当前年
	 * @return
	 */
	public static int getYear(){
		Calendar now = Calendar.getInstance();
		return now.get(Calendar.YEAR);
	}

	/**
	 *  得到几年前的日期  yyyy-mm-dd
	 * @param year
	 * @return
	 */
	public static String getYearAgo(int year) {
		Calendar now = Calendar.getInstance();
        now.add(Calendar.YEAR, -year);
        String dateStr = dfdays.format(now.getTime());
		return dateStr;
	}

	public static void main(String[] args) {

		String date = "20170831";
		String id = date+"_" +TimeUtil.getNowStrYMD();
		System.out.println(id);
		String path = "/riskbell/master/"+id;

		System.out.println(path);

//		getYearAgo(1);
//	getNowYMDStr();
//		System.out.println(getNowStrYMD());
	}
}
