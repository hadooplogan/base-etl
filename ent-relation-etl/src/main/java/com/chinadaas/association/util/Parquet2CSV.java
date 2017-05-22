package com.chinadaas.association.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import com.chinadaas.association.common.CommonConfig;
import com.chinadaas.association.common.DatabaseValues;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

public class Parquet2CSV {
	public static void main(String[] args) {
		Calendar cal = Calendar.getInstance();
		Date dt = cal.getTime();
		SimpleDateFormat sdf = new SimpleDateFormat("_yyyy-MM-dd");
		String strdt = sdf.format(dt);
		String input = null;
		if (args.length > 0) {
			input = args[0];
		} else {
			input = CommonConfig.getValue(DatabaseValues.CHINADAAS_CACHETABLE_PARQUET_PATH) + strdt;
		}
		String output = null;
		if (args.length > 1) {
			output = args[1];
		} else {
			output = CommonConfig.getValue(DatabaseValues.CHINADAAS_CACHETABLE_PARQUET_PATH) + strdt;
		}
		
		SparkConf conf = new SparkConf().setAppName("Taikang PARQUET TO CSV ETL");
		SparkContext sc = new SparkContext(conf);

		HiveContext sqlContext = new HiveContext(sc);
		
		DataFrame df = sqlContext.load(input);
		df.repartition(1);
		
		DataFrameUtil.saveAsCsv(df, output);
		sqlContext.clearCache();
		sc.stop();
	}
}
