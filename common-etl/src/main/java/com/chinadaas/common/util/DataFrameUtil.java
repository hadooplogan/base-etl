package com.chinadaas.common.util;


import com.chinadaas.common.common.Constants;
import com.chinadaas.common.common.CommonConfig;
import com.chinadaas.common.common.DatabaseValues;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import java.io.IOException;
import java.util.HashMap;

public class DataFrameUtil {
	/*
	 * Init Hive external table from hbase
	 */
	public static void InitTable(SparkSession sqlContext, String descTableName, String destFieldString,
								 String srcTableName, String srcFieldString) {
		sqlContext.sql("CREATE EXTERNAL TABLE IF NOT EXISTS " + descTableName + " (key string, " + destFieldString
				+ ") " + "STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'   WITH SERDEPROPERTIES "
				+ "(\"hbase.columns.mapping\" = \":key," + srcFieldString
				+ "\" ) TBLPROPERTIES (\"hbase.table.name\" = \"" + srcTableName + "\")");
	}

	public static Dataset getDataFrame(SparkSession sqlContext, String hql, String tmpTableName) {
		return getDataFrame(sqlContext, hql, tmpTableName, UNCACHE_TABLE);
	}

	public static final int UNCACHE_TABLE = -1;
	public static final int CACHETABLE_LAZY = 0;
	public static final int CACHETABLE_EAGER = 1;
	public static final int CACHETABLE_MAGIC = 2;
	public static final int CACHETABLE_PARQUET = 3;

	public static Dataset getDataFrame(SparkSession sqlContext, String hql, String tmpTableName, int cacheMode) {
		Dataset df = null;
		if (cacheMode != CACHETABLE_MAGIC) {
			df = sqlContext.sql(hql);
		}
		switch (cacheMode) {
			case UNCACHE_TABLE:
			{
				df = sqlContext.sql(hql);
				df.registerTempTable(tmpTableName);
				break;
			}
			case CACHETABLE_LAZY: // memory async cache
			{
				df.cache().registerTempTable(tmpTableName);
				break;
			}
			case CACHETABLE_EAGER: // memory sync cache
			{
				df.registerTempTable(tmpTableName);
				sqlContext.sql("CACHE TABLE " + tmpTableName);
				break;
			}
			case CACHETABLE_MAGIC:
			{
				df = sqlContext.sql("CACHE TABLE " + tmpTableName + " AS " + hql);
				df.registerTempTable(tmpTableName);
				break;
			}
			case CACHETABLE_PARQUET: // parquet cache
			{
				String path = CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_PARQUET_TMP)  + tmpTableName;
				DataFormatConvertUtil.deletePath(path);
				df.write().mode(SaveMode.Overwrite).parquet(path);
				sqlContext.read().load(path).registerTempTable(tmpTableName);
				break;
			}
			default:
			{
				df = sqlContext.sql(hql);
				df.registerTempTable(tmpTableName);
				break;
			}
		}
		return df;
	}


	public static Dataset distinct2TemplateTable(SparkSession sqlContext, String sourceTablename,
			String templateTableName, String columnName) {
			String hql = "SELECT * " + "FROM " + sourceTablename + " " + "WHERE " + columnName + " IS NOT NULL AND "
				+ columnName + " <> ''";
		return DataFrameUtil.getDataFrame(sqlContext, hql, templateTableName);
	}

	public static Dataset distinct2TemplateTable(SparkSession sqlContext, String sql, String templateTableName) {
		return DataFrameUtil.getDataFrame(sqlContext, sql, templateTableName);
	}

	public static String getNotNullSql(String columnName) {
		String sql = " " + columnName + " IS NOT NULL AND  " + columnName + "  <> '' ";
		return sql;
	}

	/*
	 * UDF Test
	 */
	public static void UDFTest(SparkContext sc, SparkSession sqlContext) {
		SQLContext sqlCtx = new SQLContext(sc);
		sqlCtx.udf().register("stringLengthTest", new UDF1<String, Integer>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = -6709911270475566751L;

			@Override
			public Integer call(String str) {
				return str.length();
			}
		}, DataTypes.IntegerType);
		Dataset dd = sqlContext.sql("SELECT stringLengthTest('test')");
		dd.show();
	}
	
	public static Dataset saveAsCsv(Dataset df, String path) {
		HashMap<String, String> saveOptions = new HashMap<String, String>();
		saveOptions.put("path", path);
		saveOptions.put("header", CommonConfig.getValue(Constants.ASSOCIATION_CSV_HEADER));
		saveOptions.put("delimiter", CommonConfig.getValue(Constants.ASSOCIATION_CSV_DELIMITER));
		saveOptions.put("nullValue", CommonConfig.getValue(Constants.ASSOCIATION_CSV_NULLVALUE));
		System.out.println(path);
		System.out.println("delimiter"+ CommonConfig.getValue(Constants.ASSOCIATION_CSV_DELIMITER));
		df.write().mode(SaveMode.Overwrite).format("com.databricks.spark.csv").options(saveOptions).save();
		return df;
	}


	public static Dataset saveAsCsvAppend(Dataset df, String path) {
		HashMap<String, String> saveOptions = new HashMap<String, String>();
		saveOptions.put("path", path);
		saveOptions.put("header", CommonConfig.getValue(Constants.ASSOCIATION_CSV_HEADER));
		saveOptions.put("delimiter", CommonConfig.getValue(Constants.ASSOCIATION_CSV_DELIMITER));
		saveOptions.put("nullValue", CommonConfig.getValue(Constants.ASSOCIATION_CSV_NULLVALUE));
		System.out.println(path);
		System.out.println("delimiter"+ CommonConfig.getValue(Constants.ASSOCIATION_CSV_DELIMITER));
		df.write().mode(SaveMode.Append).format("com.databricks.spark.csv").options(saveOptions).save();
		return df;
	}

	public static Dataset saveAsParquetOverwrite(Dataset df, String path) {

		DataFormatConvertUtil.deletePath(path);

		if(!DataFormatConvertUtil.isExistsPath(path)){
			DataFormatConvertUtil.mkdir(path);
		}

		df.write().mode(SaveMode.Overwrite).parquet(path);
		return df;
	}

	public static Dataset saveAsTextFileOverwrite(Dataset df,String path){

		DataFormatConvertUtil.deletePath(path);

		if(!DataFormatConvertUtil.isExistsPath(path)){
			DataFormatConvertUtil.mkdir(path);
		}
        //临时设置文件输出分区为1分区。
		df.repartition(1).write().mode(SaveMode.Overwrite).text(path);
		return df;

	}


	/**
	 * 熔断文件判断程序是否执行成功
	 *
	 * @return
	 */
	public static boolean saveAsFlag(String path){
		try {

			return	FileSystem.newInstance(new Configuration()).createNewFile(new Path(path));

		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * 删除熔断文件
	 *
	 * @return
	 */
	public static boolean deleteFlag(String path){
		try {

			return	FileSystem.newInstance(new Configuration()).delete(new Path(path),true);

		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * 检查熔断文件
	 *
	 * @return
	 */
	public static boolean checkFlag(String path){
		try {
			return	FileSystem.newInstance(new Configuration()).isFile(new Path(path));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}


}
