package com.chinadaas.common.util;


import com.chinadaas.common.common.Constants;
import com.chinadaas.common.common.CommonConfig;
import com.chinadaas.common.common.DatabaseValues;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;

import java.util.HashMap;

public class DataFrameUtil {
	/*
	 * Init Hive external table from hbase
	 */
	public static void InitTable(HiveContext sqlContext, String descTableName, String destFieldString,
			String srcTableName, String srcFieldString) {
		sqlContext.sql("CREATE EXTERNAL TABLE IF NOT EXISTS " + descTableName + " (key string, " + destFieldString
				+ ") " + "STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'   WITH SERDEPROPERTIES "
				+ "(\"hbase.columns.mapping\" = \":key," + srcFieldString
				+ "\" ) TBLPROPERTIES (\"hbase.table.name\" = \"" + srcTableName + "\")");
	}

	public static DataFrame getDataFrame(HiveContext sqlContext, String hql, String tmpTableName) {
		return getDataFrame(sqlContext, hql, tmpTableName, UNCACHE_TABLE);
	}

	public static final int UNCACHE_TABLE = -1;
	public static final int CACHETABLE_LAZY = 0;
	public static final int CACHETABLE_EAGER = 1;
	public static final int CACHETABLE_MAGIC = 2;
	public static final int CACHETABLE_PARQUET = 3;

	public static DataFrame getDataFrame(HiveContext sqlContext, String hql, String tmpTableName, int cacheMode) {
		DataFrame df = null;
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
				df.registerTempTable(tmpTableName);
				sqlContext.cacheTable(tmpTableName);
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
				String path = CommonConfig.getValue(DatabaseValues.CHINADAAS_CACHETABLE_PARQUET_PATH) + "/" + tmpTableName;
				DataFormatConvertUtil.deletePath(path);
				df.saveAsParquetFile(path);
				df = sqlContext.load(path);
				df.registerTempTable(tmpTableName);
				//sqlContext.cacheTable(tmpTableName);
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

	public static void uncacheTable(HiveContext sqlContext, String tableName) {
		sqlContext.uncacheTable(tableName);
	}

	public static DataFrame distinct2TemplateTable(HiveContext sqlContext, String sourceTablename,
			String templateTableName, String columnName) {
		String hql = "SELECT * " + "FROM " + sourceTablename + " " + "WHERE " + columnName + " IS NOT NULL AND "
				+ columnName + " <> ''";
		return DataFrameUtil.getDataFrame(sqlContext, hql, templateTableName);
	}

	public static DataFrame distinct2TemplateTable(HiveContext sqlContext, String sql, String templateTableName) {
		return DataFrameUtil.getDataFrame(sqlContext, sql, templateTableName);
	}

	public static String getNotNullSql(String columnName) {
		String sql = " " + columnName + " IS NOT NULL AND  " + columnName + "  <> '' ";
		return sql;
	}

	/*
	 * UDF Test
	 */
	public static void UDFTest(SparkContext sc, HiveContext sqlContext) {
		SQLContext sqlCtx = new SQLContext(sc);
		sqlCtx.udf().register("stringLengthTest", new UDF1<String, Integer>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = -6709911270475566751L;

			public Integer call(String str) {
				return str.length();
			}
		}, DataTypes.IntegerType);
		DataFrame dd = sqlContext.sql("SELECT stringLengthTest('test')");
		dd.show();
	}
	
	public static DataFrame saveAsCsv(DataFrame df, String path) {
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

	public static DataFrame saveASEs(DataFrame df,String path){
		return null;
	}

	/*public static DataFrame cacheTable(HiveContext sqlContext, String table, String cols, String conds) {
		String schema = DataFormatConvertUtil.getSchema();
		String sql = "CACHE TABLE " + table + " AS SELECT " + cols + " FROM " + schema + table;
		if (conds != null) {
			sql += " WHERE " + conds;
		}
		return sqlContext.sql(sql);
	}*/

}
