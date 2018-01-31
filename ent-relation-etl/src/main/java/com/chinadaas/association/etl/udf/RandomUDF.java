package com.chinadaas.association.etl.udf;

import java.util.Random;

import com.chinadaas.common.util.IDUtil;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;


public class RandomUDF {
    public static final String ALLCHAR = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";  
    public static final String LETTERCHAR = "abcdefghijkllmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";  
    public static final String NUMBERCHAR = "0123456789"; 
	/*
	 * generate random number
	 */
	public static void genrandom(SparkSession spark) {
		spark.udf().register("genrandom", new UDF1<String, String>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = -6709911270475566751L;

			public String call(String str) { 
				return IDUtil.getUUID()+str;
			}
		}, DataTypes.StringType);
	}
	
	public static String generateMixString(int length) {  
        StringBuffer sb = new StringBuffer();  
        Random random = new Random();  
        for (int i = 0; i < length; i++) {  
            sb.append(ALLCHAR.charAt(random.nextInt(LETTERCHAR.length())));  
        }  
        return sb.toString();  
    }
}
