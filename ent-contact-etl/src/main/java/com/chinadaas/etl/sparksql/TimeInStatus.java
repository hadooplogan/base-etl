package com.chinadaas.etl.sparksql;

import com.chinadaas.common.util.DataFrameUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * @author haoxing
 */
public class TimeInStatus {
    public static Dataset getInStatus(SparkSession spark, String date) {

        String sql = "select pripid,\n" +
                "credit_code,\n" +
                "entname,\n" +
                "" + date + " as date \n" +
                "from base_full \n" +
                "where pripid <> '' and entstatus = '1'";

        return DataFrameUtil.getDataFrame(spark, sql, "tmpbase");
    }
}
