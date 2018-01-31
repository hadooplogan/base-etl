package com.chinadaas.etl.sparksql;

import com.chinadaas.common.util.DataFrameUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * @author haoxing
 * merge两个增量数据来源
 */
public class UpdateES {
    //merge了人员的inc数据和照面的inc数据
    public static Dataset MergeToEs(SparkSession spark) {
        String hql = "select * from tmpbaseinc union all select * from tmppersoninc";

        return DataFrameUtil.getDataFrame(spark, hql, "IncData");

    }

}
