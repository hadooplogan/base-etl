package com.chinadaas.etl.sparksql;

import com.chinadaas.common.load.impl.ParquetDataAdapterImpl;
import com.chinadaas.common.util.DataFrameUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class MockData {

    public static Dataset getMockData(SparkSession spark){

        String hql = "select distinct pripid from entcontact_extract limit 1000000";

        return DataFrameUtil.getDataFrame(spark,hql,"Mock");
    }


}
