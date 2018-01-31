package com.chinadaas.etl.sparksql;

import com.chinadaas.common.util.DataFrameUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class UpdateExtraIndex {
    public static Dataset getUpdateExtraIndex(SparkSession spark){

        String hql = "select * from baseinc";

        return DataFrameUtil.getDataFrame(spark,hql,"baseinces");
    }

}
