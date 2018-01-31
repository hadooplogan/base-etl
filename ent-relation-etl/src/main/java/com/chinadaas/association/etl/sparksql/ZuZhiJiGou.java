package com.chinadaas.association.etl.sparksql;

import com.chinadaas.common.util.DataFrameUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class ZuZhiJiGou {
    public  static Dataset InformationGainStats(SparkSession spark){
        String sql = "select * from s_ogz_baseinfo";

    return DataFrameUtil.getDataFrame(spark,sql,"tmpogz");
    }

}
