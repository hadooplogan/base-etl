package com.chinadaas.etl.sparksql;

import com.chinadaas.common.util.DataFrameUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class TimeEmail {
    private static Dataset getEmail(SparkSession spark){

        String sql = "select pripid,\n" +
                "email,\n" +
                "udt \n" +
                "from ent_contactinfo_full where email <> 'null'";

        return   DataFrameUtil.getDataFrame(spark,sql,"tmpemail");
    }


    private static Dataset getDescOrder(SparkSession spark){

        String sql =  "select pripid ,\n" +
                "email\n" +
                "from \n" +
                "(select pripid,\n" +
                "email,\n" +
                "row_number () over (partition by pripid ORDER BY udt desc) rn from  tmpemail) a\n" +
                "where a.rn =1";

        return DataFrameUtil.getDataFrame(spark,sql,"tmporder");
    }



    public static Dataset getExtracEmail(SparkSession spark){
        getEmail(spark);
     return  getDescOrder(spark);


    }
}
