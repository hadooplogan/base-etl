package com.chinadaas.etl.sparksql;

import com.chinadaas.common.util.DataFrameUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * @author haoxing
 */
public class TImeAddress {
    private static Dataset getAddress(SparkSession spark){

        String sql = "select pripid,\n" +
                "address,\n" +
                "udt \n" +
                "from ent_contactinfo_full where address <> 'null' and length(trim(address)) > 6";

        return   DataFrameUtil.getDataFrame(spark,sql,"tmpaddress");
    }


    private static Dataset getDescOrder(SparkSession spark){

        String sql =  "select pripid ,\n" +
                "address\n" +
                "from \n" +
                "(select pripid,\n" +
                "address,\n" +
                "row_number () over (partition by pripid ORDER BY udt desc) rn from  tmpaddress) a\n" +
                "where a.rn =1";

        return DataFrameUtil.getDataFrame(spark,sql,"tmporder");
    }



    public static Dataset getExtracAddress(SparkSession spark){
        getAddress(spark);
      return getDescOrder(spark);


    }

}
