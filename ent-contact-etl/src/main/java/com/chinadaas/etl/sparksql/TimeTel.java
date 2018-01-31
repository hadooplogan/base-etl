package com.chinadaas.etl.sparksql;

import com.chinadaas.common.util.DataFrameUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * @author haoxing
 *
 * 按照数据的更新日期来，来使用数据更新日期最新的批次数据。
 */
public class TimeTel {

    private static Dataset getTel(SparkSession spark){

        String sql = "select pripid,\n" +
                "tel,\n" +
                "udt \n" +
                "from ent_contactinfo_full where tel <> 'null'";

      return   DataFrameUtil.getDataFrame(spark,sql,"tmptel");
    }


    private static Dataset getDescOrder(SparkSession spark){

        String sql = "select pripid,\n" +
                "tel\n" +
                "from \n" +
                "(select pripid,\n" +
                "tel,\n" +
                "row_number () over (partition by pripid ORDER BY udt desc) rn from  tmptel) a\n" +
                "where a.rn =1";

       return DataFrameUtil.getDataFrame(spark,sql,"tmporder");
    }



    public static Dataset getExtractTel(SparkSession spark){
        getTel(spark);
     return   getDescOrder(spark);



    }
}
