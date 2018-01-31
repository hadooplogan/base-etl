package com.chinadaas.etl.sparksql;

import com.chinadaas.common.util.DataFrameUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * @author haoxing
 *
 */
public class FullContactToEs {

    public static Dataset getFullContactToEs(SparkSession spark){

        String hql = "select id,\n" +
                "pripid,\n" +
                "credit_code,\n" +
                "entname,\n" +
                "tel,\n" +
                "email,\n" +
                "address,\n" +
                "position,\n" +
                "source,\n" +
                "date\n" +
                "from fullcontactinfo";

        return DataFrameUtil.getDataFrame(spark,hql,"tmpfull");

    }
}
