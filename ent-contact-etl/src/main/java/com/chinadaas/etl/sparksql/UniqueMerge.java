package com.chinadaas.etl.sparksql;

import com.chinadaas.common.util.DataFrameUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * @author haoxing
 * date
 */
public class UniqueMerge {

    public static Dataset getMerge(SparkSession spark) {

        String hql = "select a.pripid,\n" +
                "a.credit_code,\n" +
                "a.entname,\n" +
                "b.tel,\n" +
                "c.email,\n" +
                "d.address," +
                "a.date\n" +
                "from tmpbase a \n" +
                "left join tmptel b on a.pripid = b.pripid\n" +
                "left join tmpemail c on a.pripid = c.pripid\n"+
                "left join tmpaddr d on a.pripid = d.pripid";

        return DataFrameUtil.getDataFrame(spark, hql, "tmpuniquemerged");

    }

    public static Dataset getUniqueContactMerge(SparkSession spark) {

        return getMerge(spark);

    }
}
