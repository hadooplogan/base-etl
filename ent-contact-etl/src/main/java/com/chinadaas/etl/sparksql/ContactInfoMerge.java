package com.chinadaas.etl.sparksql;

import com.chinadaas.common.util.DataFrameUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * @author haoxing
 */
public class ContactInfoMerge {
    /**
     * getContactinfo  为能够匹配上的在营企业的无区号号码补上区号。
     * @param spark
     * @return Dataset
     */
    public static  Dataset getcontact (SparkSession spark){

        String hql = "select * FROM (select * from personinfocontact union all select * from baseinfocontact union all select * from annulcontact  union all select * from taxcontact) a";

        return DataFrameUtil.getDataFrame(spark,hql,"contactunion");
    }


    public static Dataset association(SparkSession spark){

        String hql = "select \n" +
                "a.id,\n" +
                "a.pripid,\n" +
                "b.credit_code,\n" +
                "b.entname,\n" +
                "a.tel,\n" +
                "a.email,\n" +
                "a.address,\n" +
                "a.position,\n" +
                "a.source,\n" +
                "a.udt,\n" +
                "a.date \n" +
                "from contactunion a\n" +
                "left join enterprisebaseinfocollect b on a.pripid = b.pripid";

        return DataFrameUtil.getDataFrame(spark,hql,"tmpassociation");

    }

    public static Dataset getContactinfo(SparkSession spark){
      getcontact(spark);

     return   association(spark);

    }
}
