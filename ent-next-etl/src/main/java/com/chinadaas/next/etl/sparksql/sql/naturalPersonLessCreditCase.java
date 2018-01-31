package com.chinadaas.next.etl.sparksql.sql;

import com.chinadaas.common.util.DataFrameUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class naturalPersonLessCreditCase {

    private static Dataset tmpForJoin1(SparkSession spark, String datadate){
        String hql = "select a.zspid ,b.pripid from \n" +
                "(select zspid,pripid from e_inv_investment where pripid is not null and pripid <> '' and invtype in('20','21','22','35','50')) a \n" +
                " inner join\n" +
                "(select pripid from enterprisebaseinfocollect_full_20170927 where pripid is not null and pripid <> '') b \n" +
                "on a.pripid = b.pripid";

        return DataFrameUtil.getDataFrame(spark,hql.replaceAll("datadate",datadate),"tmpforjoin1");


    }
    private static Dataset tmpForJoin2(SparkSession spark,String datadate) {
        String hql = "select zspid from s_ju_lesscreditimp where zspid is not null and zspid <> '' and zspid <> 'null' and zspid <> 'NULL' and fsx_enddate is null ";

        return DataFrameUtil.getDataFrame(spark,hql.replaceAll("datadate",datadate),"tmpforjoin2");


    }
    private static Dataset tmpForJoin1a(SparkSession spark,String datadate){
        String hql = "select pripid ,zspid from tmpforjoin1 where zspid is not null and zspid <> '' and zspid <> 'null' and zspid <> 'NULL'";

        return DataFrameUtil.getDataFrame(spark,hql.replaceAll("datadate",datadate),"tmpforjoin1a");

    }

    private static Dataset tmpForGroup (SparkSession spark,String datadate){
        String hql = "select b.pripid from tmpforjoin2 a join tmpforjoin1a b on a.zspid = b.zspid";
        return  DataFrameUtil.getDataFrame(spark,hql.replaceAll("datadate",datadate),"tmpforgroup");
    }


    private static Dataset naturalPersonLessCreditCase(SparkSession spark,String datadate){
        String hql = "select pripid ,count(1)  as naturalpersonlesscreditcase from tmpforgroup group by pripid";

        return DataFrameUtil.getDataFrame(spark,hql.replaceAll("datadate",datadate),"naturalPersonLessCreditCase");

    }


    public static Dataset getNatrualPersonLessCreditCase(SparkSession spark,String datadate){

     tmpForJoin1(spark, datadate);
     tmpForJoin1a(spark, datadate);
     tmpForJoin2(spark, datadate);
     tmpForGroup(spark, datadate);
     return  naturalPersonLessCreditCase(spark, datadate);

    }


}
