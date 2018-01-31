package com.chinadaas.next.etl.sparksql.sql;

import com.chinadaas.common.util.DataFrameUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class legalPersonLessCreditCase {

    public static Dataset getLegalPersonLessCreditCase(SparkSession spark, String datadate){

        tmpForJoin1(spark,datadate);
        tmpForJoin2(spark,datadate);
        tmpForJoin1a(spark,datadate);
        tmpForGroup(spark,datadate);


        return  legalPersonLessCreidtCase(spark, datadate);



    }



    private static Dataset tmpForJoin1(SparkSession spark ,String datadate) {
        String hql = "select b.zspid ,b.pripid from \n" +
                "(select zspid ,pripid from e_pri_person_full_20170927 where pripid is not  null and pripid <> '' and lerepsign = 1) b \n" +
                "join \n" +
                "(select pripid from enterprisebaseinfocollect_full_20170927 where pripid is not null and pripid <> '')c \n" +
                "on b.pripid = c.pripid ";

        return DataFrameUtil.getDataFrame(spark, hql.replaceAll("datadate", datadate), "tmpforjoin1");
    }


    private static Dataset tmpForJoin2(SparkSession spark,String datadate){
        String hql = "select zspid\n" +
                "          from s_ju_lesscreditimp\n" +
                "         where zspid is not null\n" +
                "           and zspid <> ''\n"+
                "          and zspid <> 'null' \n"+
                "          and zspid <> 'NULL'\n"+
                "          and  fsx_enddate is null";

        return DataFrameUtil.getDataFrame(spark,hql.replaceAll("datadate",datadate),"tmpforjoin2");


    }

    private static Dataset tmpForJoin1a (SparkSession spark, String datadate){

        String hql = "select pripid,zspid from tmpforjoin1 where zspid is not null and zspid <> '' and zspid <> 'null' and zspid <> 'NULL'";
        return DataFrameUtil.getDataFrame(spark,hql.replaceAll("datadate",datadate),"tmpforjoin1a");
    }
private static Dataset tmpForGroup (SparkSession spark,String datadate){
        String hql = "select b.pripid from tmpforjoin2 a join tmpforjoin1a b on a.zspid = b.zspid";
        return DataFrameUtil.getDataFrame(spark,hql.replaceAll("datadate",datadate),"tmpforgroup");

}



    private static Dataset legalPersonLessCreidtCase(SparkSession spark,String datadate) {


        String hql = "select pripid ,count(1) as legallesscase from tmpforgroup \n" +
                "group by pripid";
        return DataFrameUtil.getDataFrame(spark,hql.replaceAll("datadate",datadate),"legalpersonlesscredit");
    }


}
