package com.chinadaas.next.etl.sparksql.sql;

import com.chinadaas.common.util.DataFrameUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class topLessCreditCase {

    public static Dataset getTopLessCreditCases(SparkSession spark,String datadate){
        tmpJoin2Case(spark, datadate);
        tmpJoin1Case(spark, datadate);
        tmpJoin1aCase(spark,datadate);
        tmpForGroup(spark,datadate);

        return lessCreditCases(spark,datadate);





    }

    private static Dataset lessCreditCases(SparkSession spark,String datadate){

       String hql = "select pripid,count(1) as toplessCreditCase  from tmpforgroup \n"+
                    "group by pripid";

        return DataFrameUtil.getDataFrame(spark,hql.replaceAll("datadate",datadate),"toplesscreidtcaseskpi");
    }

private static Dataset tmpJoin1Case(SparkSession spark ,String datadate){
    String hql ="select b.zspid ,b.pripid from \n" +
            "(select zspid ,pripid from e_pri_person_full_20170927 where pripid is not  null and pripid <> '' and lerepsign != 1 ) b \n" +
            "join \n" +
            "(select pripid from enterprisebaseinfocollect_full_20170927 where pripid is not null and pripid <> '')c \n" +
            "on b.pripid = c.pripid ";
    return DataFrameUtil.getDataFrame(spark,hql.replaceAll("datadate",datadate),"tmpJoincase1");

}
private static Dataset tmpJoin2Case(SparkSession spark,String datadate){
    String hql = "select zspid\n" +
            "          from s_ju_lesscreditimp\n" +
            "         where fsx_enddate is null and zspid is not null\n" +
            "           and zspid <> '' and zspid <> 'null' and zspid <> 'NULL'";


    return DataFrameUtil.getDataFrame(spark,hql.replaceAll("datadate",datadate),"tmpJoincase2");

}
    private static Dataset tmpJoin1aCase(SparkSession spark ,String datadate){
        String hql =" select pripid,zspid from tmpJoincase1 where zspid is not null and zspid <> '' and zspid <> 'null' and zspid <> 'NULL'";
                return  DataFrameUtil.getDataFrame(spark,hql.replaceAll("datadate",datadate),"tmpJoincase1a");
    }

    private static Dataset tmpForGroup(SparkSession spark,String datadate){

        String hql = "select b.pripid from tmpJoincase2  a join tmpJoincase1a  b on a.zspid = b.zspid";
        return DataFrameUtil.getDataFrame(spark,hql.replaceAll("datadate",datadate),"tmpforgroup");
    }
}

