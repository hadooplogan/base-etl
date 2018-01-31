package com.chinadaas.next.etl.sparksql.sql;

import com.chinadaas.common.util.DataFrameUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * @author haoxing
 */

public class LegalInvestment {



  /*  public static Dataset getLegalInvestment(SparkSession spark,String datadate){

      String hql = "SELECT\n" +
              "pripid,\n" +
              "COUNT(x) as eb0026,\n" +
              "SUM(CASE WHEN ENTSTATUS='1' THEN 1 ELSE 0 END) as eb0027,\n" +
              "SUM(CASE WHEN ENTSTATUS <>'1' THEN 1 ELSE 0 END) as eb0028\n" +
              "from\n" +
              "(select distinct a.pripid ,c.pripid as x,c.entstatus\n" +
              "from (select * from e_pri_person_full_20171207  where lerepsign ='1') a\n" +
              "join  e_inv_investment_full_20171207 b on  a.zspid = b.zspid and a.zspid <>'' and a.zspid <>'null' and a.zspid is not null and b.zspid <>'' and b.zspid <>'null' and b.zspid <> 'null'\n" +
              "join enterprisebaseinfocollect_full_20171207 c on b.pripid = c.pripid\n" +
              "where a.pripid <> c.pripid)\n" +
              "group by pripid";

     return DataFrameUtil.getDataFrame(spark,hql.replaceAll("datadate",datadate),"tmp01");

    }*/


    private static Dataset tmp01(SparkSession spark, String datadate) {


        String hql = "select distinct a.pripid,d.pripid y,d.entstatus\n" +
                "from enterprisebaseinfocollect a\n" +
                "join e_pri_person b on a.pripid = b.pripid and b.lerepsign = '1'\n" +
                "join e_inv_investment c on b.zspid = c.zspid \n" +
                "and b.zspid is not null and b.zspid <> '' and b.zspid <> 'null'\n" +
                "and c.zspid is not null and c.zspid <> '' and c.zspid <> 'null' and c.inv = b.name\n" +
                "join enterprisebaseinfocollect d on c.pripid = d.pripid\n" +
                "where d.entname <> a.entname";


        return DataFrameUtil.getDataFrame(spark, hql.replace("datadate", datadate), "tmp01");

    }


    private static Dataset tmp02(SparkSession spark, String datadate) {

        String hql = "select pripid,\n" +
                "count(y) as eb0026,\n" +
                "sum(case when entstatus = '1' then 1 else 0 end) as eb0027,\n" +
                "sum(case when entstatus <> '1' then 1 else 0 end) as eb0028\n" +
                "from tmp01\n" +
                "group by pripid";

        return DataFrameUtil.getDataFrame(spark, hql.replaceAll("datadate", datadate), "tmp02");
    }


    public static Dataset getLegalInvestment(SparkSession spark, String datadate) {

        tmp01(spark, datadate);
        return tmp02(spark, datadate);

    }

}