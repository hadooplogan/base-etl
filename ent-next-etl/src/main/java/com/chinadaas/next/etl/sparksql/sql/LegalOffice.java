package com.chinadaas.next.etl.sparksql.sql;

import com.chinadaas.common.util.DataFrameUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import javax.xml.crypto.Data;


/**
 * @author haoxing
 *
 * 计算法人对外任职（对外并不担任法人的其他职位）
 */
public class LegalOffice {


    private static Dataset tmp01(SparkSession spark, String datadate) {

        String hql = "select distinct a.pripid,c.pripid as y,c.entstatus from enterprisebaseinfocollect a \n" +
                "join e_pri_person b \n" +
                "on a.zspid = b.zspid \n" +
                "and a.zspid <> 'null' and a.zspid <> '' \n" +
                "and a.zspid is not null \n" +
                "and b.zspid <> 'null' \n" +
                "and b.zspid <> '' \n" +
                "and b.zspid is not null \n" +
                "and b.lerepsign <> 1 \n" +
                "join enterprisebaseinfocollect c \n" +
                "on b.pripid = c.pripid \n" +
                "where a.entname <> c.entname";

        return DataFrameUtil.getDataFrame(spark, hql.replaceAll("datadate", datadate), "tmp01");
    }


    private static Dataset tmp02(SparkSession spark, String datadate) {

        String hql = "select pripid,\n" +
                "count(y) as eb0041,\n" +
                "sum(case when entstatus = '1' then 1 else 0 end) as eb0042\n" +
                "from tmp01\n" +
                "group by pripid";

        return DataFrameUtil.getDataFrame(spark, hql.replaceAll("datadate", datadate), "tmp02");
    }


    public static Dataset getLegalOffice(SparkSession spark, String datadate) {

        tmp01(spark, datadate);
        return tmp02(spark, datadate);


    }


}
