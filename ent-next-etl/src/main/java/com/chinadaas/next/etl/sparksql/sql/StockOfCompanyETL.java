package com.chinadaas.next.etl.sparksql.sql;

import com.chinadaas.common.util.DataFrameUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * @author haoxing
 * <p>
 * 企业持股。实缴与认缴比。
 */
public class StockOfCompanyETL {
    public static Dataset getstock(SparkSession spark, String datadate) {

        String hql = " SELECT\n" +
                "    a.pripid, \n" +
                "    COUNT(1) as eb0043,\n" +
                "    SUM(CASE WHEN b.INVTYPE  IN (\n" +
                "    '20',\n" +
                "    '21',\n" +
                "    '22',\n" +
                "    '30',\n" +
                "    '35',\n" +
                "    '36'\n" +
                "    ) THEN 1 ELSE 0 END) as eb0044\n" +
                "    FROM enterprisebaseinfocollect a\n" +
                "    INNER JOIN e_inv_investment b ON  a.PRIPID=b.PRIPID group by a.pripid";

        return DataFrameUtil.getDataFrame(spark, hql.replaceAll("datadate", datadate), "tmpstockofcompany");

    }


    //实缴与认缴比例
    public static Dataset getRateOfSubA(SparkSession spark, String datadate) {

        String hql = "select a.pripid, round(sum(b.acconam)/sum(b.subconam),4) as eb0021 \n" +
                "from enterprisebaseinfocollect a \n" +
                "join e_inv_investment b \n" +
                "on a.pripid = b.pripid \n" +
                "group by a.pripid";

        return DataFrameUtil.getDataFrame(spark, hql.replaceAll("datadate", datadate), "tmprateofsubacc");
    }

    public static Dataset getEnpStatus(SparkSession spark, String datadate) {

        String hql = "select a.pripid,b.eb0043,b.eb0044,c.eb0021 from enterprisebaseinfocollect a\n " +
                "left join tmpstockofcompany b on a.pripid = b.pripid\n" +
                "left join tmprateofsubacc c on a.pripid = c.pripid";

        return DataFrameUtil.getDataFrame(spark, hql.replaceAll("datadate", datadate), "enpstatus");
    }


    public static Dataset getStockOfCompany(SparkSession spark, String datadate) {

        getstock(spark, datadate);
        getRateOfSubA(spark,datadate);
        return getEnpStatus(spark, datadate);


    }
}
