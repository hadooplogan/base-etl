package com.chinadaas.next.etl.sparksql.sql;

import com.chinadaas.common.util.DataFrameUtil;
import com.chinadaas.common.util.TimeUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * @author haoxing
 * 2018-1-4 增加企业近一年对外投资公司数标签eb0108,
 * 2018-1-4 增加企业对外投资频率（一年内对外投资企业数）eb0109,
 * 2018-1-4 增加股投资比 eb0110
 */
public class EnterpriseInvestment {


    private static Dataset tmp01(SparkSession spark, String datadate) {

        String hql = " SELECT\n" +
                " a.pripid, \n" +
                " case when count(d.pripid) > 0 then \n" +
                "round(SUM(case when regexp_replace(substr(c.condate,1,10),'-','')>regexp_replace('" + TimeUtil.getYearAgo(1) + "','-','') THEN 1 ELSE 0 END)/count(d.pripid),2) else '0' end as eb0109,\n" +//企业对外投资频率
                " SUM(case when regexp_replace(substr(c.condate,1,10),'-','')>regexp_replace('" + TimeUtil.getYearAgo(1) + "','-','') THEN 1 ELSE 0 END) as eb0108,\n" +//新增的近一年内对外投资公司数。
                " SUM(case when regexp_replace(substr(c.condate,1,10),'-','')>regexp_replace('" + TimeUtil.getYearAgo(3) + "','-','') THEN 1 ELSE 0 END) as eb0029,\n" +
                " SUM(case when regexp_replace(substr(c.condate,1,10),'-','')>regexp_replace('" + TimeUtil.getYearAgo(3) + "','-','') AND d.ENTSTATUS='1' THEN 1 ELSE 0 END) as eb0030,\n" +
                " SUM(case when regexp_replace(substr(c.condate,1,10),'-','')>regexp_replace('" + TimeUtil.getYearAgo(3) + "','-','') AND d.ENTSTATUS!='1' THEN 1 ELSE 0 END) as eb0031,\n" +
                " COUNT(d.pripid) as eb0032,\n" +
                " SUM(CASE WHEN d.ENTSTATUS='1' THEN 1 ELSE 0 END) as eb0033,\n" +
                " SUM(CASE WHEN d.ENTSTATUS!='1' THEN 1 ELSE 0 END) as eb0034,\n" +
                " SUM(case when regexp_replace(substr(c.condate,1,10),'-','')>regexp_replace('" + TimeUtil.getYearAgo(3) + "','-','') THEN c.SUBCONAM ELSE 0 END) as eb0035,\n" +
                " SUM(case when regexp_replace(substr(c.condate,1,10),'-','')>regexp_replace('" + TimeUtil.getYearAgo(3) + "','-','') AND d.ENTSTATUS='1' THEN c.SUBCONAM ELSE 0 END) as eb0036,\n" +
                " SUM(case when regexp_replace(substr(c.condate,1,10),'-','')>regexp_replace('" + TimeUtil.getYearAgo(3) + "','-','') AND d.ENTSTATUS!='1' THEN c.SUBCONAM ELSE 0 END) as eb0037,\n" +
                " SUM(c.SUBCONAM) as eb0038,\n" +
                " SUM(CASE WHEN d.ENTSTATUS='1' THEN  c.SUBCONAM  ELSE 0 END) as eb0039,\n" +
                " SUM(CASE WHEN d.ENTSTATUS!='1' THEN  c.SUBCONAM  ELSE 0 END) as eb0040\n" +
                "from  enterprisebaseinfocollect a    \n" +
                "INNER JOIN e_inv_investment c ON a.entname = c.inv \n" +
                "INNER JOIN enterprisebaseinfocollect d ON d.PRIPID=c.PRIPID\n" +
                " WHERE a.entname != d.entname group by a.pripid";
        return DataFrameUtil.getDataFrame(spark, hql.replaceAll("datadate", datadate), "tmpiv");
    }

    //企业注册资本百分位
    //  "case when SUM(CASE WHEN d.ENTSTATUS='1' THEN  c.SUBCONAM  ELSE 0 END) = 0 then '0'\n" +
    //  "when SUM(CASE WHEN d.ENTSTATUS='1' THEN  c.SUBCONAM  ELSE 0 END) <> 0 then \n" +
    // " round(a.regcap/SUM(CASE WHEN d.ENTSTATUS='1' THEN  c.SUBCONAM  ELSE 0 END),2) end as eb0110,\n"+//资本投资比
    private static Dataset tmp02(SparkSession spark) {
        String hql = "select a.pripid,\n" +
                "b.eb0108,b.eb0109,\n" +
                "case when b.eb0039 <> 0 and b.eb0039 is not null\n" +
                "then round(a.regcap/b.eb0039,2) else '0' end as eb0110,\n" +
                "b.eb0029,b.eb0030,b.eb0031,\n" +
                "b.eb0032,b.eb0033,b.eb0034,\n" +
                "b.eb0035,b.eb0036,b.eb0037,\n" +
                "b.eb0038,b.eb0039,b.eb0040\n" +
                "from enterprisebaseinfocollect a \n" +
                "left join tmpiv b on a.pripid= b.pripid";


        return DataFrameUtil.getDataFrame(spark, hql, "tmp02");


    }

    public static Dataset getEnterpriseInvestment(SparkSession spark, String datadate) {
        tmp01(spark, datadate);

        return tmp02(spark);

    }


}

