package com.chinadaas.next.etl.sparksql.sql;

import com.chinadaas.common.util.DataFrameUtil;
import com.chinadaas.common.util.TimeUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * @Auther: zhouzhen@chinadaas.com
 * @description:
 * @Date: 14:57 2017/9/8
 */
public class AbnormityKpiETL {

    public static Dataset getAbnormityKpi(SparkSession spark, String datadate) {

        /** String hql = "select a.pripid ,case when a.intotal is null then 99 else a.intotal end as eb0091,case when b.isex is null then 99 else b.isex end as ee0023,c.outtotal as eb0093,d.inyeartotal as eb0092,e.outyeartotal as eb0094 from\n" +
         "(select pripid,count(1) as intotal from s_en_abnormity_hdfs_ext_datadate where pripid <>'' and indate <> '1900-01-01' group by pripid) a\n" +
         "left join\n" +
         "(select pripid, 1 as isex  from s_en_abnormity_hdfs_ext_datadate where pripid <>'' and indate<> '1900-01-01' and  outdate = '1900-01-01' group by pripid ) b on a.pripid = b.pripid\n" +
         "left join\n" +
         "(select pripid,count(1) as outtotal from s_en_abnormity_hdfs_ext_datadate where pripid <>'' and outdate <> '1900-01-01' group by pripid  )c on a.pripid = c.pripid\n" +
         "left join\n" +
         "(select pripid,count(1) as inyeartotal from s_en_abnormity_hdfs_ext_datadate  where  pripid <>'' and regexp_replace(substr(indate,1,10) >=" + TimeUtil.getYearAgo(1) + " group by pripid )d on a.pripid = d.pripid\n" +
         "left join\n" +
         "(select pripid,count(1) as outyeartotal from s_en_abnormity_hdfs_ext_datadate  where  pripid <>'' and regexp_replace(substr(outdate,1,10) >=" + TimeUtil.getYearAgo(1) + "  group by pripid) e on a.pripid = e.pripid\n" ;
         */
        /** String hql = "select a.pripid ,case when a.intotal is null then '0' else a.intotal end as eb0091,case when b.isex is null then '否' else b.isex end as ee0023,c.outtotal as eb0093,d.inyeartotal as eb0092,e.outyeartotal as eb0094 from\n" +
         " (select pripid,count(1) as intotal from s_en_abnormity where pripid <>'' and indate <> '1900-01-01' group by pripid) a\n" +
         " left join\n" +
         " (select pripid, '是' as isex  from s_en_abnormity where pripid <>'' and indate<> '1900-01-01' and  outdate = '1900-01-01' group by pripid) b \n" +
         " on a.pripid = b.pripid\n" +
         " left join\n" +
         " (select pripid,count(1) as outtotal from s_en_abnormity where pripid <>'' and outdate <> '1900-01-01' group by pripid) c \n" +
         " on a.pripid = c.pripid\n" +
         " left join\n" +
         " (select pripid,count(1) as inyeartotal from s_en_abnormity  where  pripid <>'' and substr(indate,1,10) >= regexp_replace('" + TimeUtil.getYearAgo(1) + "','-','') group by pripid) d \n" +
         " on a.pripid = d.pripid\n" +
         " left join\n" +
         " (select pripid,count(1) as outyeartotal from s_en_abnormity  where  pripid <>'' and substr(outdate,1,10) >=regexp_replace('" + TimeUtil.getYearAgo(1) + "','-','') group by pripid) e on a.pripid = e.pripid";
         return DataFrameUtil.getDataFrame(spark, hql.replaceAll("datadate", datadate), "abnormityKpiTmp");*/

        // 2018- 01 - 05 修改 异常经营的逻辑
        String hql = "select a.pripid,\n" +
                "a.intoal as eb0091,\n" +
                "b.outtoal as eb0093,\n" +
                "c.inyear as eb0092,\n" +
                "d.outyear as eb0094,\n" +
                "case when (b.outtoal - a.intoal) > 0 then '是' else '否' end as ee0023 from \n" +
                "(select pripid ,count(1)  as intoal from s_en_abnormity where pripid <> '' and (indate <> '' or yr_regorg <> '' or inreason <> '') group by pripid) a \n" +
                "left join\n" +
                "(select pripid,count(1) as outtoal  from s_en_abnormity where pripid <> '' and ((outdate <> '' and outdate <> '1900-01-01') or outreason <> '' or yc_regorg <> '')group by pripid) b\n" +
                "on a.pripid = b.pripid\n" +
                "left join \n" +
                "(select pripid,count(1) as inyear from s_en_abnormity where pripid <> '' and indate <> '' and substr(indate,1,10) >= regexp_replace('" + TimeUtil.getYearAgo(1) + "','-','')  group by pripid) c \n" +
                "on a.pripid = c.pripid\n" +
                "left join (select pripid,count(1) as outyear from s_en_abnormity where pripid<> '' and outdate <> '' and outdate <> '1900-01-01' and substr(outdate,1,10) >=regexp_replace('" + TimeUtil.getYearAgo(1) + "','-','') group by pripid) d \n" +
                "on a.pripid =d.pripid";

        return DataFrameUtil.getDataFrame(spark, hql.replaceAll("datadate", datadate), "abnormityKpiTmp");


    }

}
