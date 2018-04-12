package com.chinadaas.next.etl.sparksql.sql;

import com.chinadaas.common.util.DataFrameUtil;
import com.chinadaas.common.util.TimeUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * @author haoxing 2018-01-05
 * @Auther: zhouzhen@chinadaas.com
 * @description:
 * @Date: 15:07 2017/9/11
 * 严重违法标签
 */
public class BreakLawKpiETL {

    // 数据量少不做限制
    public static Dataset getBreakLawKpi(SparkSession spark, String datadate) {
        /**  String hql = "select a.pripid, a.intotal as eb0096, b.isex as eb0095,c.outtotal as eb0098, d.inyeartotal as eb0097, e.outyeartotal as eb0099 from " +
         "(SELECT pripid, COUNT(1) as intotal FROM s_en_break_law_hdfs_ext_datadate where brl_idat is not null group by pripid) a" +
         " left join "+
         //如果说数据中没有重复的情况下，处理方法就是select pripid,sum(case when brl_idat <>'null' and brl_odat = 'null' then 1 end) as isex from s_en_break_law_hdfs_ext_20170831;
         "(select pripid,1 as isex from s_en_break_law_hdfs_ext_datadate where brl_idat <> 'null' and brl_odat = 'null)b  on a.pripid = b.pripid" +
         " left join " +
         "(SELECT pripid, count(1) as outtotal FROM s_en_break_law_hdfs_ext_datadate where brl_odat is not null group by pripid) c  on a.pripid = c.pripid" +
         " left join " +
         "(SELECT pripid, COUNT(1) as inyeartotal FROM s_en_break_law_hdfs_ext_datadate where regexp_replace(substr(BRL_IDAT,1,10),'-','') >= " + TimeUtil.getYearAgo(1) + "  group by pripid)d on a.pripid = d.pripid" +
         " left join " +
         "(SELECT pripid, COUNT(1) as outyeartotal FROM s_en_break_law_hdfs_ext_datadate where regexp_replace(substr(BRL_DAT,1,10),'-','') >= " + TimeUtil.getYearAgo(1) + "  group by pripid)e on a.pripid = e.pripid ";
         */


        /** String hql = "select a.pripid, a.intotal as eb0096, b.isex as eb0095,c.outtotal as eb0098, d.inyeartotal as eb0097, e.outyeartotal as eb0099 from\n" +
         "(SELECT pripid, COUNT(1) as intotal FROM s_en_break_law where brl_idat is not null group by pripid) a\n" +
         "left join\n" +
         "(select pripid,1 as isex from s_en_break_law where brl_idat <> 'null' and brl_odat = 'null')b  on a.pripid = b.pripid\n" +
         "left join \n" +
         "(SELECT pripid, count(1) as outtotal FROM s_en_break_law where brl_odat is not null group by pripid) c  on a.pripid = c.pripid\n" +
         "left join\n" +
         "(SELECT pripid, COUNT(1) as inyeartotal FROM s_en_break_law where regexp_replace(substr(BRL_IDAT,1,10),'-','') >= regexp_replace('" + TimeUtil.getYearAgo(1) + "','-','') group by pripid)d on a.pripid = d.pripid\n" +
         "left join\n" +
         "(SELECT pripid, COUNT(1) as outyeartotal FROM s_en_break_law where regexp_replace(substr(BRL_ODAT,1,10),'-','') >= regexp_replace('" + TimeUtil.getYearAgo(1) + "','-','') group by pripid)e on a.pripid = e.pripid";
         return DataFrameUtil.getDataFrame(spark, hql.replaceAll("datadate", datadate), "breakLawKpiTmp");*/

        /**   //严重违法 修改与2018-01-05
         String hql = "select pripid,intoal as eb0096,\n" +
         "inyear as eb0097,\n" +
         "outtoal as eb0098,\n" +
         "outyear as eb0099,\n" +
         "case when (intoal - outtoal) > 0 then '是' else '否' end as eb0095 \n" +
         "from \n" +
         "(select pripid,\n" +
         "sum(case when (((brl_idat <> '' and brl_idat <> 'null' and brl_idat is not null) or (brl_iorg <> '' and brl_iorg <> 'null' and brl_iorg is not null) or (brl_ino <> '' and brl_ino <> 'null' and brl_ino is null)or (brl_inr <> '' and brl_inr <> 'null' and brl_inr is not null)) then 1 else 0 end ) as intoal,\n" +
         "sum(case when brl_idat <> '' and brl_idat <> 'null' and brl_idat is not null and regexp_replace(substr(BRL_IDAT,1,10),'-','') >= regexp_replace('" + TimeUtil.getYearAgo(1) + "','-','') then 1 else 0 end ) as inyear,\n" +
         "sum(case when( brl_odat <> '' or brl_oorg <> '' or brl_ono <> '' or brl_onr <> '') then 1 else 0 end) as outtoal,\n" +
         "sum(case when brl_odat <> '' and brl_odat <> 'null' and brl_odat is not null and regexp_replace(substr(BRL_ODAT,1,10),'-','') >= regexp_replace('" + TimeUtil.getYearAgo(1) + "','-','')  then 1 else 0 end) as outyear\n" +
         "from s_en_break_law group by pripid) ";
         */

        String hql = "select pripid,intoal as eb0096,\n" +
                "inyear as eb0097,\n" +
                "outtoal as eb0098,\n" +
                "outyear as eb0099,\n" +
                "case when (intoal - outtoal) > 0 then 'y' else 'n' end as eb0095\n" +
                "from\n" +
                "(select pripid,\n" +
                "sum(case when (brl_idat <> '' and brl_idat <> 'null' and brl_idat is not null) \n" +
                "or (brl_iorg <> '' and brl_iorg <> 'null' and brl_iorg is not null) \n" +
                "or (brl_ino <> '' and brl_ino <> 'null' and brl_ino is null) \n" +
                "or (brl_inr <> '' and brl_inr <> 'null' and brl_inr is not null) \n" +
                "then 1 else 0 end ) as intoal,\n" +
                "sum(case when brl_idat <> '' and brl_idat <> 'null' and brl_idat is not null and regexp_replace(substr(BRL_IDAT,1,10),'-','') >= regexp_replace('" + TimeUtil.getYearAgo(1) + "','-','') then 1 else 0 end ) as inyear,\n" +
                "sum(case when(brl_odat <> '' and brl_odat <> 'null' and brl_odat is not null) \n" +
                "or (brl_oorg <> '' and brl_oorg <> 'null' and brl_oorg is not null) \n" +
                "or (brl_ono <> '' and brl_ono <> 'null' and brl_ono is not null) \n" +
                "or (brl_onr <> '' and brl_onr <> 'null' and brl_onr is not null) \n" +
                "then 1 else 0 end) as outtoal,\n" +
                "sum(case when brl_odat <> '' and brl_odat <> 'null' and brl_odat is not null and regexp_replace(substr(BRL_ODAT,1,10),'-','') >= regexp_replace('" + TimeUtil.getYearAgo(1) + "','-','') then 1 else 0 end) as outyear\n" +
                "from s_en_break_law group by pripid)";
        return DataFrameUtil.getDataFrame(spark, hql.replaceAll("datadate", datadate), "breakLawKpiTmp");
    }
}
