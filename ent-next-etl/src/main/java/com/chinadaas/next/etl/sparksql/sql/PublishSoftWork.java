package com.chinadaas.next.etl.sparksql.sql;

import com.chinadaas.common.util.DataFrameUtil;
import com.chinadaas.common.util.TimeUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * @author haoxing
 * 优化减少merge时内存中注册的临时表
 */
public class PublishSoftWork {


    public static Dataset tmp01(SparkSession spark, String datadate) {
        String hql = "select b.pripid,count(*) as eb0053 from enterprisebaseinfocollect a join s_en_copyrightorg b on a.entname = \n" +//enterprisebaseinfocollect_full_datadate
                "b.frj_zzqr join s_en_copyrightinfo c on b.frj_djh = c.frj_djh\n" +
                "group by b.pripid";

        return DataFrameUtil.getDataFrame(spark, hql.replaceAll("datadate", datadate), "tmp01");

    }

    public static Dataset tmp02(SparkSession spark, String datadate) {
        String hql = "select b.pripid,count(*) as eb0054 from enterprisebaseinfocollect a join s_en_copyrightorg b on a.entname = \n" +//enterprisebaseinfocollect_full_datadate
                "b.frj_zzqr join s_en_copyrightinfo c on b.frj_djh = c.frj_djh where c.frj_status = 1\n" +
                "group by b.pripid";
        return DataFrameUtil.getDataFrame(spark, hql.replaceAll("datadate", datadate), "tmp02");
    }


    public static Dataset tmp03(SparkSession spark, String datadate) {
        String hql = "select b.pripid,count(*) as eb0052 from enterprisebaseinfocollect  a join s_en_copyrightorg b on a.entname = \n" +//enterprisebaseinfocollect_full_datadate
                "b.frj_zzqr join s_en_copyrightinfo c on b.frj_djh = c.frj_djh where c.frj_status = 1 and\n " +
                "substr(c.frj_djdate,1,10) > '"+TimeUtil.getYearAgo(3)+"'\n" +
                "and c.frj_djdate <> 'null' and c.frj_djdate is not null\n"+
                "group by b.pripid";

        return DataFrameUtil.getDataFrame(spark, hql.replaceAll("datadate", datadate), "tmp03");

    }


    public static Dataset tmp04(SparkSession spark, String datadate) {

        String hql = "select b.pripid,count(*) as eb0051 from enterprisebaseinfocollect a join s_en_copyrightorg b on a.entname = \n" +//enterprisebaseinfocollect_full_datadate
                "b.frj_zzqr join s_en_copyrightinfo c on b.frj_djh = c.frj_djh where c.frj_status = 1 and \n" +
                "substr(c.frj_djdate,1,10) > '"+TimeUtil.getYearAgo(1)+"'\n" +
                "and c.frj_djdate <> 'null' and c.frj_djdate is not null\n"+
                "group by b.pripid";

        return DataFrameUtil.getDataFrame(spark, hql.replaceAll("datadate", datadate), "tmp04");


    }
//2018-1-4 增加
    public static Dataset tmp05(SparkSession spark) {

        String hql = "select a.pripid,\n" +
                "case when b.eb0054 = 0 then '0' else round(c.eb0052/b.eb0054,2) end as eb0113,\n" +//软著近三年增加量比
                "a.eb0053 as eb0053,b.eb0054 as eb0054,c.eb0052 as eb0052,d.eb0051 as eb0051\n" +
                "from tmp01 a\n" +
                "left join tmp02 b on a.pripid = b.pripid\n" +
                "left join tmp03 c on a.pripid = c.pripid\n" +
                "left join tmp04 d on a.pripid = d.pripid";

        return DataFrameUtil.getDataFrame(spark, hql, "tmp05");

    }

    public static Dataset getPublishSoftWork(SparkSession spark, String datadate) {


        tmp01(spark,datadate);
        tmp02(spark, datadate);
        tmp03(spark, datadate);
        tmp04(spark, datadate);


        return tmp05(spark);


    }

}








