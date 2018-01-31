package com.chinadaas.next.etl.sparksql.sql;

import com.chinadaas.common.util.DataFrameUtil;
import com.chinadaas.common.util.TimeUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * @author haoxing
 * 软著发布著作权指标。优化merge，减少临时表。
 */
public class PublishCopyright {

    public static Dataset getPublishCopyright(SparkSession spark, String datadate) {
        String hql = "select w.pripid,w.eb0057,x.eb0058,y.eb0056,z.eb0055 from" +
                "(select a.pripid,count(1) as eb0057 from enterprisebaseinfocollect a\n" +
                "join s_en_productcopyrightinfo b\n" +
                "on a.entname = b.fzd_zzqrname\n" +
                "group by a.pripid) w\n" +
                "left join \n" +
                "(select a.pripid,count(1) as eb0058 from enterprisebaseinfocollect a\n" +
                "join s_en_productcopyrightinfo b\n" +
                "on a.entname = b.fzd_zzqrname\n" +
                "where b.fzd_status = 1\n" +
                "group by a.pripid) x\n" +
                "on w.pripid = x.pripid \n" +
                "left join \n" +
                "(select a.pripid,count(1) as eb0056 from enterprisebaseinfocollect a\n" +
                "join s_en_productcopyrightinfo b\n" +
                "on a.entname = b.fzd_zzqrname\n" +
                "where b.fzd_status = 1\n" +
                "and b.fzd_djdate <> 'null' and b.fzd_djdate is not null\n" +
                "and substr(b.fzd_djdate,1,10) > '" + TimeUtil.getYearAgo(3) + "'\n" +//'"+TimeUtil.getYearAgo(3)+"'
                "group by a.pripid) y\n" +
                "on w.pripid  = y.pripid \n" +
                "left join \n" +
                "(select a.pripid,count(1) as eb0055 from\n" +
                "enterprisebaseinfocollect a\n" +
                "join s_en_productcopyrightinfo b\n" +
                "on a.entname = b.fzd_zzqrname\n" +
                "where b.fzd_status = 1\n" +
                "and b.fzd_djdate <> 'null' and b.fzd_djdate is not null\n" +
                "and substr(b.fzd_djdate,1,10) > '" + TimeUtil.getYearAgo(1) + "' \n" +//'"+TimeUtil.getYearAgo(1)+"'
                "group by a.pripid) z\n" +
                "on w.pripid = z.pripid";

        return DataFrameUtil.getDataFrame(spark, hql.replaceAll("datadate", datadate), "tmppublishcopyright");
    }


    private static Dataset tmp01(SparkSession spark, String datadate) {

        String hql = "select a.pripid,count(1) as eb0057 from enterprisebaseinfocollect_full_datadate a\n" +
                "join s_en_productcopyrightinfo_hdfs_ext_20170831 b\n" +
                "on a.entname = b.fzd_zzqrname\n" +
                "group by a.pripid";

        return DataFrameUtil.getDataFrame(spark, hql.replaceAll("datadate", datadate), "tmp01");
    }


    private static Dataset tmp02(SparkSession spark, String datadate) {

        String hql = "select a.pripid,count(1) as eb0058 from enterprisebaseinfocollect_full_datadate a\n" +
                "join s_en_productcopyrightinfo_hdfs_ext_20170831 b\n" +
                "on a.entname = b.fzd_zzqrname\n" +
                "where b.fzd_status = 1\n" +
                "group by a.pripid";

        return DataFrameUtil.getDataFrame(spark, hql.replaceAll("datadate", datadate), "tmp02");
    }

    private static Dataset tmp03(SparkSession spark, String datadate) {

        String hql = "select a.pripid,count(1) as eb0056 from enterprisebaseinfocollect_full_datadate a\n" +
                "join s_en_productcopyrightinfo_hdfs_ext_20170831 b\n" +
                "on a.entname = b.fzd_zzqrname\n" +
                "where b.fzd_status = 1\n" +
                "b.fzd_djdate <> 'null' and b.fzd_djdate is not null\n" +
                "and substr(b.fzd_djdate,1,10) > '" + TimeUtil.getYearAgo(3) + "'\n" +
                "group by a.pripid";

        return DataFrameUtil.getDataFrame(spark, hql.replaceAll("datadate", datadate), "tmp03");
    }

    private static Dataset tmp04(SparkSession spark, String datadate) {

        String hql = "select a.pripid,count(1) as eb0055 from\n" +
                "enterprisebaseinfocollect_full_datadate a\n" +
                "join s_en_productcopyrightinfo_hdfs_ext_20170831 b\n" +
                "on a.entname = b.fzd_zzqrname\n" +
                "where b.fzd_status = 1\n" +
                "b.fzd_djdate <> 'null' and b.fzd_djdate is not null\n" +
                "and substr(b.fzd_djdate,1,10) > '" + TimeUtil.getYearAgo(1) + "' \n" +
                "group by a.pripid";

        return DataFrameUtil.getDataFrame(spark, hql.replaceAll("datadate", datadate), "tmp04");
    }


    private static Dataset tmp05(SparkSession spark, String datadate) {

        String hql = "select a.pripid,a.eb0057,b.eb0058,c.eb0056,d.eb0055\n" +
                "from tmp01 a left join tmp02 on a.pripid = b.pripid\n" +
                "left join tmp03 c on a.pripid = c.pripid\n" +
                "left join tmp04 d on a.pripid = d.pripid";

        return DataFrameUtil.getDataFrame(spark, hql.replaceAll("datadate", datadate), "tmp05");

    }

}
