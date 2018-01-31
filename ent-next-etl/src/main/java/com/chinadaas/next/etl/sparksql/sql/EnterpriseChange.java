package com.chinadaas.next.etl.sparksql.sql;

import com.chinadaas.common.util.DataFrameUtil;
import com.chinadaas.common.util.TimeUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.net.DatagramSocket;

/**
 * @author haoxing
 * 企业变更信息指标
 * 2018-1-4 增加eb0111 股权变化频度 一年内的股变化次数
 */
public class EnterpriseChange {




    private static Dataset ChangeTable1(SparkSession spark, String datadate) {

        String hql = "select * from e_alter_recoder where altbe <> altaf";

        return DataFrameUtil.getDataFrame(spark, hql.replaceAll("datadate", datadate), "tmpchange1");
    }

    private static Dataset Changetable2(SparkSession spark, String datadate) {

        String hql = "select * from e_alter_recoder where altbe = altaf";

        return DataFrameUtil.getDataFrame(spark, hql.replaceAll("datadate", datadate), "tmpchange2");
    }

    private static Dataset ChangeTable3(SparkSession spark, String datadate) {

        String hql = "select * from tmpchange2 where instr('10',altitem) <> 0 or instr('18',altitem)<>0 or instr('76',altitem)<>0";

        return DataFrameUtil.getDataFrame(spark, hql, "tmpchange3");
    }

    private static Dataset ChangeUnion(SparkSession spark) {

        String hql = "select * from tmpchange1 union all select * from tmpchange3";

        return DataFrameUtil.getDataFrame(spark, hql, "tmpchangeunion");
    }

    private static Dataset CaculateKpi(SparkSession spark, String datadate) {

        String hql = "select a.pripid,\n" +
                "cast(sum((length(b.altitem) - length(regexp_replace(b.altitem,',',''))+1)) as Int) as eb0070,\n" +
                "cast(sum((length(b.altitem) - length(regexp_replace(b.altitem,'03','')))/2) as Int) as eb0071,\n" +
                "(cast(sum((length(b.altitem) - length(regexp_replace(b.altitem,'06','')))/2) as Int) + cast(sum((length(b.altitem) - length(regexp_replace(b.altitem,'07','')))/2) as Int)+\n" +
                "cast(sum((length(b.altitem) - length(regexp_replace(b.altitem,'12','')))/2) as Int)+cast(sum((length(b.altitem) - length(regexp_replace(b.altitem,'25','')))/2) as Int)) as eb0072,\n" +
                "cast(sum((length(b.altitem) - length(regexp_replace(b.altitem,'10','')))/2) as Int)  as eb0073,\n" +
                "cast(sum((length(b.altitem) - length(regexp_replace(b.altitem,'13','')))/2) as Int) as eb0074,\n" +
                "cast(sum((length(b.altitem) - length(regexp_replace(b.altitem,'21','')))/2) as Int)  as eb0075,\n" +
                "cast(sum((length(b.altitem) - length(regexp_replace(b.altitem,'04','')))/2) as Int)  as eb0076,\n" +
                "(cast(sum((length(b.altitem) - length(regexp_replace(b.altitem,'73','')))/2) as Int) + cast(sum((length(b.altitem) - length(regexp_replace(b.altitem,'74','')))/2) as Int)) as eb0077,\n" +
                "(cast(sum((length(b.altitem) - length(regexp_replace(b.altitem,'26','')))/2) as Int) + cast(sum((length(b.altitem) - length(regexp_replace(b.altitem,'05','')))/2) as Int)) as eb0078\n" +
                "from enterprisebaseinfocollect a join tmpchangeunion b \n" +
                "on a.pripid = b.pripid group by a.pripid ";

        return DataFrameUtil.getDataFrame(spark, hql.replaceAll("datadate", datadate), "tmpenterpriseChange");

    }

    private static Dataset StockChangeRate(SparkSession spark,String datadate){

        String hql = "select a.pripid, cast(sum((length(b.altitem) - length(regexp_replace(b.altitem,'10','')))/2) as Int) as eb0111\n"+
                     "from enterprisebaseinfocollect a join tmpchangeunion b \n" +
                     "on a.pripid = b.pripid where b.altdate > '" + TimeUtil.getYearAgo(1) + "' group by a.pripid ";

        return DataFrameUtil.getDataFrame(spark,hql,"tmpstcokchangerate");

    }

    private static Dataset mergeChange(SparkSession spark,String datadate){

        String hql = "select a.*,b.eb0111 from tmpenterpriseChange a left join tmpstcokchangerate b on a.pripid = b.pripid";

        return DataFrameUtil.getDataFrame(spark,hql,"tmpentchange");


    }    public static Dataset getEnterpriseChange(SparkSession spark, String datadate) {

        ChangeTable1(spark, datadate);
        Changetable2(spark, datadate);
        ChangeTable3(spark, datadate);
        ChangeUnion(spark);
        CaculateKpi(spark, datadate);
        StockChangeRate(spark, datadate);

        return mergeChange(spark, datadate);
    }

}
