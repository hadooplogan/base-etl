package com.chinadaas.next.etl.sparksql.sql;

import com.chinadaas.common.util.DataFrameUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * Created by gongxs01 on 2017/9/12.
 */
public class EntRelationBiKpiETL {


    /**
     * 拿下指标处理
     * @param spark
     * @return
     */
    public static Dataset getEntRelationBiKpi(SparkSession spark){
        getEntRelationBiKpiDF01(spark);
        getEntRelationBiKpiDF02(spark);
        getEntRelationBiKpiDF03(spark);
        getEntRelationBiKpiDF04(spark);
        getEntRelationBiKpiDF05(spark);
        getEntRelationBiKpiDF06(spark);
        getEntRelationBiKpiDF07(spark);
        getEntRelationBiKpiDF08(spark);
        getEntRelationBiKpiDF09(spark);
        getEntRelationBiKpiDF10(spark);
        getEntRelationBiKpiDF11(spark);
        getEntRelationBiKpiDF12(spark);
        getEntRelationBiKpiDF13(spark);
        getEntRelationBiKpiDF14(spark);
        getEntRelationBiKpiDF15(spark);
        getEntRelationBiKpiDF16(spark);
//        getEntRelationBiKpiDF17(spark);
        getEntRelationBiKpiDF18(spark);
        getEntRelationBiKpiDF19(spark);

        return  getEntRelationBiKpiDF20(spark);
    }

    //法定代表人历史累计对外投资企业数
    public static Dataset getEntRelationBiKpiDF01(SparkSession spark){

        String hql = "select a.pripid, a.zspid, b.ct\n" +
                "  from legal a\n" +
                "  join (select  startKey as zspid, count(*) as ct from personinv group by startKey) b\n" +
                "    on a.zspid = b.zspid";

        return DataFrameUtil.getDataFrame(spark,hql,"entRelationKpiTmp01");
    }

    //法定代表人对外投资有效企业数
    private static Dataset getEntRelationBiKpiDF02(SparkSession spark){

        String hql = "select c.pripid, c.zspid, d.ct\n" +
                "  from legal c\n" +
                "  join (select b.zspid, count(*) as ct\n" +
                "          from (select distinct a.startKey as zspid, a.endKey as pripid\n" +
                "                  from personinv a\n" +
                "                  join entInfoTmp03 b\n" +
                "                    on a.endKey = b.pripid\n" +
                "                 where b.entstatus = '1') b\n" +
                "         group by b.zspid) d\n" +
                "    on c.zspid = d.zspid";

        return DataFrameUtil.getDataFrame(spark,hql,"entRelationKpiTmp02");
    }

    //法定代表人对外投资关停企业数
    private static Dataset getEntRelationBiKpiDF03(SparkSession spark){

        String hql = "select c.pripid, c.zspid, d.ct\n" +
                "  from legal c\n" +
                "  join (select b.zspid, count(*) as ct\n" +
                "          from (select a.startKey as zspid, a.endKey as pripid\n" +
                "                  from personinv a\n" +
                "                  join entInfoTmp03 b\n" +
                "                    on a.endKey = b.pripid\n" +
                //修改关停企业的条件改为不在营便为关停
                "                 where b.entstatus != '1' ) b\n" +
                "         group by b.zspid) d\n" +
                "    on c.zspid = d.zspid";

        return DataFrameUtil.getDataFrame(spark,hql,"entRelationKpiTmp03");
    }

    //近三年对外投资公司数
    private static Dataset getEntRelationBiKpiDF04(SparkSession spark){

        String hql = "select startKey as pripid, count(*) as ct\n" +
                "  from entinv\n" +
                " where date_add(condate, 3 * 365) >=\n" +
                "       from_unixtime(unix_timestamp(), 'yyyy-MM-dd')\n" +
                " group by startKey";

        return DataFrameUtil.getDataFrame(spark,hql,"entRelationKpiTmp04");
    }

    //近三年对外投资公司有效企业数
    private static Dataset getEntRelationBiKpiDF05(SparkSession spark){

        String hql = "select b.pripid, count(*) AS ct\n" +
                "  from (select a.startKey as pripid, a.endKey, a.condate\n" +
                "          from entinv a\n" +
                "          join entInfoTmp03 b\n" +
                "            on a.endKey = b.pripid\n" +
                "         where b.entstatus = '1') b\n" +
                " where date_add(b.condate, 3 * 365) >=\n" +
                "       from_unixtime(unix_timestamp(), 'yyyy-MM-dd')\n" +
                " group by b.pripid";

        return DataFrameUtil.getDataFrame(spark,hql,"entRelationKpiTmp05");
    }

    //近三年对外投资公司关停企业数
    private static Dataset getEntRelationBiKpiDF06(SparkSession spark){

        String hql = "select b.pripid, count(*) AS ct\n" +
                "  from (select a.startKey as pripid, a.endKey, a.condate\n" +
                "          from entinv a\n" +
                "          join entInfoTmp03 b\n" +
                "            on a.endKey = b.pripid\n" +
                "         where b.entstatus in ('2','21','22','3')) b\n" +
                " where date_add(b.condate, 3 * 365) >=\n" +
                "       from_unixtime(unix_timestamp(), 'yyyy-MM-dd')\n" +
                " group by b.pripid";

        return DataFrameUtil.getDataFrame(spark,hql,"entRelationKpiTmp06");
    }

    //历史对外投资公司数
    private static Dataset getEntRelationBiKpiDF07(SparkSession spark){

        String hql = "select startKey as pripid, count(*) as ct from entinv group by startKey";

        return DataFrameUtil.getDataFrame(spark,hql,"entRelationKpiTmp07");
    }

    //历史对外投资公司有效企业数
    private static Dataset getEntRelationBiKpiDF08(SparkSession spark){

        String hql = "select pripid, count(*) as ct\n" +
                "  from (select distinct a.startKey as pripid, a.endKey\n" +
                "          from entinv a\n" +
                "          join entInfoTmp03 b\n" +
                "            on a.endKey = b.pripid\n" +
                "         where b.entstatus = '1') b\n" +
                " group by pripid";

        return DataFrameUtil.getDataFrame(spark,hql,"entRelationKpiTmp08");
    }

    //历史对外投资公司关停企业数
    private static Dataset getEntRelationBiKpiDF09(SparkSession spark){

        String hql = "select pripid, count(*) as ct\n" +
                "  from (select a.startKey as pripid, a.endKey\n" +
                "          from entinv a\n" +
                "          join entInfoTmp03 b\n" +
                "            on a.endKey = b.pripid\n" +
                "         where b.entstatus in ('2','21','22','3')) b\n" +
                " group by pripid";

        return DataFrameUtil.getDataFrame(spark,hql,"entRelationKpiTmp09");
    }

    //近三年对外投资金额
    private static Dataset getEntRelationBiKpiDF10(SparkSession spark){

        String hql = "select startKey as pripid, sum(subconam) as ct\n" +
                "  from entinv\n" +
                " where date_add(condate, 3 * 365) >=\n" +
                "       from_unixtime(unix_timestamp(), 'yyyy-MM-dd')\n" +
                " group by startKey";

        return DataFrameUtil.getDataFrame(spark,hql,"entRelationKpiTmp10");
    }

    //近三年对外投资公司有效投资金额
    private static Dataset getEntRelationBiKpiDF11(SparkSession spark){

        String hql = "select startKey as pripid, sum(subconam) as ct\n" +
                "  from (select a.startKey, a.subconam, a.endKey,a.condate\n" +
                "          from entinv a\n" +
                "          join entInfoTmp03 b\n" +
                "            on a.endKey = b.pripid\n" +
                "         where b.entstatus = '1') b\n" +
                " where date_add(condate, 3 * 365) >=\n" +
                "       from_unixtime(unix_timestamp(), 'yyyy-MM-dd')\n" +
                " group by startKey";

        return DataFrameUtil.getDataFrame(spark,hql,"entRelationKpiTmp11");
    }

    //近三年对外投资公司关停投资金额
    private static Dataset getEntRelationBiKpiDF12(SparkSession spark){

        String hql = "select startKey as pripid, sum(subconam) as ct\n" +
                "  from (select a.startKey, a.subconam, a.endKey,a.condate\n" +
                "          from entinv a\n" +
                "          join entInfoTmp03 b\n" +
                "            on a.endKey = b.pripid\n" +
                "         where b.entstatus in ('2','21','22','3')) b\n" +
                " where date_add(condate, 3 * 365) >=\n" +
                "       from_unixtime(unix_timestamp(), 'yyyy-MM-dd')\n" +
                " group by startKey";

        return DataFrameUtil.getDataFrame(spark,hql,"entRelationKpiTmp12");
    }

    //历史对外投资金额
    private static Dataset getEntRelationBiKpiDF13(SparkSession spark){

        String hql = "select startKey as pripid, sum(subconam) as ct from entinv group by startKey";

        return DataFrameUtil.getDataFrame(spark,hql,"entRelationKpiTmp13");
    }

    //历史对外投资公司有效投资金额
    private static Dataset getEntRelationBiKpiDF14(SparkSession spark){

        String hql = "select startKey as pripid, sum(subconam) as ct\n" +
                "  from (select startKey, subconam, endKey\n" +
                "          from entinv a\n" +
                "          join entInfoTmp03 b\n" +
                "            on a.endKey = b.pripid\n" +
                "         where b.entstatus = '1') b\n" +
                " group by startKey";

        return DataFrameUtil.getDataFrame(spark,hql,"entRelationKpiTmp14");
    }

    //历史对外投资公司关停投资金额
    private static Dataset getEntRelationBiKpiDF15(SparkSession spark){

        String hql = "select startKey as pripid , sum(subconam) as ct\n" +
                "  from (select startKey, subconam, endKey\n" +
                "          from entinv a\n" +
                "          join entInfoTmp03 b\n" +
                "            on a.endKey = b.pripid\n" +
                "         where b.entstatus in ('2','21','22','3')) b\n" +
                " group by startKey";

        return DataFrameUtil.getDataFrame(spark,hql,"entRelationKpiTmp15");
    }

    //法定代表人对外任职企业数
    private static Dataset getEntRelationBiKpiDF16(SparkSession spark){

        String hql = "select a.pripid, a.zspid, b.ct\n" +
                "  from legal a\n" +
                "  join (select startkey, (count(*)-1)  as ct from vistaff group by startkey) b\n" +
                "    on a.zspid = b.startkey";

        return DataFrameUtil.getDataFrame(spark,hql,"entRelationKpiTmp16");
    }

  /*  private static Dataset getEntRelationBiKpiDF17(SparkSession spark){

        String hql = "select endKey as pripid,count(*) as ct from entinv group by endKey ";

        return DataFrameUtil.getDataFrame(spark,hql,"entRelationKpiTmp17");
    }*/


    //企业自然人股东数
    private static Dataset getEntRelationBiKpiDF18(SparkSession spark){

        String hql = "select count(*) as ct,endKey as pripid  from personinv group by  endKey";

        return DataFrameUtil.getDataFrame(spark,hql,"entRelationKpiTmp18");
    }

    //企业法人股东数
    private static Dataset getEntRelationBiKpiDF19(SparkSession spark){

        String hql = "select count(*) as ct, endKey as pripid from entinv group  by endKey";

        return DataFrameUtil.getDataFrame(spark,hql,"entRelationKpiTmp19");
    }

    private static Dataset  getEntRelationBiKpiDF20(SparkSession spark){

        String hql = "select a.pripid,\n" +
                "       ent.ct   as eb0026,\n" +
                "       b.ct     as eb0027,\n" +
                "       c.ct     as eb0028,\n" +
                "       d.ct     as eb0029,\n" +
                "       e.ct     as eb0030,\n" +
                "       f.ct     as eb0031,\n" +
                "       g.ct     as eb0032,\n" +
                "       h.ct     as eb0033,\n" +
                "       i.ct     as eb0034,\n" +
                "       j.ct     as eb0035,\n" +
                "       k.ct     as eb0036,\n" +
                "       l.ct     as eb0037,\n" +
                "       m.ct     as eb0038,\n" +
                "       n.ct     as eb0039,\n" +
                "       o.ct     as eb0040,\n" +
                "       p.ct     as eb0042,\n" +
                "       r.ct+s.ct   as eb0043,\n" +
                "       r.ct     as eb0044,\n" +
                "       s.ct     as eb0045,\n" +
                "       case when s.ct >0 then 1 else 0 end eb0106 \n" +
                "  from entInfoTmp03 a\n" +
                "  left join entRelationKpiTmp01 ent\n" +
                "    on a.pripid = ent.pripid\n" +
                "  left join entRelationKpiTmp02 b\n" +
                "    on a.pripid = b.pripid\n" +
                "  left join entRelationKpiTmp03 c\n" +
                "    on a.pripid = c.pripid\n" +
                "  left join entRelationKpiTmp04 d\n" +
                "    on a.pripid = d.pripid\n" +
                "  left join entRelationKpiTmp05 e\n" +
                "    on a.pripid = e.pripid\n" +
                "  left join entRelationKpiTmp06 f\n" +
                "    on a.pripid = f.pripid\n" +
                "  left join entRelationKpiTmp07 g\n" +
                "    on a.pripid = g.pripid\n" +
                "  left join entRelationKpiTmp08 h\n" +
                "    on a.pripid = h.pripid\n" +
                "  left join entRelationKpiTmp09 i\n" +
                "    on a.pripid = i.pripid\n" +
                "  left join entRelationKpiTmp10 j\n" +
                "    on a.pripid = j.pripid\n" +
                "  left join entRelationKpiTmp11 k\n" +
                "    on a.pripid = k.pripid\n" +
                "  left join entRelationKpiTmp12 l\n" +
                "    on a.pripid = l.pripid\n" +
                "  left join entRelationKpiTmp13 m\n" +
                "    on a.pripid = m.pripid\n" +
                "  left join entRelationKpiTmp14 n\n" +
                "    on a.pripid = n.pripid\n" +
                "  left join entRelationKpiTmp15 o\n" +
                "    on a.pripid = o.pripid\n" +
                "  left join entRelationKpiTmp16 p\n" +
                "    on a.pripid = p.pripid\n" +
//                "  left join entRelationKpiTmp17 q\n" +
//                "    on a.pripid = q.pripid\n" +
                "  left join entRelationKpiTmp18 r\n" +
                "    on a.pripid = r.pripid\n" +
                "  left join entRelationKpiTmp19 s\n" +
                "    on a.pripid = s.pripid ";

        return DataFrameUtil.getDataFrame(spark,hql,"entRelationKpiTmp20");
    }
}
