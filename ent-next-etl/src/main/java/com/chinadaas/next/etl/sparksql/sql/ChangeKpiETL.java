package com.chinadaas.next.etl.sparksql.sql;

import com.chinadaas.common.util.DataFrameUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * Created by gongxs01 on 2017/9/21.
 */
public class ChangeKpiETL {


    public static Dataset getChangeKpiDF(SparkSession spark,String date){
        getChangeKpiDF00(spark,date);
        getChangeKpiDF01(spark);
        getChangeKpiDF02(spark);
        getChangeKpiDF03(spark);
        getChangeKpiDF04(spark);
        getChangeKpiDF05(spark);
        getChangeKpiDF06(spark);
        getChangeKpiDF07(spark);
        getChangeKpiDF08(spark);
        getChangeKpiDF09(spark);
        return getChangeKpiDF10(spark);
    }


    //变更次数合计
    public static Dataset getChangeKpiDF00(SparkSession spark,String date){
        String hql = "SELECT pripid,altitem \n" +
                "  FROM e_alter_recoder_hdfs_ext_%s\n";
        return DataFrameUtil.getDataFrame(spark,String.format(hql,date),"e_alter_recoder_hdfs_ext",DataFrameUtil.CACHETABLE_PARQUET);
    }


    //变更次数合计
    private static Dataset getChangeKpiDF01(SparkSession spark){
        String hql = "SELECT pripid, count(*) as eb0070\n" +
                "  FROM e_alter_recoder_hdfs_ext\n" +
                " GROUP BY pripid";

        return DataFrameUtil.getDataFrame(spark,hql,"ChangeIndexTmp01");
    }

    //法人变更
    private static Dataset getChangeKpiDF02(SparkSession spark){
        String hql = "select pripid, count(*) as eb0071\n" +
                "  from e_alter_recoder_hdfs_ext\n" +
                " where altitem = '02'\n" +
                " group by pripid";

        return DataFrameUtil.getDataFrame(spark,hql,"ChangeIndexTmp02");
    }

    //经营内容变更次数
    private static Dataset getChangeKpiDF03(SparkSession spark){
        String hql = "select pripid, count(*) as EB0072\n" +
                "  from e_alter_recoder_hdfs_ext\n" +
                " where ALTITEM in ('06', '07', '12', '25')\n" +
                " group by pripid";

        return DataFrameUtil.getDataFrame(spark,hql,"ChangeIndexTmp03");
    }
    //投资人(股权)变更累计次数
    private static Dataset getChangeKpiDF04(SparkSession spark){
        String hql = "select pripid, count(*) as eb0073\n" +
                "  from e_alter_recoder_hdfs_ext\n" +
                " where altitem = '10'\n" +
                " group by pripid";

        return DataFrameUtil.getDataFrame(spark,hql,"ChangeIndexTmp04");
    }

    //--经营场所变更累计次数
    private static Dataset getChangeKpiDF05(SparkSession spark){
        String hql = "select pripid, count(*) as eb0074\n" +
                "  from e_alter_recoder_hdfs_ext\n" +
                " where altitem = '13'\n" +
                " group by pripid";

        return DataFrameUtil.getDataFrame(spark,hql,"ChangeIndexTmp05");
    }

    //负责人变更累计次数
    private static Dataset getChangeKpiDF06(SparkSession spark){
        String hql = "select pripid, count(*) as eb0075\n" +
                "  from e_alter_recoder_hdfs_ext\n" +
                " where altitem = '21'\n" +
                " group by pripid";

        return DataFrameUtil.getDataFrame(spark,hql,"ChangeIndexTmp06");
    }

    //企业类型变更累计次数
    private static Dataset getChangeKpiDF07(SparkSession spark){
        String hql = "select pripid, count(*) as eb0076\n" +
                "  from e_alter_recoder_hdfs_ext\n" +
                " where altitem = '04'\n" +
                " group by pripid";
        return DataFrameUtil.getDataFrame(spark,hql,"ChangeIndexTmp07");
    }
    //章程变更累计次数
    private static Dataset getChangeKpiDF08(SparkSession spark){
        String hql = "select pripid, count(*) as eb0077\n" +
                "  from e_alter_recoder_hdfs_ext\n" +
                " where altitem in ('73', '74')\n" +
                " group by pripid";

        return DataFrameUtil.getDataFrame(spark,hql,"ChangeIndexTmp08");
    }
    //注册资本变更累计次数
    private static Dataset getChangeKpiDF09(SparkSession spark){
        String hql = "select pripid, count(*) as eb0078\n" +
                "  from e_alter_recoder_hdfs_ext\n" +
                " where altitem in ('26', '05')\n" +
                " group by pripid";
        return DataFrameUtil.getDataFrame(spark,hql,"ChangeIndexTmp09");
    }


    private static Dataset getChangeKpiDF10(SparkSession spark){
        String hql = "  select a.pripid,\n" +
                "        case when  b.eb0070 is null  then 0 else  b.eb0070 end eb0070,\n" +
                "        case when  c.eb0071 is null  then 0 else  c.eb0071 end eb0071,\n" +
                "        case when  d.eb0072 is null  then 0 else  d.eb0072 end eb0072 ,\n" +
                "        case when  e.eb0073 is null  then 0 else  e.eb0073 end eb0073 ,\n" +
                "        case when  f.eb0074 is null  then 0 else  f.eb0074 end eb0074 ,\n" +
                "        case when  g.eb0075 is null  then 0 else  g.eb0075 end eb0075 ,\n" +
                "        case when  h.eb0076 is null  then 0 else  h.eb0076 end eb0076 ,\n" +
                "        case when  i.eb0077 is null  then 0 else  i.eb0077 end eb0077 ,\n" +
                "        case when  j.eb0078 is null  then 0 else  j.eb0078 end eb0078 \n" +
                "   from entInfoTmp03 a\n" +
                "   left join ChangeIndexTmp01 b\n" +
                "     on a.pripid = b.pripid\n" +
                "   left join ChangeIndexTmp02 c\n" +
                "     on a.pripid = c.pripid\n" +
                "   left join ChangeIndexTmp03 d\n" +
                "     on a.pripid = d.pripid\n" +
                "   left join ChangeIndexTmp04 e\n" +
                "     on a.pripid = e.pripid\n" +
                "   left join ChangeIndexTmp05 f\n" +
                "     on a.pripid = f.pripid\n" +
                "   left join ChangeIndexTmp06 g\n" +
                "     on a.pripid = g.pripid\n" +
                "   left join ChangeIndexTmp07 h\n" +
                "     on a.pripid = h.pripid\n" +
                "   left join ChangeIndexTmp08 i\n" +
                "     on a.pripid = i.pripid\n" +
                "   left join ChangeIndexTmp09 j\n" +
                "     on a.pripid = j.pripid ";

        return DataFrameUtil.getDataFrame(spark,hql,"ChangeIndexTmp10");
    }


}
