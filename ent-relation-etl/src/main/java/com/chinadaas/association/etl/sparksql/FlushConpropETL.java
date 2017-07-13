package com.chinadaas.association.etl.sparksql;

import com.chinadaas.common.util.DataFrameUtil;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;

/**
 * Created by gongxs01 on 2017/6/13.
 */
public class FlushConpropETL {
    public String date;
    public void setDate(String date) {
        this.date = date;
    }
    public DataFrame getFlushBadData(HiveContext sqlContext) {
        getFlushBadData01(sqlContext);
        getFlushBadData011(sqlContext);
        getFlushBadData02(sqlContext);
        getFlushBadData031(sqlContext);
        getFlushBadData03(sqlContext);
        getFlushBadData042(sqlContext);
        getFlushBadData041(sqlContext);
        getFlushBadData04(sqlContext);
        getFlushBadData05(sqlContext);
        getFlushBadData06(sqlContext);
        getFlushBadData061(sqlContext);
        //getFlushBadData062(sqlContext);
        DataFrame df = getFlushBadData07(sqlContext);
//        df.limit(100000).show();
        return df;
    }


    private DataFrame getFlushBadData01(HiveContext sqlContext){
        String hql = "select *\n" +
                "  from (select a.pripid, b.regno, b.entname,b.regcap, a.conam / b.REGCAP as bili\n" +
                "          from (select pripid, sum(SUBCONAM) as conam\n" +
                "                  from e_inv_investment_hdfs_ext_%s\n" +
                "                 group by pripid) a\n" +
                "          join (select pripid, REGCAP, regno, entname\n" +
                "                 from enterprisebaseinfocollect_hdfs_ext_%s\n" +
                "                ) b\n" +
                "            on a.pripid = b.pripid\n" +
                "         where b.REGCAP <> '0'\n" +
                "           and b.REGCAP <> '') f\n" +
                " where f.bili > 1.05\n" +
                "    or f.bili < 0.95";
        return DataFrameUtil.getDataFrame(sqlContext,String.format(hql,date,date),"probleDataTmp01",DataFrameUtil.CACHETABLE_EAGER);
    }

    private DataFrame getFlushBadData011(HiveContext sqlContext){
        String hql = "select b.ancheid, b.regno, b.entname\n" +
                "  from (select pripid, max(ancheyear) ancheyear\n" +
                "          from C_GS_AN_BASEINFO\n" +
                "         group by pripid) a\n" +
                "  join C_GS_AN_BASEINFO b\n" +
                "    on a.pripid = b.pripid\n" +
                "   and a.ancheyear = b.ancheyear\n";

        return DataFrameUtil.getDataFrame(sqlContext,hql,"CGSANBASEINFOTMP011");
    }


    private DataFrame getFlushBadData02(HiveContext sqlContext){
        String hql = "select a.pripid, b.inv,case when b.lisubconam is null or b.lisubconam='' or b.lisubconam='null' then '0' else  b.lisubconam end liacconam\n" +
                "    from (select b.ancheid, a.pripid\n" +
                "            from probleDataTmp01 a\n" +
                "            join CGSANBASEINFOTMP011 b\n" +
                "              on a.regno = b.regno\n" +
                "             and a.entname = b.entname) a\n" +
                "    join C_GS_AN_SUBCAPITAL b\n" +
                "      on a.ancheid = b.ancheid ";

        return DataFrameUtil.getDataFrame(sqlContext,hql,"probleDataTmp02",DataFrameUtil.CACHETABLE_EAGER);
    }

    private DataFrame getFlushBadData031(HiveContext sqlContext){
        String hql = "select pripid,\n" +
                "               inv,\n" +
                "               row_number() over(partition by pripid order by inv) rk\n" +
                "          from probleDataTmp02 ";

        return  DataFrameUtil.getDataFrame(sqlContext,hql,"probleDataTmp031");
    }

    private DataFrame getFlushBadData03(HiveContext sqlContext){
        String hql = "select pripid, collect_set(inv) as inv\n" +
                "  from probleDataTmp031 \n" +
                " group by pripid";

        return  DataFrameUtil.getDataFrame(sqlContext,hql,"probleDataTmp03");
    }

    private DataFrame getFlushBadData042(HiveContext sqlContext){
        String hql = "select a.pripid, b.inv\n" +
                "                     from probleDataTmp01 a\n" +
                "                     join e_inv_investment_hdfs_ext_%s b\n" +
                "                       on a.pripid = b.pripid";

        return  DataFrameUtil.getDataFrame(sqlContext,String.format(hql,date),"probleDataTmp042");
    }

    private DataFrame getFlushBadData041(HiveContext sqlContext){
        String hql = "select pripid,inv, row_number() over(partition by pripid order by inv) rk\n" +
                "             from probleDataTmp042";

        return  DataFrameUtil.getDataFrame(sqlContext,hql,"probleDataTmp041");
    }

    private DataFrame getFlushBadData04(HiveContext sqlContext){
        String hql = "select pripid as pripidpro, collect_set(inv) as invpro\n" +
                "     from probleDataTmp041 \n" +
                "    group by pripid";

        return  DataFrameUtil.getDataFrame(sqlContext,hql,"probleDataTmp04");
    }

    private DataFrame getFlushBadData05(HiveContext sqlContext){
        String hql = "select c.pripid\n" +
                "  from (select pripid, collectsame(inv, invpro) as sety\n" +
                "          from probleDataTmp03 \n" +
                "          join probleDataTmp04 \n" +
                "            on pripid = pripidpro) c\n" +
                " where c.sety = '1' ";

        return  DataFrameUtil.getDataFrame(sqlContext,hql,"probleDataTmp05");
    }

    private DataFrame getFlushBadData06(HiveContext sqlContext){
        String hql = "select a.pripid, b.liacconam/a.regcap as radio,a.regcap\n" +
                "  from (select a.pripid, cast(a.regcap as double) regcap\n" +
                "          from probleDataTmp01 a\n" +
                "          join probleDataTmp05 b\n" +
                "            on a.pripid = b.pripid) a\n" +
                "  join (select a.pripid, cast(sum(liacconam) as double) as liacconam\n" +
                "          from probleDataTmp02 a\n" +
                "         group by pripid) b\n" +
                "    on a.pripid = b.pripid";
        return  DataFrameUtil.getDataFrame(sqlContext,hql,"probleDataTmp06");
    }

    private DataFrame getFlushBadData061(HiveContext sqlContext){
        String hql = "select pripid,regcap from  probleDataTmp06 where radio > 0.95 and radio<1.05";
        return  DataFrameUtil.getDataFrame(sqlContext,hql,"probleDataTmp061",DataFrameUtil.CACHETABLE_EAGER);
    }

    /*private void getFlushBadData062(HiveContext sqlContext){
        String hql = "select a.s_ext_nodenum,\n" +
                "       a.pripid,\n" +
                "       a.s_ext_sequence,\n" +
                "       a.invid,\n" +
                "       a.inv,\n" +
                "       a.invtype,\n" +
                "       a.certype,\n" +
                "       a.cerno,\n" +
                "       a.blictype,\n" +
                "       a.blicno,\n" +
                "       a.country,\n" +
                "       a.currency,\n" +
                "       c.liacconam as subconam,\n" +
                "       a.acconam,\n" +
                "       a.subconamusd,\n" +
                "       a.acconamusd,\n" +
                "       round(c.liacconam/b.regcap,3) as conprop,\n" +
                "       a.conform,\n" +
                "       a.condate,\n" +
                "       a.baldelper,\n" +
                "       a.conam ,\n" +
                "       a.exeaffsign,\n" +
                "       a.s_ext_timestamp,\n" +
                "       a.s_ext_batch,\n" +
                "       a.s_ext_validflag,\n" +
                "       a.linkman,\n" +
                "       a.cerno_old,\n" +
                "       a.subconam_new,\n" +
                "       a.conprop_new,\n" +
                "       a.status,\n" +
                "       a.record_stat,\n" +
                "       a.record_desc,\n" +
                "       a.match,\n" +
                "       a.zspid\n" +
                "  from e_inv_investment_hdfs_ext_%s a\n" +
                "  join probleDataTmp061 b\n" +
                "    on a.pripid = b.pripid\n" +
                "  join probleDataTmp02 c\n" +
                "    on a.pripid = c.pripid\n" +
                "   and a.inv = c.inv\n" ;
        DataFrameUtil.getDataFrame(sqlContext,String.format(hql,date),"probleDataTmp062").write().mode(SaveMode.Overwrite).parquet("/tmp/hive_export_inv/e_inv_investment_parquet_verify/");
        //return  DataFrameUtil.getDataFrame(sqlContext,hql,"probleDataTmp062");
    }*/

    private DataFrame getFlushBadData07(HiveContext sqlContext){
        String hql = "select a.s_ext_nodenum,\n" +
                "       a.pripid,\n" +
                "       a.s_ext_sequence,\n" +
                "       a.invid,\n" +
                "       a.inv,\n" +
                "       a.invtype,\n" +
                "       a.certype,\n" +
                "       a.cerno,\n" +
                "       a.blictype,\n" +
                "       a.blicno,\n" +
                "       a.country,\n" +
                "       a.currency,\n" +
                "       c.liacconam as subconam,\n" +
                "       a.acconam,\n" +
                "       a.subconamusd,\n" +
                "       a.acconamusd,\n" +
                "       round(c.liacconam/b.regcap,3) as conprop,\n" +
                "       a.conform,\n" +
                "       a.condate,\n" +
                "       a.baldelper,\n" +
                "       a.conam ,\n" +
                "       a.exeaffsign,\n" +
                "       a.s_ext_timestamp,\n" +
                "       a.s_ext_batch,\n" +
                "       a.s_ext_validflag,\n" +
                "       a.linkman,\n" +
                "       a.cerno_old,\n" +
                "       a.subconam_new,\n" +
                "       a.conprop_new,\n" +
                "       a.status,\n" +
                "       a.record_stat,\n" +
                "       a.record_desc,\n" +
                "       a.match,\n" +
                "       a.zspid\n" +
                "  from e_inv_investment_hdfs_ext_%s a\n" +
                "  join probleDataTmp061 b\n" +
                "    on a.pripid = b.pripid\n" +
                "  join probleDataTmp02 c\n" +
                "    on a.pripid = c.pripid\n" +
                "   and a.inv = c.inv\n" +
                "union\n" +
                "  select a.s_ext_nodenum,\n" +
                "        a.pripid,\n" +
                "        a.s_ext_sequence,\n" +
                "        a.invid,\n" +
                "        a.inv,\n" +
                "        a.invtype,\n" +
                "        a.certype,\n" +
                "        a.cerno,\n" +
                "        a.blictype,\n" +
                "        a.blicno,\n" +
                "        a.country,\n" +
                "        a.currency,\n" +
                "        a.subconam,\n" +
                "        a.acconam,\n" +
                "        a.subconamusd,\n" +
                "        a.acconamusd,\n" +
                "        round(a.subconam / c.regcap, 3) as conprop,\n" +
                "        a.conform,\n" +
                "        a.condate,\n" +
                "        a.baldelper,\n" +
                "        a.conam,\n" +
                "        a.exeaffsign,\n" +
                "        a.s_ext_timestamp,\n" +
                "        a.s_ext_batch,\n" +
                "        a.s_ext_validflag,\n" +
                "        a.linkman,\n" +
                "        a.cerno_old,\n" +
                "        a.subconam_new,\n" +
                "        a.conprop_new,\n" +
                "        a.status,\n" +
                "        a.record_stat,\n" +
                "        a.record_desc,\n" +
                "        a.match,\n" +
                "        a.zspid\n" +
                "   from e_inv_investment_hdfs_ext_%s a\n" +
                "   left join probleDataTmp061 b\n" +
                "     on a.pripid = b.pripid\n" +
                "  inner join (select pripid, REGCAP\n" +
                "                from enterprisebaseinfocollect_hdfs_ext_%s\n" +
                "               ) c on a.pripid = c.pripid\n" +
                "  where b.pripid is null ";

        return  DataFrameUtil.getDataFrame(sqlContext,String.format(hql,date,date,date),"probleDataTmp07");
    }

}
