package com.chinadaas.association.etl.sparksql;

import com.chinadaas.association.etl.common.CommonApp;
import com.chinadaas.common.util.DataFrameUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * Created by gongxs01 on 2017/6/13.
 */
public class FlushConpropETL {
    public String date;
    public void setDate(String date) {
        this.date = date;
    }
    public Dataset getFlushBadData(SparkSession sqlContext) {
        CommonApp.loadAndRegiserTable(sqlContext,new String[]{CommonApp.ENT_INFO,CommonApp.ENT_INV_INFO});
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
        getFlushBadData07(sqlContext);
        Dataset df = getFlushBadData08(sqlContext);
        return df;
    }


    private Dataset getFlushBadData01(SparkSession sqlContext){
        String hql = "select *\n" +
                "  from (select a.pripid, b.regno, b.entname,b.regcap, a.conam / b.regcap as bili\n" +
                "          from (select pripid, sum(subconam) as conam\n" +
                "                  from entInvTmp\n" +
                "                 group by pripid) a\n" +
                "          join (select pripid, regcap, regno, entname\n" +
                "                 from entInfoTmp03\n" +
                "                ) b\n" +
                "            on a.pripid = b.pripid\n" +
                "         where b.REGCAP <> '0'\n" +
                "           and b.REGCAP <> '') f\n" +
                " where f.bili > 1.05\n" +
                "    or f.bili < 0.95";
        return DataFrameUtil.getDataFrame(sqlContext,hql,"probleDataTmp01",DataFrameUtil.CACHETABLE_EAGER);
    }

    private Dataset getFlushBadData011(SparkSession sqlContext){
        String hql = "select b.ancheid, b.regno, b.entname\n" +
                "  from (select pripid, max(ancheyear) ancheyear\n" +
                "          from e_annreport_baseinfo\n" +
                "         group by pripid) a\n" +
                "  join e_annreport_baseinfo b\n" +
                "    on a.pripid = b.pripid\n" +
                "   and a.ancheyear = b.ancheyear\n";
        return DataFrameUtil.getDataFrame(sqlContext,hql,"CGSANBASEINFOTMP011");
    }


    private Dataset getFlushBadData02(SparkSession sqlContext){
        String hql = "select a.pripid, b.inv,case when b.lisubconam is null or b.lisubconam='' or b.lisubconam='null' then '0' else  b.lisubconam end liacconam\n" +
                "    from (select b.ancheid, a.pripid\n" +
                "            from probleDataTmp01 a\n" +
                "            join CGSANBASEINFOTMP011 b\n" +
                "              on a.regno = b.regno\n" +
                "             and a.entname = b.entname) a\n" +
                "    join e_annreport_subcapital b\n" +
                "      on a.ancheid = b.ancheid ";

        return DataFrameUtil.getDataFrame(sqlContext,hql,"probleDataTmp02",DataFrameUtil.CACHETABLE_EAGER);
    }


    private Dataset getFlushBadData031(SparkSession sqlContext){
        String hql = "select pripid,\n" +
                "               inv,\n" +
                "               row_number() over(partition by pripid order by inv) rk\n" +
                "          from probleDataTmp02 ";

        return  DataFrameUtil.getDataFrame(sqlContext,hql,"probleDataTmp031");
    }

    private Dataset getFlushBadData03(SparkSession sqlContext){
        String hql = "select pripid, collect_set(inv) as inv\n" +
                "  from probleDataTmp031 \n" +
                " group by pripid";

        return  DataFrameUtil.getDataFrame(sqlContext,hql,"probleDataTmp03");
    }

    private Dataset getFlushBadData042(SparkSession sqlContext){
        String hql = "select a.pripid, b.inv\n" +
                "                     from probleDataTmp01 a\n" +
                "                     join entInvTmp b\n" +
                "                       on a.pripid = b.pripid";

        return  DataFrameUtil.getDataFrame(sqlContext,hql,"probleDataTmp042");
    }

    private Dataset getFlushBadData041(SparkSession sqlContext){
        String hql = "select pripid,inv, row_number() over(partition by pripid order by inv) rk\n" +
                "             from probleDataTmp042";

        return  DataFrameUtil.getDataFrame(sqlContext,hql,"probleDataTmp041");
    }

    private Dataset getFlushBadData04(SparkSession sqlContext){
        String hql = "select pripid as pripidpro, " +
                "            collect_set(inv) as invpro\n" +
                "            from probleDataTmp041 \n" +
                "            group by pripid";

        return  DataFrameUtil.getDataFrame(sqlContext,hql,"probleDataTmp04");
    }

    private Dataset getFlushBadData05(SparkSession sqlContext){
        String hql = "select c.pripid\n" +
                "  from (select pripid, collectsame(inv, invpro) as sety\n" +
                "          from probleDataTmp03 \n" +
                "          join probleDataTmp04 \n" +
                "            on pripid = pripidpro) c\n" +
                " where c.sety = '1' ";

        return  DataFrameUtil.getDataFrame(sqlContext,hql,"probleDataTmp05");
    }

    private Dataset getFlushBadData06(SparkSession sqlContext){
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

    private Dataset getFlushBadData061(SparkSession sqlContext){
        String hql = "select pripid,regcap from  probleDataTmp06 where radio > 0.95 and radio<1.05";
        return  DataFrameUtil.getDataFrame(sqlContext,hql,"probleDataTmp061",DataFrameUtil.CACHETABLE_EAGER);
    }

    /*private void getFlushBadData062(SparkSession sqlContext){
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

    private Dataset getFlushBadData07(SparkSession sqlContext){
        String hql = "select a.s_ext_nodenum,\n" +
                "       a.pripid,\n" +
                "       a.s_ext_sequence,\n" +
                "       a.invid,\n" +
                "       a.inv,\n" +
                "       a.invtype,\n" +
                "       regexp_replace(a.certype,'!','') as certype,\n" +
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
                "  from entInvTmp a\n" +
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
                "        regexp_replace(a.invtype, '!', '') as invtype,\n" +
                "        regexp_replace(a.certype,'!','') as certype ,\n" +
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
                "   from entInvTmp a\n" +
                "   left join probleDataTmp061 b\n" +
                "     on a.pripid = b.pripid\n" +
                "  inner join (select pripid, REGCAP\n" +
                "                from entInfoTmp03 \n" +
                "               ) c on a.pripid = c.pripid\n" +
                "  where b.pripid is null ";

        return  DataFrameUtil.getDataFrame(sqlContext,hql,"probleDataTmp07");
    }

    private Dataset getFlushBadData08(SparkSession sqlContext){
        String hql = "select a.s_ext_nodenum,\n" +
                "       a.pripid,\n" +
                "       a.s_ext_sequence,\n" +
                "       a.invid,\n" +
                "       a.inv,\n" +
                "       case\n" +
                "         when (a.invtype = '' or a.invtype = 'null' or a.invtype is null) and\n" +
                "              (a.certype <> '' and a.certype <> 'null' and\n" +
                "              a.certype is not null) and substr(a.certype, 1, 1) = 'P' then\n" +
                "          '77'\n" +
                "         when (a.invtype = '' or a.invtype = 'null' or a.invtype is null) and\n" +
                "              (a.certype <> '' and a.certype <> 'null' and\n" +
                "              a.certype is not null) and substr(a.certype, 1, 1) = 'C' then\n" +
                "          '88'\n" +
                "         when (a.invtype = '' or a.invtype = 'null' or a.invtype is null) and\n" +
                "              (a.certype = '' or a.certype = 'null' or a.certype is null) and\n" +
                "              length(a.inv) < 4 then\n" +
                "          '77'\n" +
                "         when (a.invtype = '' or a.invtype = 'null' or a.invtype is null) and\n" +
                "              (a.certype = '' or a.certype = 'null' or a.certype is null) and\n" +
                "              length(a.inv) >= 4 then\n" +
                "          '88'\n" +
                "         else\n" +
                "          a.invtype\n" +
                "       end invtype,\n" +
                "       a.certype,\n" +
                "       a.cerno,\n" +
                "       a.blictype,\n" +
                "       a.blicno,\n" +
                "       a.country,\n" +
                "       a.currency,\n" +
                "       a.subconam,\n" +
                "       a.acconam,\n" +
                "       a.subconamusd,\n" +
                "       a.acconamusd,\n" +
                "       case when a.conprop ='' or a.conprop ='null' or a.conprop ='NULL' or a.conprop is null " +
                "            then '0.0'" +
                "       else a.conprop  end conprop,\n" +
                "       a.conform,\n" +
                "       case when a.condate='1900-01-01' " +
                "            then '' " +
                "            else a.condate end condate,\n" +
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
                "  from probleDataTmp07 a\n";

        return  DataFrameUtil.getDataFrame(sqlContext,hql,"probleDataTmp08");
    }
}
