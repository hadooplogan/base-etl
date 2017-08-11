package com.chinadaas.association.etl.sparksql;

import com.chinadaas.association.etl.common.CommonApp;
import com.chinadaas.common.common.CommonConfig;
import com.chinadaas.common.common.DatabaseValues;
import com.chinadaas.common.util.DataFrameUtil;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

import java.io.Serializable;


/**
 * Created by gongxs01 on 2017/5/15.
 */
public class Hdfs2EsETL implements Serializable{
    public String date;
    public void setDate(String date) {
        this.date = date;
    }

    /**
     * ES企业节点数据
     * @param sqlContext
     * @return
     */
    public DataFrame getEntDataFrame(HiveContext sqlContext) {
        getEntDataFrame01(sqlContext);
        return getEntDataFrame02(sqlContext);
    }


    private DataFrame getEntDataFrame01(HiveContext sqlContext) {
        String hql =" select a.pripid,b.encode_v1 " +
                "  from entInfoTmp03 a inner join " +
                "  getcifindmaptmp01  b " +
                "  on a.zspid=b.zspid \n" +
                " WHERE a.pripid <> '' and a.zspid<>'' and a.zspid<>'null'\n";
        return DataFrameUtil.getDataFrame(sqlContext,  hql, "entDataInfoTmp01");
    }


    private DataFrame getEntDataFrame02(HiveContext sqlContext) {
        String hql ="select a.s_ext_nodenum,\n" +
                "       a.pripid,\n" +
                "       stringhandle(a.entname) as entname,\n" +
                "       a.regno,\n" +
                "       a.enttype,\n" +
                "       a.industryphy,\n" +
                "       a.industryco,\n" +
                "       a.abuitem,\n" +
                "       a.opfrom,\n" +
                "       a.opto,\n" +
                "       a.postalcode,\n" +
                "       a.tel,\n" +
                "       a.email,\n" +
                "       a.esdate,\n" +
                "       a.apprdate,\n" +
                "       a.regorg,\n" +
                "       a.entstatus,\n" +
                "       a.regcap,\n" +
                "       a.opscope,\n" +
                "       a.opform,\n" +
                "       a.dom,\n" +
                "       a.reccap,\n" +
                "       a.regcapcur,\n" +
                "       stringhandle(a.forentname) as forentname,\n" +
                "       a.country,\n" +
                "       stringhandle(a.entname_old) as entname_old,\n" +
                "       stringhandle(a.name) as name,\n" +
                "       a.ancheyear,\n" +
                "       a.candate,\n" +
                "       a.revdate,\n" +
                "       case\n" +
                "         when (a.licid = '' or a.licid = 'null' or a.licid is null) and\n" +
                "              length(a.credit_code) > 17 then\n" +
                "          substr(a.credit_code, 9, 9)\n" +
                "         else\n" +
                "          a.licid\n" +
                "       end licid,\n" +
                "       a.credit_code,\n" +
                "       case\n" +
                "         when (a.tax_code = '' or a.tax_code = 'null' or a.tax_code is null) and\n" +
                "              length(a.credit_code) > 17 then\n" +
                "          substr(a.credit_code, 3, 13)\n" +
                "         else\n" +
                "          tax_code\n" +
                "       end tax_code,\n" +
                "       a.zspid,\n" +
                "       a.empnum,\n" +
                "       a.cerno,\n" +
                "       a.oriregno,\n" +
                "       '1' as entityType, \n" +
                "        b.encode_v1," +
                "        c.shortname \n" +
                "  from entInfoTmp03  a left join " +
                "  entDataInfoTmp01  b " +
                "  on a.pripid=b.pripid " +
                "  left join shortNameTmp01 c" +
                "  on a.entname=c.entname \n" ;
        return DataFrameUtil.getDataFrame(sqlContext, hql, "entDataInfoTmp");
    }



    public DataFrame getGtEntBaseInfo(HiveContext sqlContext){
        String hql =
                " select s_ext_nodenum,\n" +
                "       pripid,\n" +
                "       stringhandle(entname) as entname,\n" +
                "       regno,\n" +
                "       enttype,\n" +
                "       industryphy,\n" +
                "       industryco,\n" +
                "       abuitem,\n" +
                "       opfrom,\n" +
                "       opto,\n" +
                "       postalcode,\n" +
                "       tel,\n" +
                "       email,\n" +
                "       esdate,\n" +
                "       apprdate,\n" +
                "       regorg,\n" +
                "       entstatus,\n" +
                "       regcap,\n" +
                "       '' as opscope,\n" +
                "       '' as opform,\n" +
                "       '' as dom,\n" +
                "       '' as reccap,\n" +
                "       '' as regcapcur,\n" +
                "       '' as forentname,\n" +
                "       '' as country,\n" +
                "       '' as entname_old,\n" +
                "       stringhandle(name) as name,\n" +
                "       '' as ancheyear,\n" +
                "       '' as candate,\n" +
                "       '' as revdate,\n" +
                "       '' as licid,\n" +
                "       credit_code,\n" +
                "       '' as tax_code,\n" +
                "       '' as zspid,\n" +
                "       empnum,\n" +
                "       '' as cerno,\n" +
                "       oriregno,\n" +
                "       '2' as entityType," +
                "       '' as encode_v1," +
                "       '' as shortname\n" +
                "  from e_gt_baseinfo_hdfs_ext_%s \n";
        return DataFrameUtil.getDataFrame(sqlContext, String.format(hql,date), "gtEntDataTmp01");
    }


    public DataFrame getcifindmap(HiveContext sqlContext){
        String hql =  "select distinct zspid,encode_v1 from  s_cif_indmap_hdfs_ext_%s ";
        return DataFrameUtil.getDataFrame(sqlContext,  String.format(hql,date), "getcifindmaptmp01",DataFrameUtil.CACHETABLE_PARQUET);
    }


    //企业的投资企业
    public  DataFrame getEntInvDf(HiveContext sqlContext){
        CommonApp.loadAndRegiserTable(sqlContext,new String[]{CommonApp.ENT_INFO});
        sqlContext.load(CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_INV_RADIO_PATH)).registerTempTable("e_inv_investment_parquet");
        getcifindmap(sqlContext);
        return getEntInfoDf02(sqlContext);
    }

    private  DataFrame getEntInfoDf02(HiveContext sqlContext){
        String hql="select a.s_ext_nodenum,\n" +
                "       a.pripid,\n" +
                "       a.invid,\n" +
                "       a.inv,\n" +
                "       a.invtype,\n" +
                "       a.certype,\n" +
                "       a.cerno,\n" +
                "       a.blictype,\n" +
                "       a.blicno,\n" +
                "       a.country,\n" +
                "       a.currency,\n" +
                "       a.subconam,\n" +
                "       a.acconam,\n" +
                "       a.conprop,\n" +
                "       a.conform,\n" +
                "       a.condate,\n" +
                "       a.conam,\n" +
                "       a.cerno_old,\n" +
                "       a.zspid," +
                "       b.encode_v1\n" +
                "  from e_inv_investment_parquet a" +
                "  left join getcifindmaptmp01 b " +
                "  on a.zspid=b.zspid " +
                "  where a.zspid<>'null' and a.zspid<>'' ";
        return DataFrameUtil.getDataFrame(sqlContext,hql, "invDataTmp01");
    }


    public  DataFrame getPersonManagerDf(HiveContext sqlContext){
        CommonApp.loadAndRegiserTable(sqlContext,new String[]{CommonApp.ENT_PERSON_INFO});
        getPersonManagerDf01(sqlContext);
        getPersonManagerDf02(sqlContext);
        return getPersonManagerDf03(sqlContext);
    }

    private  DataFrame getPersonManagerDf01(HiveContext sqlContext){
        String hql =  "select " +
                "       a.s_ext_nodenum,\n" +
                "       a.pripid,\n" +
                "       a.name,\n" +
                "       a.certype,\n" +
                "       a.cerno,\n" +
                "       a.sex,\n" +
                "       a.natdate,\n" +
                "       a.lerepsign,\n" +
                "       a.country,\n" +
                "       a.position,\n" +
                "       a.offhfrom,\n" +
                "       a.offhto,\n" +
                "       case when a.zspid ='' or a.zspid='null' or a.zspid is null " +
                "       then genrandom('_ZSPID') else a.zspid end zspid " +
                " from entPersonTmp a" +
                " where a.pripid<>'' and a.pripid <> 'null'";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "epripersonTmp01");
    }

    private  DataFrame getPersonManagerDf02(HiveContext sqlContext){
        String hql = "select a.*,b.encode_v1" +
                "     from epripersonTmp01 a " +
                "     left join getcifindmaptmp01 b" +
                "     on a.zspid=b.zspid ";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "personDataTmp01");
    }
    private  DataFrame getPersonManagerDf03(HiveContext sqlContext){
        String hql ="select a.s_ext_nodenum,\n" +
                "       a.pripid,\n" +
                "       a.name,\n" +
                "       a.certype,\n" +
                "       a.cerno,\n" +
                "       a.sex,\n" +
                "       a.natdate,\n" +
                "       a.lerepsign,\n" +
                "       a.country,\n" +
                "       a.position,\n" +
                "       a.offhfrom,\n" +
                "       a.offhto,\n" +
                "       case\n" +
                "         when substr(a.zspid, 38, 6) = 'ZSPID' then\n" +
                "          ''\n" +
                "         else\n" +
                "          a.zspid\n" +
                "       end zspid,\n" +
                "       a.encode_v1\n" +
                "  from personDataTmp01 a\n";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "invDataTmp05");
    }


    public  DataFrame getGtPersonManagerDf(HiveContext sqlContext){
        getGtPerson(sqlContext);
        getGtPersonManagerDf01(sqlContext);
        return getGtPersonManagerDf02(sqlContext);
    }

    private  DataFrame getGtPersonManagerDf01(HiveContext sqlContext){
        String hql ="select " +
                "           b.s_ext_nodenum," +
                "           b.pripid," +
                "           b.name, " +
                "           b.certype," +
                "           b.cerno," +
                "           b.sex," +
                "           b.natdate," +
                "           '' as lerepsign," +
                "           b.country," +
                "           '' as position," +
                "           '' as offhfrom," +
                "           '' as offhto," +
                "           b.zspid," +
                "           c.encode_v1" +
                "    from gtpersonTmp01 b" +
                "    left join getcifindmaptmp01 c " +
                "    on b.zspid=c.zspid " +
                "    where b.pripid<>'' and b.pripid <> 'null'";

        return DataFrameUtil.getDataFrame(sqlContext, hql, "invDataTmp04");
    }

    private  DataFrame getGtPersonManagerDf02(HiveContext sqlContext){
        String hql =
                "select b.s_ext_nodenum,\n" +
                "       b.pripid,\n" +
                "       b.name,\n" +
                "       b.certype,\n" +
                "       b.cerno,\n" +
                "       b.sex,\n" +
                "       b.natdate,\n" +
                "       '' as lerepsign,\n" +
                "       b.country,\n" +
                "       '' as position,\n" +
                "       '' as offhfrom,\n" +
                "       '' as offhto,\n" +
                "       case\n" +
                "         when substr(b.zspid, 38, 6) = 'ZSPID' then\n" +
                "          ''\n" +
                "         else\n" +
                "          b.zspid\n" +
                "       end zspid,\n" +
                "       b.encode_v1\n" +
                "  from invDataTmp04 b " +
                " inner join e_gt_baseinfo_hdfs_ext_%s c" +
                " on b.name=c.name  and b.pripid= c.pripid";
        return DataFrameUtil.getDataFrame(sqlContext, String.format(hql,date), "gtPersonManagerTmp01");
    }


    public DataFrame getAlterDataDF(HiveContext sqlContext){
        String hql = "select * from e_alter_recoder_hdfs_ext_%s";
        return DataFrameUtil.getDataFrame(sqlContext, String.format(hql,date), "invDataTmp02");
    }

    public DataFrame getGtPerson(HiveContext sqlContext){
        String hql
                = "select s_ext_nodenum," +
                "            pripid," +
                "            name," +
                "            certype," +
                "            cerno," +
                "            sex," +
                "            natdate," +
                "            country," +
                "            case when zspid ='' or zspid='null' or zspid is null " +
                "            then genrandom('_ZSPID') else zspid end zspid " +
                "            from " +
                "            e_gt_person_hdfs_ext_%s ";
        return DataFrameUtil.getDataFrame(sqlContext, String.format(hql,date), "gtpersonTmp01");
    }



    private   DataFrame getEntInfoDf03(HiveContext sqlContext){
        String hql = "select hd.*\n" +
                "  from (select * from entDataTmp where credit_code <> '') en,\n" +
                "       (select * from invDataTmp01 where blicno <> '') hd\n" +
                " where hd.blicno = en.credit_code\n" +
                "   and en.pripid <> hd.pripid\n" +
                "union all\n" +
                "select hd.*\n" +
                "  from (select * from entDataTmp) en,\n" +
                "        (select * from invDataTmp01 where inv <> '') hd\n" +
                "        where hd.inv = en.entname";
        return DataFrameUtil.getDataFrame(sqlContext, hql.toString(), "invDataTmp02");
    }


    private   DataFrame getEntInfoDf04(HiveContext sqlContext){
        String hql = "select s_ext_nodenum,\n" +
                "                       pripid,\n" +
                "                       invid,\n" +
                "                       inv,\n" +
                "                       invtype,\n" +
                "                       certype,\n" +
                "                       cerno,\n" +
                "                       blictype,\n" +
                "                       blicno,\n" +
                "                       country,\n" +
                "                       currency,\n" +
                "                       subconam,\n" +
                "                       acconam,\n" +
                "                       conprop,\n" +
                "                       conform,\n" +
                "                       condate,\n" +
                "                       conam,\n" +
                "                       cerno_old,\n" +
                "                       zspid,\n" +
                "                       concat_ws('-',pripid, inv, blicno) as key\n" +
                "                  from invDataTmp02";
        return DataFrameUtil.getDataFrame(sqlContext, hql.toString(), "invDataTmp04");
    }

    private   DataFrame getEntInfoDf05(HiveContext sqlContext){
        String hql = "select * from (select *, row_number() over(partition by key) rk\n" +
                "             from invDataTmp04) ent\n" +
                "             where ent.rk = 1";
        return DataFrameUtil.getDataFrame(sqlContext, hql.toString(), "invDataTmp05");
    }


}
