package com.chinadaas.association.etl.sparksql;

import com.chinadaas.association.etl.common.CommonApp;
import com.chinadaas.common.common.CommonConfig;
import com.chinadaas.common.common.DatabaseValues;
import com.chinadaas.common.util.DataFrameUtil;
import com.chinadaas.common.util.TimeUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;


/**
 * Created by gongxs01 on 2017/5/15.
 */
public class Hdfs2EsETL implements Serializable{
    public String date_day;


    public void setDate_day(String date_day) {
        this.date_day = date_day;
    }

    /**
     * ES企业节点数据
     * @param sqlContext
     * @return
     */
    public Dataset getEntDataFrame(SparkSession sqlContext) {
        getEntDataFrame01(sqlContext);
        return getEntDataFrame02(sqlContext);
    }


    private Dataset getEntDataFrame01(SparkSession sqlContext) {
        String hql =" select a.pripid,b.encode_v1 " +
                "  from enterprisebaseinfocollect a inner join " +
                "  s_cif_indmap  b " +
                "  on a.zspid=b.zspid \n" +
                " WHERE a.pripid <> '' and a.zspid<>'' and a.zspid<>'null'\n";
        return DataFrameUtil.getDataFrame(sqlContext,  hql, "entDataInfoTmp01");
    }


    private Dataset getEntDataFrame02(SparkSession sqlContext) {
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
                "        1 as entityType, \n" +
                "        b.encode_v1," +
                "        c.shortname," +
                "        a.s_ext_sequence, \n" +
                "        a.data_date, \n" +
                "        concat_ws('\\u0001', a.s_ext_nodenum, a.pripid, a.s_ext_sequence) as docid,\n" +
                "       a.cbuitem\n" +
                "  from enterprisebaseinfocollect  a left join " +
                "  entDataInfoTmp01  b " +
                "  on a.pripid=b.pripid " +
                "  left join shortNameTmp01 c" +
                "  on a.entname=c.entname \n" ;
        return DataFrameUtil.getDataFrame(sqlContext, hql, "entDataInfoTmp");
    }



    public Dataset getGtEntBaseInfo(SparkSession sqlContext){
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
                "       2 as entityType," +
                "       '' as encode_v1," +
                "       '' as shortname," +
                "       s_ext_sequence, \n" +
                "       data_date, \n" +
                "       concat_ws('\\u0001', s_ext_nodenum, pripid, s_ext_sequence) as docid, \n" +
                "       cbuitem\n" +
                "  from e_gt_baseinfo \n";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "gtEntDataTmp01");
    }

    public Dataset getcifindmap(SparkSession sqlContext,String date){
        String hql =  "select distinct zspid,encode_v1 from  s_cif_indmap_hdfs_ext_%s ";
        return DataFrameUtil.getDataFrame(sqlContext,  String.format(hql,date), "S_CIF_INDMAP",DataFrameUtil.CACHETABLE_PARQUET);
    }


    //企业的投资企业
    public Dataset getEntInvDf(SparkSession sqlContext){
       /* CommonApp.loadAndRegiserTable(sqlContext,new String[]{CommonApp.ENT_INFO});
        sqlContext.read().parquet(CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_PARQUET_TMP)+
                CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_INV_PARQUET_PATH)).
                registerTempTable(CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_INV_PARQUET_PATH));*/

//        getcifindmap(sqlContext,date_day);
        return getEntInfoDf02(sqlContext);
    }

    private  Dataset getEntInfoDf02(SparkSession sqlContext){
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
                "       b.encode_v1," +
                "       a.s_ext_sequence\n" +
                "  from e_inv_investment a" +
                "  left join s_cif_indmap b " +
                "  on a.zspid=b.zspid " +
                "  where a.zspid<>'null' and a.zspid<>'' ";
        return DataFrameUtil.getDataFrame(sqlContext,hql, "invDataTmp01");
    }


    public  Dataset getPersonManagerDf(SparkSession sqlContext){
//        CommonApp.loadAndRegiserTable(sqlContext,new String[]{CommonApp.ENT_PERSON_INFO});
        getPersonManagerDf01(sqlContext);
        getPersonManagerDf02(sqlContext);
        return getPersonManagerDf03(sqlContext);
    }

    private  Dataset getPersonManagerDf01(SparkSession sqlContext){
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
                "       then genrandom('_ZSPID') else a.zspid end zspid,a.s_ext_sequence " +
                " from e_pri_person a" +
                " where a.pripid<>'' and a.pripid <> 'null'";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "epripersonTmp01");
    }

    private  Dataset getPersonManagerDf02(SparkSession sqlContext){
        String hql = "select a.*,b.encode_v1" +
                "     from epripersonTmp01 a " +
                "     left join S_CIF_INDMAP b" +
                "     on a.zspid=b.zspid ";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "personDataTmp01");
    }
    private  Dataset getPersonManagerDf03(SparkSession sqlContext){
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
                "       a.encode_v1," +
                "       a.s_ext_sequence\n" +
                "  from personDataTmp01 a\n";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "invDataTmp05");
    }


    public  Dataset getGtPersonManagerDf(SparkSession sqlContext){
//        getcifindmap(sqlContext);

        getGtPerson(sqlContext);
        getGtPersonManagerDf01(sqlContext);
        Dataset ds =  getGtPersonManagerDf02(sqlContext);
        return ds;
    }

    private  Dataset getGtPersonManagerDf01(SparkSession sqlContext){
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
                "           c.encode_v1," +
                "           b.s_ext_sequence" +
                "    from gtpersonTmp01 b" +
                "    left join S_CIF_INDMAP c " +
                "    on b.zspid=c.zspid " +
                "    where b.pripid<>'' and b.pripid <> 'null'";

        return DataFrameUtil.getDataFrame(sqlContext, hql, "invDataTmp04");
    }

    private  Dataset getGtPersonManagerDf02(SparkSession sqlContext){
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
                "       b.encode_v1," +
                        " b.s_ext_sequence\n" +
                "  from invDataTmp04 b" +
                        " inner join e_gt_baseinfo c" +
                " on b.name=c.name  and b.pripid= c.pripid";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "gtPersonManagerTmp01");
    }


    public Dataset getAlterDataDF(SparkSession sqlContext){
        String hql = "select *,from_unixtime(unix_timestamp(),'yyyy-MM-dd') as write_date,concat_ws('-',s_ext_nodenum,pripid,s_ext_sequence) as alter_id from e_alter_recoder";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "invDataTmp02");
    }

    public Dataset getGtPerson(SparkSession sqlContext){
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
                "            then genrandom('_ZSPID') else zspid end zspid,s_ext_sequence " +
                "            from " +
                "            e_gt_person ";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "gtpersonTmp01");
    }



    private   Dataset getEntInfoDf03(SparkSession sqlContext){
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


    private   Dataset getEntInfoDf04(SparkSession sqlContext){
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

    private   Dataset getEntInfoDf05(SparkSession sqlContext){
        String hql = "select * from (select *, row_number() over(partition by key) rk\n" +
                "             from invDataTmp04) ent\n" +
                "             where ent.rk = 1";
        return DataFrameUtil.getDataFrame(sqlContext, hql.toString(), "invDataTmp05");
    }

    public Dataset getV1Code(SparkSession sqlContext){
        String hql = "select encode_v1,zspid from S_CIF_INDMAP";

       return DataFrameUtil.getDataFrame(sqlContext,hql,"tmpv1code");
    }
}
