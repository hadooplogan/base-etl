package com.chinadaas.association.etl.sparksql;

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

        String hql =" select s_ext_nodenum,\n" +
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
                "       opscope,\n" +
                "       opform,\n" +
                "       dom,\n" +
                "       reccap,\n" +
                "       regcapcur,\n" +
                "       stringhandle(forentname) as forentname,\n" +
                "       country,\n" +
                "       stringhandle(entname_old) as entname_old,\n" +
                "       stringhandle(name) as name,\n" +
                "       ancheyear,\n" +
                "       candate,\n" +
                "       revdate,\n" +
                "       case when (licid='' or licid='null' or licid is null) and length(credit_code)>17 then substr(credit_code,9,9) else licid end licid ,\n" +
                "       credit_code,\n" +
                "       case when (tax_code='' or tax_code='null' or tax_code is null) and length(credit_code)>17 then substr(credit_code,3,13) else tax_code end tax_code,\n" +
                "       zspid\n" +
                "  from entInfoTmp03 WHERE pripid <> ''";


        return DataFrameUtil.getDataFrame(sqlContext, hql.toString(), "entDataInfoTmp");
    }


    //企业的投资企业
    public  DataFrame getEntInvDf(HiveContext sqlContext){
//      return  getEntInfoDf01(sqlContext);
        return getEntInfoDf02(sqlContext);
//        getEntInfoDf03(sqlContext);
//        getEntInfoDf04(sqlContext);
//        return getEntInfoDf05(sqlContext);
    }

    private   DataFrame getEntInfoDf01(HiveContext sqlContext){
        String hql = "select pripid, regno, credit_code,entname from enterprisebaseinfocollect_hdfs_ext_%s\n";
        return DataFrameUtil.getDataFrame(sqlContext, String.format(hql,date), "entDataTmp",DataFrameUtil.CACHETABLE_EAGER);
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
                "  left join s_cif_indmap_hdfs_ext_%s b " +
                "  on a.zspid=b.zspid " +
                "  where a.zspid<>'null' and a.zspid<>'' ";
        return DataFrameUtil.getDataFrame(sqlContext,String.format(hql,date), "invDataTmp01");
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


    public  DataFrame getPersonManagerDf(HiveContext sqlContext){
       String hql = "select a.s_ext_nodenum,\n" +
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
               "       a.zspid," +
               "       b.encode_v1  \n" +
               "  from e_pri_person_hdfs_ext_%s a " +
               "  left join s_cif_indmap_hdfs_ext_%s b " +
               "  on a.zspid=b.zspid " +
               "  where pripid<>'' and pripid <> 'null' and a.zspid <>'null'";
        return DataFrameUtil.getDataFrame(sqlContext, String.format(hql,date,date), "invDataTmp02");
    }

    public DataFrame getAlterDataDF(HiveContext sqlContext){
        String hql = "select * from e_alter_recoder_hdfs_ext_%s";

        return DataFrameUtil.getDataFrame(sqlContext, String.format(hql,date), "invDataTmp02");
    }
}
