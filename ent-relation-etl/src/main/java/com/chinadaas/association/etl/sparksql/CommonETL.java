package com.chinadaas.association.etl.sparksql;

import com.chinadaas.common.util.DataFrameUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.functions.*;

/**
 * Created by gongxs01 on 2017/7/26.
 */
public class CommonETL {

    public static Dataset getEntInfo(SparkSession sqlContext, String date) {
        String hql =" select s_ext_nodenum,\n" +
                "       pripid,\n" +
                "       s_ext_sequence,\n" +
                "       entname,\n" +
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
                "       forentname,\n" +
                "       country,\n" +
                "       entname_old,\n" +
                "       name,\n" +
                "       ancheyear,\n" +
                "       candate,\n" +
                "       revdate,\n" +
                "       case when (licid='' or licid='null' or licid is null) and length(credit_code)>17 then substr(credit_code,9,9) else licid end licid ,\n" +
                "       credit_code,\n" +
                "       case when (tax_code='' or tax_code='null' or tax_code is null) and length(credit_code)>17 then substr(credit_code,3,13) else tax_code end tax_code,\n" +
                "       zspid,\n" +
                "       empnum,\n" +
                "       cerno," +
                "       oriregno, " +
                "       data_date " +
                "  from enterprisebaseinfocollect WHERE pripid <> ''";

        return DataFrameUtil.getDataFrame(sqlContext,hql , "entInfoTmp03");
    }

    public static Dataset getEntInvInfo(SparkSession sqlContext, String date) {
        getEntInvInfo01(sqlContext,date);


        return getEntInvInfo02(sqlContext,date);
    }

    private static  Dataset getEntInvInfo01(SparkSession sqlContext, String date) {
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
                "       regexp_replace(a.certype,'!','') as certype,\n" +
                "       a.cerno,\n" +
                "       a.blictype,\n" +
                "       a.blicno,\n" +
                "       a.country,\n" +
                "       a.currency,\n" +
                "       a.subconam,\n" +
                "       a.acconam,\n" +
                "       a.subconamusd,\n" +
                "       a.acconamusd,\n" +
                "        round(a.subconam / b.regcap, 3) as conprop,\n" +
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
                "  from e_inv_investment a\n " +
                "  join entInfoTmp03 b " +
                "  on a.pripid = b.pripid ";


        return DataFrameUtil.getDataFrame(sqlContext,hql , "entInfoTmp010");
    }

    private static Dataset  getEntInvInfo02(SparkSession sqlContext, String date) {
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
                "       a.subconam,\n" +
                "       a.acconam,\n" +
                "       a.subconamusd,\n" +
                "       a.acconamusd,\n" +
                "       case when a.conprop ='' or a.conprop ='null' or a.conprop ='NULL' or a.conprop is null " +
                "            then '0.0'" +
                "       else a.conprop  end conprop,\n" +
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
                "  from entInfoTmp010 a\n " ;


        return DataFrameUtil.getDataFrame(sqlContext,hql , "entInfoTmp");
    }


    public static Dataset getEntPersonInfo(SparkSession sqlContext, String date) {
        String hql =  "SELECT " +
                "       a.s_ext_nodenum,\n" +
                "       a.pripid,\n" +
                "       a.s_ext_sequence,\n" +
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
                "       a.zspid,\n" +
                "       a.dom,\n" +
                "       a.data_date\n" +
                " FROM e_pri_person a" +
                " WHERE a.pripid<>'' and a.pripid <> 'null'";
        return DataFrameUtil.getDataFrame(sqlContext,hql , "entPersonTmp");
    }



    //经营异常名录
    public static Dataset getAbnormityDF(SparkSession sqlContext) {
        String hql =  "SELECT " +
                "       a.entname,\n" +
                "       a.pripid,\n" +
                "       case when emptydeal(a.indate)='1900-01-01' then '' else emptydeal(a.indate)  end indate,\n" +
                "       case when emptydeal(a.outdate)='1900-01-01' then '' else emptydeal(a.outdate) end outdate,\n" +
                "       emptydeal(a.inreason) as inreason,\n" +
                "       emptydeal(a.outreason) as outreason\n" +
                " FROM (select * from s_en_abnormity where date_add(date_idt,  365) >= from_unixtime(unix_timestamp(), 'yyyy-MM-dd')) a" +
                " join (select max(indate) as indate, pripid from s_en_abnormity group by pripid) b"+
                " on a.indate=b.indate " +
                " and a.pripid=b.pripid";

        return DataFrameUtil.getDataFrame(sqlContext,hql , "abnormityTmp");
    }

    //严重违法
    public static Dataset getBreakLawDF(SparkSession sqlContext) {
        String hql =  "SELECT " +
                "       emptydeal(a.brl_inr) as brl_inr,\n" +
                "       case when a.brl_idat='1900-01-01' then '' else   emptydeal(a.brl_idat) end  brl_idat,\n" +
                "       emptydeal(a.brl_onr) as brl_onr,\n" +
                "       case when a.brl_odat='1900-01-01' then '' else  emptydeal(a.brl_odat)  end brl_odat,\n" +
                "       a.pripid\n" +
                " FROM s_en_break_law a" ;

        return DataFrameUtil.getDataFrame(sqlContext,hql , "breakLawTmp");
    }

    //被执行人
    public static Dataset getBzxrDF(SparkSession sqlContext) {
        String hql =  "SELECT " +
                "       emptydeal(a.casestate) as fss_status ,\n" +
                "       emptydeal(a.courtname) as fss_enfcourt,\n" +
                "       emptydeal(a.execmoney) as fss_money ,\n" +
                "       emptydeal(a.iname) as fss_name,\n" +
                "       case when a.regdate='1900-01-01' then '' else  emptydeal(a.regdate) end fss_time,\n" +
                "       a.zspid\n" +
                " FROM (select * from dis_bzxr_new where date_add(substr(regexp_replace(regdate,'年|月|日','-'),1,10),  365) >=from_unixtime(unix_timestamp(), 'yyyyMMdd')) a" +
                " join (select max(regdate) as regdate, iname from dis_bzxr_new group by iname)b" +
                " on a.regdate=b.regdate" +
                " and a.iname=b.iname" ;

        return DataFrameUtil.getDataFrame(sqlContext,hql , "abnormityTmp");
    }

    //失信被执行人
    public static Dataset getSxBzxrDF(SparkSession sqlContext) {
        String hql =  "SELECT " +
                "       emptydeal(a.iname) as fsx_name, \n" +
                "       emptydeal(a.cardnum) as  fsx_sfzh_all,\n" +
                "       emptydeal(a.businessentity) as fsx_frdb ,\n" +
                "       emptydeal(a.courtname) as fsx_zxfy,\n" +
                "       case when emptydeal(a.regdate)='1900-01-01' then '' else emptydeal(a.regdate) end fsx_lasj,\n" +
                "       emptydeal(a.performance) as fsx_lxqk,\n" +
                "       emptydeal(a.disrupttypename) as fsx_sxjtqx,\n" +
                "       a.zspid,\n" +
                "       case when emptydeal(a.publishdate)='1900-01-01' then '' else emptydeal(a.publishdate) end fsx_fbdate\n" +
                " FROM (select * from dis_sxbzxr_new where date_add(substr(regexp_replace(publishdate,'年|月|日','-'),1,10),  365) >=from_unixtime(unix_timestamp(), 'yyyy-MM-dd'))a " +
                " join (select max(publishdate) as publishdate, iname from dis_sxbzxr_new group by iname) b" +
                " on a.publishdate=b.publishdate" +
                " and a.iname=b.iname";

        return DataFrameUtil.getDataFrame(sqlContext,hql , "abnormityTmp");
    }




/*    private static Dataset getJsonColum(Dataset ds,String key,String... clom){
        return ds.select(ds.col(key), functions.to_json(functions.struct(key,clom)));
    }*/

   /* public static Dataset getSxBzxrDF(SparkSession sqlContext, String date) {
        String hql =  "SELECT " +
                "       a.tax_name,\n" +
                "       a.leg_detail,\n" +
                "       a.fina_detail,\n" +
                "       a.case_type,\n" +
                "       a.pripid\n" +
                " FROM dis_sxbzxr_new a" ;
        return DataFrameUtil.getDataFrame(sqlContext,String.format(hql,date) , "abnormityTmp");
    }*/

   /* private static Dataset convertData(Dataset ds,Class c){
        ds.toJavaRDD().map



        return null;

    }*/



}
