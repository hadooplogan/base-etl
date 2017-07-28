package com.chinadaas.association.etl.sparksql;

import com.chinadaas.common.util.DataFrameUtil;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

/**
 * Created by gongxs01 on 2017/7/26.
 */
public class CommonETL {

    public static DataFrame getEntInfo(HiveContext sqlContext, String date) {
        String hql =" select s_ext_nodenum,\n" +
                "       pripid,\n" +
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
                "       oriregno " +
                "  from enterprisebaseinfocollect_hdfs_ext_%s WHERE pripid <> ''";

        return DataFrameUtil.getDataFrame(sqlContext,String.format(hql,date) , "entInfoTmp03");
    }


    public static DataFrame getEntInvInfo(HiveContext sqlContext, String date) {
        String hql = "  select a.s_ext_nodenum,\n" +
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
                "        a.conprop,\n" +
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
                "   from e_inv_investment_hdfs_ext_%s a\n" ;

        return DataFrameUtil.getDataFrame(sqlContext,String.format(hql,date) , "entInfoTmp");
    }


    public static DataFrame getEntPersonInfo(HiveContext sqlContext, String date) {
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
                "       a.zspid,\n" +
                "       a.dom\n" +
                " from e_pri_person_hdfs_ext_%s a" +
                " where a.pripid<>'' and a.pripid <> 'null'";
        return DataFrameUtil.getDataFrame(sqlContext,String.format(hql,date) , "entPersonTmp");
    }

}
