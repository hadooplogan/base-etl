package com.chinadaas.association.etl.sparksql;

import com.chinadaas.association.etl.common.CommonApp;
import com.chinadaas.common.common.CommonConfig;
import com.chinadaas.common.common.DatabaseValues;
import com.chinadaas.common.util.DataFrameUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * Created by hejianning on 2017/9/20.
 * 上海电信查询企业信息 spark sql
 */
public class Hdfs2EsETL4ShangHai {


    public Dataset getEntDataFrame(SparkSession sqlContext) {
        //先加载数据
        getEntDataFrame01(sqlContext);
        getAnnReport(sqlContext);
        getCodeList(sqlContext);
        return getEntDataFrame02(sqlContext);

    }

    public Dataset getEntDataFrame01(SparkSession sqlContext) {
        String hql ="select s_ext_nodenum,\n" +
                "       pripid,\n" +
                "       stringhandle(entname) as entname,\n" +
                "       regno,\n" +
                "       enttype,\n" +
                "       industryphy,\n" +
                "       industryco,\n" +
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
                "       dom,\n" +
                "       regcapcur,\n" +
                "       stringhandle(entname_old) as entname_old,\n" +
                "       stringhandle(name) as name,\n" +
                "       candate,\n" +
                "       revdate,\n" +
                "       case\n" +
                "         when (licid = '' or licid = 'null' or licid is null) and\n" +
                "              length(credit_code) > 17 then\n" +
                "          substr(credit_code, 9, 9)\n" +
                "         else\n" +
                "          licid\n" +
                "       end licid,\n" +
                "       credit_code,\n" +
                "       case\n" +
                "         when (tax_code = '' or tax_code = 'null' or tax_code is null) and\n" +
                "              length(credit_code) > 17 then\n" +
                "          substr(credit_code, 3, 13)\n" +
                "         else\n" +
                "          tax_code\n" +
                "       end tax_code,\n" +
                "       oriregno,\n" +
                "       s_ext_sequence,\n" +
                "       '1' as entitytype\n" +
                "  from enterprisebaseinfocollect  \n" +
                "       union\n" +
                " select s_ext_nodenum,\n" +
                "       pripid,\n" +
                "       stringhandle(entname) as entname,\n" +
                "       regno,\n" +
                "       enttype,\n" +
                "       industryphy,\n" +
                "       industryco,\n" +
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
                "       '' as dom,\n" +
                "       '' as regcapcur,\n" +
                "       '' as entname_old,\n" +
                "       stringhandle(name) as name,\n" +
                "       '' as candate,\n" +
                "       '' as revdate,\n" +
                "       '' as licid,\n" +
                "       credit_code,\n" +
                "       '' as tax_code,\n" +
                "       oriregno,\n" +
                "       s_ext_sequence,\n" +
                "       '2' as entitytype\n" +
                "  from e_gt_baseinfo \n";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "entDataInfoTmp");
    }


    private Dataset getAnnReport(SparkSession sqlContext){
        String hql = "select b.tel, b.email, b.addr,b.ancheyear,b.pripid\n" +
                "  from (select pripid, max(ancheyear) ancheyear\n" +
                "          from e_annreport_baseinfo_hdfs_ext_20171110\n" +
                "         group by pripid) a\n" +
                "  join e_annreport_baseinfo_hdfs_ext_20171110 b\n" +
                "    on a.pripid = b.pripid\n" +
                "   and a.ancheyear = b.ancheyear\n";
        return DataFrameUtil.getDataFrame(sqlContext,hql,"cgsanbaseinfotmp011");
    }


    //t_dex_app_codelist
    private Dataset getCodeList(SparkSession spark){
        String hql = "select codevalue,codename from t_dex_app_codelist t where  code_type_value='CA12' ";

        return DataFrameUtil.getDataFrame(spark,hql,"codelist",DataFrameUtil.CACHETABLE_EAGER);
    }

    private Dataset getEntDataFrame02(SparkSession sqlContext){
        String hql = "select a.s_ext_nodenum,\n" +
                "               a.pripid,\n" +
                "               a.entname,\n" +
                "               a.regno,\n" +
                "               a.enttype,\n" +
                "               a.industryphy,\n" +
                "               a.industryco,\n" +
                "               a.opfrom,\n" +
                "               a.opto,\n" +
                "               a.postalcode,\n" +
                "               a.tel,\n" +
                "               a.email,\n" +
                "               a.esdate,\n" +
                "               a.apprdate,\n" +
                "               c.codename as regorg,\n" +
                "               a.entstatus,\n" +
                "               a.regcap,\n" +
                "               a.opscope,\n" +
                "               a.dom,\n" +
                "               a.regcapcur,\n" +
                "               a.entname_old,\n" +
                "               a.name,\n" +
                "               a.candate,\n" +
                "               a.revdate,\n" +
                "               a.licid,\n" +
                "               a.credit_code,\n" +
                "               a.tax_code,\n" +
                "               a.oriregno,\n" +
                "               b.addr,\n" +
                "               b.ancheyear," +
                "               a.regorg as regorgcode,\n" +
                "               b.tel as ancheyear_tel," +
                "               b.email as ancheyear_email," +
                "               a.s_ext_sequence, " +
                "               a.entitytype" +
                "          from entDataInfoTmp a\n" +
                "          left join cgsanbaseinfotmp011 b\n" +
                "            on a.pripid = b.pripid " +
                "          left join codelist c " +
                "            on a.regorg=c.codevalue ";
        return DataFrameUtil.getDataFrame(sqlContext,hql,"cgsanbaseinfotmp012");
    }

    //企业的投资企业
    public Dataset getEntInvDf(SparkSession sqlContext){
        sqlContext.read().parquet(CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_PARQUET_TMP)+
                CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_INV_PARQUET_PATH)).
                registerTempTable(CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_INV_PARQUET_PATH));
        return getEntInfoDf02(sqlContext);
    }


    private  Dataset getEntInfoDf02(SparkSession sqlContext){
        String hql="select a.s_ext_nodenum,\n" +
                "       a.pripid,\n" +
                "       a.inv,\n" +
                "       a.invtype,\n" +
                "       a.subconam,\n" +
                "       a.condate,\n" +
                "       a.currency,\n" +
                "       a.s_ext_sequence\n" +
                "  from e_inv_investment_parquet a";
        return DataFrameUtil.getDataFrame(sqlContext,hql, "invDataTmp01");
    }




}
