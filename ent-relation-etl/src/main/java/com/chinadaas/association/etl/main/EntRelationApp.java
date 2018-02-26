package com.chinadaas.association.etl.main;

import com.chinadaas.association.etl.common.CommonApp;
import com.chinadaas.association.etl.sparksql.CommonETL;
import com.chinadaas.association.etl.sparksql.EntRelationETL;
import com.chinadaas.association.etl.udf.ScoreModelUDF;
import com.chinadaas.common.common.CommonConfig;
import com.chinadaas.common.common.DatabaseValues;
import com.chinadaas.common.register.RegisterTable;
import com.chinadaas.common.udf.CollectionSameUDF;
import com.chinadaas.common.udf.NullValueDealUDF;
import com.chinadaas.common.util.DataFrameUtil;
import com.chinadaas.common.util.LogUtil;
import com.chinadaas.common.util.MyFileUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.util.Map;

/**
 * Created by gongxs01 on 2017/5/2.
 * <p>
 * ************************************************************
 * <p>
 * 关联洞察生成关系和节点入口程序
 * 主要处理企业节点，人员节点，地址节点
 * 电话节点，投资关系，疑似关系，控股关系，参股关系
 * <p>
 * ************************************************************
 */

public class EntRelationApp {

    protected static LogUtil logger = LogUtil.getLogger(EntRelationApp.class);

    private static final String parquetPath = CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_PARQUET_TMP);

    public static void main(String[] args) throws  Exception{

        String date = args[0];
        String cfgPath = args[1];

        Map<String,String> cfg = MyFileUtil.getFileCfg(cfgPath);

        String srcPath = CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_SRCPATH_TMP);
        String dstPath = CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_DSTPATH_TMP)+date;

        SparkSession spark = SparkSession
                .builder()
                .appName("Chinadaas ENT-RELATION ETL APP")
                .enableHiveSupport()
                .getOrCreate();

        NullValueDealUDF.registerNUllUDF(spark);
        CollectionSameUDF.collectSame(spark);
        ScoreModelUDF.riskScore(spark);

        registerTable(spark,cfg);
        befor(spark);
        EntRelationETL dfEtl = new EntRelationETL();
        saveDF(spark, srcPath, dstPath, dfEtl);
        spark.close();
    }




    public static void befor(SparkSession spark){
        preproccess2Json(spark);

        CommonApp.loadAndRegiserTable(spark,new String[]{CommonApp.ENT_INFO,
                CommonApp.ENT_INV_INFO,CommonApp.ENT_PERSON_INFO,CommonApp.ABNORMITY,
                CommonApp.BREAKLAW,CommonApp.BZXR,CommonApp.SXBZR});


    }

    public static void registerTable(SparkSession spark,Map<String,String> cfg){

        RegisterTable.regiserBreakLawTable(spark,"S_EN_BREAK_LAW",cfg.get("S_EN_BREAK_LAW"));
        RegisterTable.regiserAbnormityTable(spark,"S_EN_ABNORMITY",cfg.get("S_EN_ABNORMITY"));
        RegisterTable.regiserBZXRTable(spark,"DIS_BZXR_NEW",cfg.get("DIS_BZXR_NEW"));
        RegisterTable.regiserSXBZXRTable(spark,"DIS_SXBZXR_NEW",cfg.get("DIS_SXBZXR_NEW"));
        RegisterTable.regiserInMapTable(spark,"S_CIF_INDMAP_T",cfg.get("S_CIF_INDMAP"));
        RegisterTable.registerUsednameTable(spark,"S_EN_USEDNAME",cfg.get("S_EN_USEDNAME"));

    }


    /**
     * save 5node and 6relations to many csv
     *
     * @param sqlContext
     * @param srcPath
     * @param dstPath
     */
    private static void saveDF(SparkSession sqlContext, String srcPath, String dstPath, EntRelationETL dfEtl) throws  Exception{

            //保存批次号：

            //上市公司十大股东节点关系
            saveTopTen2Csv(sqlContext,srcPath,dstPath,dfEtl);

            //企业节点 alone 1.7min
            DataFrameUtil.saveAsCsv(dfEtl.getEntDataFrame(sqlContext), srcPath);
            MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_NODE_ENT), CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_NODE_ENT_HEADER), true);

           //企业相同电话节点    4.8min
            DataFrameUtil.saveAsParquetOverwrite(dfEtl.getTelInfoDF(sqlContext),parquetPath+CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_NODE_ENTTEL));
            Dataset dfTel = sqlContext.read().load(parquetPath+CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_NODE_ENTTEL));
            dfTel.registerTempTable("telInfoTmp02");
            DataFrameUtil.saveAsCsv(dfTel, srcPath);
            MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_NODE_ENTTEL), CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_NODE_ENTTEL_HEADER), true);

            //企业相同电话关系    5min
            Dataset dfTelRa = dfEtl.getTelRelaInfoDF(sqlContext);
            DataFrameUtil.saveAsCsv(dfTelRa, srcPath);
            MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ENTTEL), CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ENTTEL_HEADER), true);

            //企业与企业相同电话关系（直接相连）    5.7min
            DataFrameUtil.saveAsParquetOverwrite(dfEtl.getEntAndEntTelInfoDF01(sqlContext),parquetPath+CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ENTANDENT_TEL));
            Dataset dfEntTel = sqlContext.read().load(parquetPath+CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ENTANDENT_TEL));
            dfEntTel.registerTempTable("telRelaInfoTmp111");
            DataFrameUtil.saveAsCsv(dfEntTel, srcPath);
            MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ENTANDENT_TEL), CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ENTANDENT_TEL_HEADER), true);
            //人员相同地址节点    1.7min
            Dataset dfPersonAr = dfEtl.getPersonAddrDataFrame(sqlContext);
            DataFrameUtil.saveAsCsv(dfPersonAr, srcPath);
            MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_NODE_PERSONADDR), CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_NODE_PERSONADDR_HEADER), true);

            //人员相同地址关系  1.5m
            Dataset dfpersonArRa = dfEtl.getPersonAddrRelaDF(sqlContext);
            DataFrameUtil.saveAsCsv(dfpersonArRa, srcPath);
            MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_PERADDR), CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_PERADDR_HEADER), true);


            //人员与人员相同地址关系（直接相连）    13min
            DataFrameUtil.saveAsParquetOverwrite(dfEtl.getPersonAndPersonDomRelaDF01(sqlContext),parquetPath+ CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_PERSONANDPERSON_ADDR));
            Dataset dfpersonAndpersonDomRa = sqlContext.read().load(parquetPath+ CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_PERSONANDPERSON_ADDR));
            dfpersonAndpersonDomRa.registerTempTable("personDomRelaTmp11");
            DataFrameUtil.saveAsCsv(dfpersonAndpersonDomRa, srcPath);
            MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_PERSONANDPERSON_ADDR), CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_PERSONANDPERSON_ADDR_HEADER), true);


             //企业相同地址节点    58s
            Dataset dfentDom = dfEtl.getEntDomInfoDF(sqlContext);
            DataFrameUtil.saveAsCsv(dfentDom, srcPath);
            MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_NODE_ENTADDR), CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_NODE_ENTADDR_HEADER), true);

            //企业相同地址关系  8min
            Dataset dfentDomRa = dfEtl.getEntDomInfoRelaDF(sqlContext);
            DataFrameUtil.saveAsCsv(dfentDomRa, srcPath);
            MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ENTADDR), CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ENTADDR_HEADER), true);
            //企业与企业相同地址关系（直接相连）    13min
            DataFrameUtil.saveAsParquetOverwrite(dfEtl.getEntAndEntDomInfoRelaDF01(sqlContext),parquetPath+ CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ENTANDENT_ADDR));
            Dataset dfentAndentDomRa = sqlContext.read().load(parquetPath+ CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ENTANDENT_ADDR));
            dfentAndentDomRa.registerTempTable("entDomRelaTmp11");
            DataFrameUtil.saveAsCsv(dfentAndentDomRa, srcPath);
            MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ENTANDENT_ADDR), CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ENTANDENT_ADDR_HEADER), true);
            //法人关系   6min
            DataFrameUtil.saveAsParquetOverwrite(dfEtl.getLegalRelaDF(sqlContext),parquetPath+CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_LEGAL));
            Dataset dfLegal = sqlContext.read().load(parquetPath+CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_LEGAL));
            dfLegal.registerTempTable("legalRelaTmp02");
            DataFrameUtil.saveAsCsv(dfLegal, srcPath);
            MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_LEGAL), CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_LEGAL_HEADER), true);
            //任职关系 14min
            DataFrameUtil.saveAsParquetOverwrite( dfEtl.getStaffRelaDF(sqlContext),parquetPath+CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_STAFF));
            Dataset dfStaff =sqlContext.read().load(parquetPath+CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_STAFF));
            dfStaff.registerTempTable("staffRelaTmp01");
            DataFrameUtil.saveAsCsv(dfStaff, srcPath);
            MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_STAFF), CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_STAFF_HEADER), true);

            //企业股东投资关系  21mingetPersonDataFrame
            dfEtl.getInvRelaDF(sqlContext).write().mode(SaveMode.Overwrite).parquet(parquetPath+CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ENTINV));
            Dataset dfInvRela = sqlContext.read().load(parquetPath+CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ENTINV));
            dfInvRela.registerTempTable("invRelaTmp04");
            DataFrameUtil.saveAsCsv(dfInvRela, srcPath);
            MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ENTINV), CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ENTINV_HEADER), true);

            //人员职位信息
            DataFrameUtil.saveAsParquetOverwrite(dfEtl.getPersonJoinRelaDF01(sqlContext),parquetPath+"personJoinRelaTmp01");
            sqlContext.read().load(parquetPath+"personJoinRelaTmp01").registerTempTable("personJoinRelaTmp01");

            //企业股东参股关系     33s
            DataFrameUtil.saveAsCsv(dfEtl.getInvJoinRelaDF(sqlContext),srcPath);
            MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_INVJOIN), CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ENTINV_HEADER), true);


            //人员股东投资关系
            DataFrameUtil.saveAsParquetOverwrite( dfEtl.getPersonInv(sqlContext),parquetPath+"personInvTmp01");
            Dataset personInvRela = sqlContext.read().load(parquetPath+"personInvTmp01");
            personInvRela.registerTempTable("personInvTmp01");

             //人员股东参股关系  10min
            DataFrameUtil.saveAsCsv(dfEtl.getPersonJoinRelaDF(sqlContext),srcPath);
            MyFileUtil.copyMergeWithHeader(srcPath, dstPath,CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_PERSONJOIN), CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_PERSONINV_HEADER), true);

//           //分支机构关系 17s
//            Dataset dfbrach = dfEtl.getBranchRelation(sqlContext);
//            DataFrameUtil.saveAsCsv(dfbrach,srcPath);
//            MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_BRANCH), CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_BRANCH_HEADER), true);

            //组织机构投资关系 20s
            DataFrameUtil.saveAsParquetOverwrite(dfEtl.getEntOrgRelatgion(sqlContext),parquetPath+CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ENTORG));
            Dataset orginvrelation = sqlContext.read().load(parquetPath+CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ENTORG));
            orginvrelation.registerTempTable("orgInv");
            DataFrameUtil.saveAsCsv(orginvrelation,srcPath);
            MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ENTORG),
                    CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ENTORG_HEADER), true);

            //组织机构节点3s
            DataFrameUtil.saveAsCsv(dfEtl.getOrgNode(sqlContext),srcPath);
            MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_NODE_ENTORG), CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_NODE_ENTORG_HEADER), true);


            //合并人员 2.9min
            DataFrameUtil.saveAsCsv(dfEtl.getPersonDataFrame(sqlContext), srcPath);
            MyFileUtil.copyMergeWithHeader(srcPath, dstPath , CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_NODE_PERSON), CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_NODE_PERSON_HEADER), true);


            //合并人员投资关系 1.8min
            DataFrameUtil.saveAsParquetOverwrite(dfEtl.getpersonOrgRelation(sqlContext),parquetPath+CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_PERSONINV));
            Dataset personInv = sqlContext.read().load(parquetPath+CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_PERSONINV));
            personInv.registerTempTable("personInv");
            DataFrameUtil.saveAsCsv(personInv, srcPath);
            MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_PERSONINV), CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_PERSONINV_HEADER), true);


            //人员股东控股关系 2.4min
            DataFrameUtil.saveAsParquetOverwrite(dfEtl.getPersonHoldRelaDF(sqlContext),parquetPath+CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_PERSONHOLD));
            Dataset personHold = sqlContext.read().load(parquetPath+CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_PERSONHOLD));
            personHold.registerTempTable("personhold");
            DataFrameUtil.saveAsCsv( personHold,srcPath);
            MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_PERSONHOLD),
                    CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_PERSONINV_HEADER), true);

            //企业股东控股关系 22s
            DataFrameUtil.saveAsParquetOverwrite(dfEtl.getInvHoldRelaDF(sqlContext),parquetPath+CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_INVHOLD));
            Dataset entHold = sqlContext.read().load(parquetPath+CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_INVHOLD));
            entHold.registerTempTable("enthold");
            DataFrameUtil.saveAsCsv(entHold,srcPath);
            MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_INVHOLD),
                    CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ENTINV_HEADER), true);

            //组织机构控股关系  37s
            DataFrameUtil.saveAsParquetOverwrite(dfEtl.getOrgHoldRelaDF(sqlContext),parquetPath+CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ORGHOLD));
            Dataset orgHold = sqlContext.read().load(parquetPath+CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ORGHOLD));
            orgHold.registerTempTable("orghold");
            DataFrameUtil.saveAsCsv(orgHold,srcPath);
            MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ORGHOLD),
             CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ORGHOLD_HEADER), true);


            //关键人员关系
            Dataset staff =sqlContext.read().load(parquetPath+CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_STAFF));
            staff.registerTempTable("staffRelationTmp");

            DataFrameUtil.saveAsParquetOverwrite(dfEtl.mainStaff(sqlContext),parquetPath+CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_MAIN_STAFF));
            Dataset mainStaff =sqlContext.read().load(parquetPath+CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_MAIN_STAFF));
            mainStaff.registerTempTable("mainstaffRelationTmp");

            DataFrameUtil.saveAsCsv(mainStaff,srcPath);
            MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_MAIN_STAFF), CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_MAIN_STAFF_HEADER), true);

            DataFrameUtil.saveAsCsv(dfEtl.getOrgInvMerge(sqlContext),srcPath);
            MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ORGMERGE), CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ORGMERGE_HEADER), true);

            //上市公司股东和工商数据股东Merge
            saveToMergeTenInv(sqlContext,srcPath,dstPath,dfEtl);

            //上市公司控股和工商数据股东Merge
            saveToMergeHold(sqlContext,srcPath,dstPath,dfEtl);

            //实质关联关系Merge_SZ
            saveToMergeTenSZRelation(sqlContext,srcPath,dstPath,dfEtl);

            //风险视图关系Merge
            saveToMergeTenFXRelation(sqlContext,srcPath,dstPath,dfEtl);
    }

    private static void saveToMergeTenInv(SparkSession sqlContext, String srcPath, String dstPath, EntRelationETL dfEtl) throws  Exception{

        //人员inv关系merge
        DataFrameUtil.saveAsCsv(dfEtl.getPersonMergeToTenListed(sqlContext),srcPath);
        MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_PERSONINVMERGE),
                CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_PERSONINVMERGE_HEADER), true);


        //企业inv关系merge
        DataFrameUtil.saveAsCsv(dfEtl.getEntMergeToTenListed(sqlContext),srcPath);
        MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ENTINVMERGE),
                CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ENTINVMERGE_HEADER), true);

        //组织机构inv关系merge
        DataFrameUtil.saveAsCsv(dfEtl.getOrgMergeToTenListed(sqlContext),srcPath);
        MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ORGINVMERGE),
                CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ORGINVMERGE_HEADER), true);

    }

    private static void saveToMergeHold(SparkSession sqlContext, String srcPath, String dstPath, EntRelationETL dfEtl) throws  Exception{

        //人员hold关系merge
        DataFrameUtil.saveAsParquetOverwrite(dfEtl.getpersonHoldMergeToTenListed(sqlContext),parquetPath+CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_PERSONHOLDMERGE));
        Dataset personHold = sqlContext.read().load(parquetPath+CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_PERSONHOLDMERGE));
        personHold.registerTempTable("personhold_sz");

        DataFrameUtil.saveAsCsv(personHold,srcPath);
        MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_PERSONHOLDMERGE),
                CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_PERSONHOLDMERGE_HEADER), true);


        //企业hold关系merge
        DataFrameUtil.saveAsCsv( dfEtl.getEntHoldMergeToTenListed(sqlContext),srcPath);
        MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ENTHOLDMERGE),
                CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ENTHOLDMERGE_HEADER), true);

        //组织机构hold关系merge
        DataFrameUtil.saveAsCsv(dfEtl.getOrgHoldMergeToTenListed(sqlContext),srcPath);
        MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ORGHOLDMERGE),
                CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ORGHOLDMERGE_HEADER), true);

    }

    private static void saveToMergeTenSZRelation(SparkSession sqlContext, String srcPath, String dstPath, EntRelationETL dfEtl) throws  Exception{

        //合并多种关系为一种关系（人员关系,实质关联关系merge_sz） 15s
        DataFrameUtil.saveAsCsv(dfEtl.getPersonMergeSZRelationDF(sqlContext),srcPath);
        MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_PERSONMERGE_SZ),
                CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_PERSONMERGE_SZ_HEADER), true);

        //合并多种关系为一种关系（企业关系,实质关联关系merge_sz）
        DataFrameUtil.saveAsCsv(dfEtl.getEntMergeSZRelationDF(sqlContext),srcPath);
        MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ENTMERGE_SZ),
                CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ENTMERGE_SZ_HEADER), true);

        //合并多种关系为一种关系（人员关系,实质关联关系merge_sz） 15s
        DataFrameUtil.saveAsCsv(dfEtl.getOrgMergeSZRelationDF(sqlContext),srcPath);
        MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ORGMERGE_SZ),
                CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ORGMERGE_SZ_HEADER), true);

    }

    private static void saveToMergeTenFXRelation(SparkSession sqlContext, String srcPath, String dstPath, EntRelationETL dfEtl) throws  Exception{

        //合并多种关系为一种关系(人员关系 风险视图关系merge)  1.4min
        Dataset ds = dfEtl.getPersonMergeRelaDF(sqlContext);
        DataFrameUtil.saveAsCsv(ds,srcPath);
        MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_PERSONMERGE),
                CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_PERSONMERGE_HEADER), true);

        //合并多种关系为一种关系（企业关系 风险视图关系merge） 15s
        Dataset dss = dfEtl.getInvMergeRelationDF(sqlContext);
        DataFrameUtil.saveAsCsv(dss,srcPath);
        MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_INVMERGE),
                CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_INVMERGE_HEADER), true);

    }


    private static void saveTopTen2Csv(SparkSession sqlContext, String srcPath, String dstPath, EntRelationETL dfEtl) throws  Exception{
        //上市公司

        DataFrameUtil.saveAsParquetOverwrite(dfEtl.beListedInfo(sqlContext),parquetPath+"entlisted");

        Dataset dfTel = sqlContext.read().load(parquetPath+"entlisted");
        dfTel.registerTempTable("comp_info_tmp");

//        十大流通股东虚拟节点
        DataFrameUtil.saveAsCsv(dfEtl.beTenInvInfo(sqlContext),srcPath);
        MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_NODE_TENINV),
                CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_NODE_TENINV_HEADER), true);

//        十大流通股东虚拟关系
        DataFrameUtil.saveAsCsv(dfEtl.beTenInvRelationInfo(sqlContext),srcPath);
        MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_TENINV),
                CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_TENINV_HEADER), true);

        //        企业股东末尾节点链接十大虚拟
        DataFrameUtil.saveAsCsv(dfEtl.beListedEntInfo(sqlContext),srcPath);
        MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_LISTENT),
                CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_LISTENT_HEADER), true);

        //组织结构节点
        DataFrameUtil.saveAsCsv(dfEtl.beTenEntOrg(sqlContext),srcPath);
        MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_NODE_LISTORG),
                CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_NODE_LISTORG_HEADER), true);


//        组织结构人员节点
        DataFrameUtil.saveAsCsv(dfEtl.beTenPersonOrg(sqlContext),srcPath);
        MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_NODE_PERSONORG),
                CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_NODE_PERSONORG_HEADER), true);


//        listedinv org
        dfEtl.beListedOrgRelation(sqlContext).
                write().mode(SaveMode.Overwrite).parquet(parquetPath+CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_LISTEDINV_ORG));
        Dataset orglistedinv = sqlContext.read().load(parquetPath+CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_LISTEDINV_ORG));
        orglistedinv.registerTempTable("listedinvorg");
        DataFrameUtil.saveAsCsv(orglistedinv,srcPath);
        MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_LISTEDINV_ORG),
                CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_LISTEDINV_ORG_HEADER), true);

//       组织机构关系末尾节点链接十大虚拟
        DataFrameUtil.saveAsCsv(dfEtl.beTenOrgRelation(sqlContext),srcPath);
        MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_LISTORG),
                CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_LISTORG_HEADER), true);

//        listedinv person
        dfEtl.beListesPersonRelation(sqlContext).select( "zsid","holderrto","sharestype","holderamt","topripid","riskscore").
                write().mode(SaveMode.Overwrite).parquet(parquetPath+CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_LISTEDINV_PERSON));
        Dataset plistedinv = sqlContext.read().load(parquetPath+CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_LISTEDINV_PERSON));
        plistedinv.registerTempTable("listedinvperson");
        DataFrameUtil.saveAsCsv(plistedinv,srcPath);
        MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_LISTEDINV_PERSON),
                CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_LISTEDINV_PERSON_HEADER), true);

//        人员股东末尾节点链接十大虚拟
        DataFrameUtil.saveAsCsv(dfEtl.beTenPersonInvRelationInfo(sqlContext),srcPath);
        MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_LISTPERSON),
                CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_LISTPERSON_HEADER), true);

//        listedinv ent
        dfEtl.beListesEntRelation(sqlContext).select("zsid","holderrto","sharestype","holderamt", "topripid","riskscore").
                write().mode(SaveMode.Overwrite).parquet(parquetPath+CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_LISTEDINV_ENT));
        Dataset elistedinv = sqlContext.read().load(parquetPath+CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_LISTEDINV_ENT));
        elistedinv.registerTempTable("listedinvent");
        DataFrameUtil.saveAsCsv(elistedinv,srcPath);
        MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_LISTEDINV_ENT),
                CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_LISTEDINV_ENT_HEADER), true);


//        hold person
        dfEtl.beTenPersonHold(sqlContext).
                write().mode(SaveMode.Overwrite).parquet(parquetPath+CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_NODE_TENPERSON));
        Dataset plistedhold = sqlContext.read().load(parquetPath+CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_NODE_TENPERSON));
        plistedhold.registerTempTable("listedholdperson");
        DataFrameUtil.saveAsCsv(plistedhold,srcPath);
        MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_NODE_TENPERSON),
                CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_TENPERSON_HEADER), true);

//        hold ent
        dfEtl.beTenEntHold(sqlContext).
                write().mode(SaveMode.Overwrite).parquet(parquetPath+CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_NODE_TENENT));
        Dataset entlistedhold = sqlContext.read().load(parquetPath+CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_NODE_TENENT));
        entlistedhold.registerTempTable("listedholdent");
        DataFrameUtil.saveAsCsv(entlistedhold,srcPath);
        MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_NODE_TENENT),
                CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_TENENT_HEADER), true);

//        hold org
        dfEtl.beTenOrgHold(sqlContext).
                write().mode(SaveMode.Overwrite).parquet(parquetPath+CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_NODE_TENORG));
        Dataset orglistedhold = sqlContext.read().load(parquetPath+CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_NODE_TENORG));
        orglistedhold.registerTempTable("listedholdorg");
        DataFrameUtil.saveAsCsv(orglistedhold,srcPath);
        MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_NODE_TENORG),
                CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_TENORG_HEADER), true);

    }

    /**
     * 【严重违法】【被执行人】【失信被执行人】【经营异常名录】json
     * @param
     * @param
     */
    private static void preproccess2Json(SparkSession spark){

        //经营异常名录
        Dataset abnormity = CommonETL.getAbnormityDF(spark);

        //严重违法
        Dataset breaklaw = CommonETL.getBreakLawDF(spark);

        //被执行人
        Dataset bzxr = CommonETL.getBzxrDF(spark);

        //失信被执行人
        Dataset sxbzr = CommonETL.getSxBzxrDF(spark);

        abnormity = abnormity.select(abnormity.col("pripid"),abnormity.col("indate"),
                functions.to_json(functions.struct("indate", "outdate", "inreason","outreason")).as("abnormity_json"));

        breaklaw = breaklaw.select(breaklaw.col("pripid"),
                functions.to_json(functions.struct("brl_inr", "brl_idat", "brl_onr","brl_odat")).as("breaklaw_json"));


        bzxr = bzxr.select(bzxr.col("fss_name"),bzxr.col("fss_time"),bzxr.col("zspid"),
                functions.to_json(functions.struct("fss_status", "fss_enfcourt", "fss_money","fss_name","fss_time")).as("bzxr_json"));

        sxbzr = sxbzr.select(sxbzr.col("fsx_name"),sxbzr.col("zspid"),sxbzr.col("fsx_fbdate"),
                functions.to_json(functions.struct("fsx_name", "fsx_sfzh_all", "fsx_frdb","fsx_zxfy","fsx_lasj",
                        "fsx_lxqk","fsx_sxjtqx","fsx_fbdate")).as("sxbzr_json"));

        DataFrameUtil.saveAsParquetOverwrite(abnormity, parquetPath + CommonApp.ABNORMITY);
        DataFrameUtil.saveAsParquetOverwrite(breaklaw, parquetPath + CommonApp.BREAKLAW);
        DataFrameUtil.saveAsParquetOverwrite(bzxr, parquetPath + CommonApp.BZXR);
        DataFrameUtil.saveAsParquetOverwrite(sxbzr, parquetPath + CommonApp.SXBZR);
    }



}
