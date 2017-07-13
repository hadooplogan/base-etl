package com.chinadaas.association.etl.main;

import com.chinadaas.association.etl.sparksql.EntRelationETL;
import com.chinadaas.association.etl.udf.ScoreModelUDF;
import com.chinadaas.common.common.CommonConfig;
import com.chinadaas.common.common.DatabaseValues;
import com.chinadaas.common.udf.CollectionSameUDF;
import com.chinadaas.common.util.DataFrameUtil;
import com.chinadaas.common.util.LogUtil;
import com.chinadaas.common.util.MyFileUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;
import org.slf4j.LoggerFactory;

/**
 * Created by gongxs01 on 2017/5/2.
 *
 * ************************************************************
 * 关联洞察生成关系和节点入口程序
 * 主要处理企业节点，人员节点，地址节点
 * 电话节点，投资关系，疑似关系，控股关系，参股关系
 * ***********************************************************
 */
public class EntRelationApp {
    protected static LogUtil logUtil = LogUtil.getLogger(LoggerFactory.getLogger("common"));
    public static void main(String[] args) {
        if(args.length<1){
            System.out.println("please input databatch date!  date format yyyymmdd  :20170508");

            return ;
        }
//        logUtil.info("","");
//
//        logUtil.info("please input databatch date!  date format yyyymmdd  :20170508","");
//        logUtil.debug("debug"+"11111111111","");
//        logUtil.error("debug"+"333333",EntRelationApp.class);

       /* //验证date日期格式
        if(!DataFormatConvertUtil.isValidDate(args[0])){
            System.out.println("please input date format yyyymmdd  for example:20170508");
            return ;
        }*/

        String date = args[0];
        SparkConf conf = new SparkConf().setAppName("Chinadaas ENT-RELATION ETL APP");
        SparkContext sc = new SparkContext(conf);
        HiveContext sqlContext = new HiveContext(sc);
        sqlContext.setConf("spark.sql.tungsten.enabled", "false");
        CollectionSameUDF.collectSame(sc,sqlContext);
        ScoreModelUDF.riskScore(sc,sqlContext);
        String srcPath = CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_SRCPATH_TMP);
        String dstPath = CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_DSTPATH_TMP);
        DataFrame radio = sqlContext.load(CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_INV_RADIO_PATH));
        radio.registerTempTable("e_inv_investment_parquet");
        EntRelationETL dfEtl = new EntRelationETL();
        dfEtl.setDate(date);
        System.out.println("date format "+date);
        saveDF(sqlContext, srcPath, dstPath,dfEtl);
        sqlContext.clearCache();
        sc.stop();
    }

    /**
     * save 5node and 6relations to many csv
     *
     * @param sqlContext
     * @param srcPath
     * @param dstPath
     */
    private static void saveDF(HiveContext sqlContext, String srcPath, String dstPath,EntRelationETL dfEtl) {
        try {

            String parquetPath = CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_PARQUET_TMP);

            //共同企业基本信息
            DataFrameUtil.saveAsParquetOverwrite(dfEtl.getEntInfo01(sqlContext),parquetPath+"entInfoTmp03");
            sqlContext.load(parquetPath+"entInfoTmp03").registerTempTable("entInfoTmp03");

            //企业节点
            DataFrameUtil.saveAsCsv(dfEtl.getEntDataFrame(sqlContext), srcPath);
            MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_NODE_ENT), CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_NODE_ENT_HEADER), true);

            //企业相同电话节点
            DataFrameUtil.saveAsParquetOverwrite(dfEtl.getTelInfoDF(sqlContext),CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_NODE_ENTTEL));
            DataFrame dfTel = sqlContext.load(parquetPath+CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_NODE_ENTTEL));
            dfTel.registerTempTable("telInfoTmp02");
            DataFrameUtil.saveAsCsv(dfTel, srcPath);
            MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_NODE_ENTTEL), CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_NODE_ENTTEL_HEADER), true);

            //企业相同电话关系
            DataFrame dfTelRa = dfEtl.getTelRelaInfoDF(sqlContext);
            DataFrameUtil.saveAsCsv(dfTelRa, srcPath);
            MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ENTTEL), CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ENTTEL_HEADER), true);

            //企业与企业相同电话关系
            DataFrameUtil.saveAsParquetOverwrite(dfEtl.getEntAndEntTelInfoDF(sqlContext),parquetPath+CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ENTANDENT_TEL));
            DataFrame dfEntTel = sqlContext.load(parquetPath+CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ENTANDENT_TEL));
            dfEntTel.registerTempTable("telRelaInfoTmp111");
            DataFrameUtil.saveAsCsv(dfEntTel, srcPath);
            MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ENTANDENT_TEL), CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ENTANDENT_TEL_HEADER), true);
            sqlContext.clearCache();

            //人员相同地址节点
            DataFrame dfPersonAr = dfEtl.getPersonAddrDataFrame(sqlContext);
            DataFrameUtil.saveAsCsv(dfPersonAr, srcPath);
            MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_NODE_PERSONADDR), CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_NODE_PERSONADDR_HEADER), true);

            //人员相同地址关系
            DataFrame dfpersonArRa = dfEtl.getPersonAddrRelaDF(sqlContext);
            DataFrameUtil.saveAsCsv(dfpersonArRa, srcPath);
            MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_PERADDR), CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_PERADDR_HEADER), true);
            sqlContext.clearCache();

            //企业相同地址节点
            DataFrame dfentDom = dfEtl.getEntDomInfoDF(sqlContext);
            DataFrameUtil.saveAsCsv(dfentDom, srcPath);
            MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_NODE_ENTADDR), CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_NODE_ENTADDR_HEADER), true);

            //企业相同地址关系
            DataFrame dfentDomRa = dfEtl.getEntDomInfoRelaDF(sqlContext);
            DataFrameUtil.saveAsCsv(dfentDomRa, srcPath);
            MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ENTADDR), CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ENTADDR_HEADER), true);

            //企业与企业相同地址关系（直接相连）
            DataFrameUtil.saveAsParquetOverwrite(dfEtl.getEntAndEntDomInfoRelaDF(sqlContext),parquetPath+ CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ENTANDENT_ADDR));
            DataFrame dfentAndentDomRa = sqlContext.load(parquetPath+ CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ENTANDENT_ADDR));
            dfentAndentDomRa.registerTempTable("entDomRelaTmp11");
            DataFrameUtil.saveAsCsv(dfentAndentDomRa, srcPath);
            MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ENTANDENT_ADDR), CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ENTANDENT_ADDR_HEADER), true);
            sqlContext.clearCache();

            //法人关系
            DataFrameUtil.saveAsParquetOverwrite(dfEtl.getLegalRelaDF(sqlContext),parquetPath+CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_LEGAL));
            DataFrame dfLegal = sqlContext.load(parquetPath+CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_LEGAL));
            dfLegal.registerTempTable("legalRelaTmp02");
            DataFrameUtil.saveAsCsv(dfLegal, srcPath);
            MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_LEGAL), CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_LEGAL_HEADER), true);
            sqlContext.clearCache();

            //任职关系
            DataFrameUtil.saveAsParquetOverwrite( dfEtl.getStaffRelaDF(sqlContext),parquetPath+CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_STAFF));
            DataFrame dfStaff =sqlContext.load(parquetPath+CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_STAFF));
            dfStaff.registerTempTable("staffRelaTmp01");
            DataFrameUtil.saveAsCsv(dfStaff, srcPath);
            MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_STAFF), CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_STAFF_HEADER), true);
            sqlContext.clearCache();

            //企业股东投资关系
            dfEtl.getInvRelaDF(sqlContext).write().mode(SaveMode.Overwrite).parquet(parquetPath+CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ENTINV));
            DataFrame dfInvRela = sqlContext.load(parquetPath+CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ENTINV));
            dfInvRela.registerTempTable("invRelaTmp04");
            DataFrameUtil.saveAsCsv(dfInvRela, srcPath);
            MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ENTINV), CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ENTINV_HEADER), true);
            sqlContext.clearCache();

            //人员职位信息
            DataFrameUtil.saveAsParquetOverwrite(dfEtl.getPersonJoinRelaDF01(sqlContext),parquetPath+"personJoinRelaTmp01");
            sqlContext.load(parquetPath+"personJoinRelaTmp01").registerTempTable("personJoinRelaTmp01");

            //企业股东参股关系
            DataFrameUtil.saveAsCsv(dfEtl.getInvJoinRelaDF(sqlContext),srcPath);
            MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_INVJOIN), CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ENTINV_HEADER), true);
            sqlContext.clearCache();

            //人员股东投资关系
            DataFrameUtil.saveAsParquetOverwrite( dfEtl.getPersonInv(sqlContext),parquetPath+CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_PERSONINV));
            DataFrame personInvRela = sqlContext.load(parquetPath+CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_PERSONINV));
            personInvRela.registerTempTable("personInvTmp01");

             //人员股东参股关系
            DataFrameUtil.saveAsCsv(dfEtl.getPersonJoinRelaDF(sqlContext),srcPath);
            MyFileUtil.copyMergeWithHeader(srcPath, dstPath,CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_PERSONJOIN), CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_PERSONINV_HEADER), true);

            //合并多种关系为一种关系(人员关系)
            DataFrameUtil.saveAsCsv(dfEtl.getPersonMergeRelaDF(sqlContext),srcPath);
            MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_PERSONMERGE), CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_PERSONMERGE_HEADER), true);
            //合并多种关系为一种关系（企业关系）
            DataFrame dfInvMergeRrelation = dfEtl.getInvMergeRelationDF(sqlContext);
            DataFrameUtil.saveAsCsv(dfInvMergeRrelation,srcPath);
            MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_INVMERGE), CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_INVMERGE_HEADER), true);
            //分支机构关系
            DataFrame dfbrach = dfEtl.getBranchRelation(sqlContext);
            DataFrameUtil.saveAsCsv(dfbrach,srcPath);
            MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_BRANCH), CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_BRANCH_HEADER), true);

            //组织机构投资关系
            DataFrameUtil.saveAsCsv(dfEtl.getEntOrgRelatgion(sqlContext),srcPath);
            MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ENTORG), CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ENTORG_HEADER), true);

            //组织机构节点
            DataFrameUtil.saveAsCsv(dfEtl.getOrgNode(sqlContext),srcPath);
            MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_NODE_ENTORG), CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_NODE_ENTORG_HEADER), true);

            //合并人员
            DataFrameUtil.saveAsCsv(dfEtl.getPersonDataFrame(sqlContext), srcPath);
            MyFileUtil.copyMergeWithHeader(srcPath, dstPath , CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_NODE_PERSON), CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_NODE_PERSON_HEADER), true);


            //合并人员投资关系
            DataFrameUtil.saveAsCsv(dfEtl.getpersonOrgRelation(sqlContext), srcPath);
            MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_PERSONINV), CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_PERSONINV_HEADER), true);


            //人员股东控股关系
            DataFrameUtil.saveAsCsv( dfEtl.getPersonHoldRelaDF(sqlContext),srcPath);
            MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_PERSONHOLD), CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_PERSONINV_HEADER), true);

            //企业股东控股关系
            DataFrameUtil.saveAsCsv(dfEtl.getInvHoldRelaDF(sqlContext),srcPath);
            MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_INVHOLD), CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ENTINV_HEADER), true);

            //组织机构控股关系
            DataFrameUtil.saveAsCsv(dfEtl.getOrgHoldRelaDF(sqlContext),srcPath);
            MyFileUtil.copyMergeWithHeader(srcPath, dstPath, CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ORGHOLD), CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ORGHOLD_HEADER), true);
            sqlContext.clearCache();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
