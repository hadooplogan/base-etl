package com.chinadaas.association.etl.main;

import com.chinadaas.association.etl.sparksql.EntRelationETL;
import com.chinadaas.association.etl.sparksql.FlushConpropETL;
import com.chinadaas.common.common.CommonConfig;
import com.chinadaas.common.common.DatabaseValues;
import com.chinadaas.common.udf.CollectionSameUDF;
import com.chinadaas.common.util.DataFormatConvertUtil;
import com.chinadaas.common.util.DataFrameUtil;
import com.chinadaas.common.util.MyFileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;

/**
 * Created by gongxs01 on 2017/5/2.
 */
public class App {
    public static void main(String[] args) {
        if(args.length<1){
            System.out.println("please input databatch date!  date format yyyymmdd  :20170508");

            return ;
        }
        //验证date日期格式
        if(!DataFormatConvertUtil.isValidDate(args[0])){
            System.out.println("please input date format yyyymmdd  for example:20170508");
            return ;
        }

        String date = args[0];
        SparkConf conf = new SparkConf().setAppName("Chinadaas Association ETL APP");
        SparkContext sc = new SparkContext(conf);
        HiveContext sqlContext = new HiveContext(sc);
        CollectionSameUDF.collectSame(sc,sqlContext);
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

            //person
            DataFrame dfPerson = dfEtl.getPersonDataFrame(sqlContext);
            DataFrameUtil.saveAsCsv(dfPerson, srcPath);
            MyFileUtil.copyMergeWithHeader(new Path(srcPath), new Path(dstPath + CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_NODE_PERSON)), CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_NODE_PERSON_HEADER), true);
            //ent
            DataFrame dfEnt = dfEtl.getEntDataFrame(sqlContext);
            DataFrameUtil.saveAsCsv(dfEnt, srcPath);
            MyFileUtil.copyMergeWithHeader(new Path(srcPath), new Path(dstPath + CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_NODE_ENT)), CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_NODE_ENT_HEADER), true);

            //ent same tel
            DataFrame dfTel = dfEtl.getTelInfoDF(sqlContext);
            DataFrameUtil.saveAsCsv(dfTel, srcPath);
            MyFileUtil.copyMergeWithHeader(new Path(srcPath), new Path(dstPath + CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_NODE_ENTTEL)), CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_NODE_ENTTEL_HEADER), true);

            //ent same tel relation
            DataFrame dfTelRa = dfEtl.getTelRelaInfoDF(sqlContext);
            DataFrameUtil.saveAsCsv(dfTelRa, srcPath);
            MyFileUtil.copyMergeWithHeader(new Path(srcPath), new Path(dstPath + CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ENTTEL)), CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ENTTEL_HEADER), true);
            sqlContext.clearCache();

            //person same addr
            DataFrame dfPersonAr = dfEtl.getPersonAddrDataFrame(sqlContext);
            DataFrameUtil.saveAsCsv(dfPersonAr, srcPath);
            MyFileUtil.copyMergeWithHeader(new Path(srcPath), new Path(dstPath + CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_NODE_PERSONADDR)), CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_NODE_PERSONADDR_HEADER), true);
            //person same addr relation
            DataFrame dfpersonArRa = dfEtl.getPersonAddrRelaDF(sqlContext);
            DataFrameUtil.saveAsCsv(dfpersonArRa, srcPath);
            MyFileUtil.copyMergeWithHeader(new Path(srcPath), new Path(dstPath + CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_PERADDR)), CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_PERADDR_HEADER), true);
            sqlContext.clearCache();

            //ent same addr
            DataFrame dfentDom = dfEtl.getEntDomInfoDF(sqlContext);
            DataFrameUtil.saveAsCsv(dfentDom, srcPath);
            MyFileUtil.copyMergeWithHeader(new Path(srcPath), new Path(dstPath + CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_NODE_ENTADDR)), CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_NODE_ENTADDR_HEADER), true);
            //ent same addr  relation
            DataFrame dfentDomRa = dfEtl.getEntDomInfoRelaDF(sqlContext);
            DataFrameUtil.saveAsCsv(dfentDomRa, srcPath);
            MyFileUtil.copyMergeWithHeader(new Path(srcPath), new Path(dstPath + CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ENTADDR)), CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ENTADDR_HEADER), true);
            sqlContext.clearCache();

            //legal relation
            DataFrame dfLegal = dfEtl.getLegalRelaDF(sqlContext);
            DataFrameUtil.saveAsCsv(dfLegal, srcPath);
            MyFileUtil.copyMergeWithHeader(new Path(srcPath), new Path(dstPath + CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_LEGAL)), CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_LEGAL_HEADER), true);

            //staff relation
            DataFrame dfStaff = dfEtl.getStaffRelaDF(sqlContext);
            DataFrameUtil.saveAsCsv(dfStaff, srcPath);
            MyFileUtil.copyMergeWithHeader(new Path(srcPath), new Path(dstPath + CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_STAFF)), CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_STAFF_HEADER), true);

            //entinv relation
            DataFrame dfInvRela = dfEtl.getInvRelaDF(sqlContext);
            DataFrameUtil.saveAsCsv(dfInvRela, srcPath);
            MyFileUtil.copyMergeWithHeader(new Path(srcPath), new Path(dstPath + CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ENTINV)), CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ENTINV_HEADER), true);

            //entinv hold relation
            DataFrame dfholdRela = dfEtl.getInvHoldRelaDF(sqlContext);
            DataFrameUtil.saveAsCsv(dfholdRela,srcPath);
            MyFileUtil.copyMergeWithHeader(new Path(srcPath), new Path(dstPath + CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_INVHOLD)), CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ENTINV_HEADER), true);

            //entinv join relation
            DataFrame dfInvJoinRela = dfEtl.getInvJoinRelaDF(sqlContext);
            DataFrameUtil.saveAsCsv(dfInvJoinRela,srcPath);
            MyFileUtil.copyMergeWithHeader(new Path(srcPath), new Path(dstPath + CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_INVJOIN)), CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_ENTINV_HEADER), true);
            sqlContext.uncacheTable("invRelaTmp04");

            //personinv relation
            DataFrame personInvRela = dfEtl.getPersonInv(sqlContext);
            DataFrameUtil.saveAsCsv(personInvRela, srcPath);
            MyFileUtil.copyMergeWithHeader(new Path(srcPath), new Path(dstPath + CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_PERSONINV)), CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_PERSONINV_HEADER), true);

            //person hold relation
            DataFrame dfPersonHoldRela = dfEtl.getPersonHoldRelaDF(sqlContext);
            DataFrameUtil.saveAsCsv(dfPersonHoldRela,srcPath);
            MyFileUtil.copyMergeWithHeader(new Path(srcPath), new Path(dstPath + CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_PERSONHOLD)), CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_PERSONINV_HEADER), true);

            //person join relation
            DataFrame dfPersonJoinRela = dfEtl.getPersonJoinRelaDF(sqlContext);
            DataFrameUtil.saveAsCsv(dfPersonJoinRela,srcPath);
            MyFileUtil.copyMergeWithHeader(new Path(srcPath), new Path(dstPath + CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_PERSONJOIN)), CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_RELATION_PERSONINV_HEADER), true);
            sqlContext.uncacheTable("personInvTmp01");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
