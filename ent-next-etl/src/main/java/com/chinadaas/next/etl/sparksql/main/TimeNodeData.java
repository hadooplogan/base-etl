package com.chinadaas.next.etl.sparksql.main;

import com.chinadaas.common.common.CommonConfig;
import com.chinadaas.common.common.Constants;
import com.chinadaas.common.load.impl.ParquetDataAdapterImpl;
import com.chinadaas.common.util.LogUtil;
import com.chinadaas.next.etl.sparksql.sql.AddTimeNode;
import org.apache.spark.sql.SparkSession;

/**
 * @author haoxing
 *
 * 存储历史的数标签，加上了timestamp，然后存储批次的数据到hdfs上
 */
public class TimeNodeData {
    protected static LogUtil logger = LogUtil.getLogger(TimeNodeData.class);
   static ParquetDataAdapterImpl parquetDataAdapterImpl = new ParquetDataAdapterImpl();
    public static void main(String[] args) {

        String timenode = args[0];

        SparkSession spark = SparkSession.
                builder().
                enableHiveSupport().
                appName("tme-node-label").
                getOrCreate();
        logger.info("------------开始处理合并数据----------------------------");


        parquetDataAdapterImpl.loadData(spark, CommonConfig.getValue(Constants.ENT_INDEX_BASE_DIR)).createOrReplaceTempView("basebikpitemp");
        parquetDataAdapterImpl.loadData(spark, CommonConfig.getValue(Constants.ENT_INDEX_ABNORMITY_DIR)).createOrReplaceTempView("abnormitykpitemp");
        parquetDataAdapterImpl.loadData(spark, CommonConfig.getValue(Constants.ENT_INDEX_BREAKLAW_DIR)).createOrReplaceTempView("breaklawkpitemp");
        parquetDataAdapterImpl.loadData(spark, CommonConfig.getValue(Constants.ENT_INDEX_CASEINFO_DIR)).createOrReplaceTempView("caseinfokpitemp");
        parquetDataAdapterImpl.loadData(spark, CommonConfig.getValue(Constants.ENT_INDEX_ENTERPRISE_EXCHANGE_DIR)).createOrReplaceTempView("tmpenterpriseChange");
        parquetDataAdapterImpl.loadData(spark, CommonConfig.getValue(Constants.ENT_INDEX_PUBLISHSOFTWORK_DIR)).createOrReplaceTempView("tmppublishsoftwork");
        parquetDataAdapterImpl.loadData(spark, CommonConfig.getValue(Constants.ENT_INDEX_SUBCOMPANY_DIR)).createOrReplaceTempView("tmpsubconpany");
        parquetDataAdapterImpl.loadData(spark, CommonConfig.getValue(Constants.ENT_INDEX_TRADEMARKINFO_DIR)).createOrReplaceTempView("tmptrademarkinfo");
        parquetDataAdapterImpl.loadData(spark, CommonConfig.getValue(Constants.ENT_INDEX_PUBLISHCOPYRIGHT_DIR)).createOrReplaceTempView("tmppublishcopyright");
        parquetDataAdapterImpl.loadData(spark, CommonConfig.getValue(Constants.ENT_INDEX_STOCKOFCOMPANY_DIR)).createOrReplaceTempView("stockcompanttmp");
        parquetDataAdapterImpl.loadData(spark, CommonConfig.getValue(Constants.ENT_INDEX_ENTERPRISEINVESTMENT_DIR)).createOrReplaceTempView("enterpriseinvestment");
        parquetDataAdapterImpl.loadData(spark, CommonConfig.getValue(Constants.ENT_INDEX_TOPEXPERIENCE_DIR)).createOrReplaceTempView("topexperiencekpi");
        parquetDataAdapterImpl.loadData(spark, CommonConfig.getValue(Constants.ENT_INDEX_LEGALOFFICE_DIR)).createOrReplaceTempView("legalofficetmp");
        parquetDataAdapterImpl.loadData(spark, CommonConfig.getValue(Constants.ENT_INDEX_LEGALINVESTMENT_DIR)).createOrReplaceTempView("legalinvestmenttmp");

        logger.info("------------载入数据完成----------------------------");


        logger.info("----------开始存储时间节点的企业标签数据-------");
        saveTohdfs(spark,timenode);
        logger.info("----------结束存储时间节点的企业标签数据-------");
    }


    public static void saveTohdfs(SparkSession spark,String datadate){

        parquetDataAdapterImpl.writeData(AddTimeNode.getAddTimeNode(spark,datadate),"/next/timenode/"+datadate);

    }
}
