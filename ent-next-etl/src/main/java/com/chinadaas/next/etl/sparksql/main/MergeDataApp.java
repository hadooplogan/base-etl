package com.chinadaas.next.etl.sparksql.main;

import com.chinadaas.common.common.CommonConfig;
import com.chinadaas.common.common.Constants;
import com.chinadaas.common.common.DatabaseValues;
import com.chinadaas.common.load.IDataAdapter;
import com.chinadaas.common.load.impl.EsDataAdapterImpl;
import com.chinadaas.common.load.impl.ParquetDataAdapterImpl;
import com.chinadaas.common.util.LogUtil;
import com.chinadaas.next.etl.sparksql.sql.MergeDataETL;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

/**
 * @author haoxing
 * @description:
 * @Date: 16:20 2017/9/8
 */
public class MergeDataApp {

    protected static LogUtil logger = LogUtil.getLogger(MergeDataApp.class);
    static IDataAdapter ida = new EsDataAdapterImpl();

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Chinadaas MERGE_DATA_APP")
                .enableHiveSupport()
                .getOrCreate();
        spark.sqlContext().conf().setConfString("spark.sql.shuffle.partitions", "800");
        String date = args[0];


        logger.info("------------开始处理合并数据----------------------------");
        ParquetDataAdapterImpl parquetDataAdapterImpl = new ParquetDataAdapterImpl();

        parquetDataAdapterImpl.loadData(spark, CommonConfig.getValue(Constants.ENT_INDEX_BASE_DIR)).createOrReplaceTempView("basebikpitemp");
        parquetDataAdapterImpl.loadData(spark, CommonConfig.getValue(Constants.ENT_INDEX_ABNORMITY_DIR)).createOrReplaceTempView("abnormitykpitemp");
        //parquetDataAdapterImpl.loadData(spark, CommonConfig.getValue(Constants.ENT_INDEX_BREAKLAW_DIR)).createOrReplaceTempView("breaklawkpitemp");
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
       // parquetDataAdapterImpl.loadData(spark, CommonConfig.getValue(Constants.ENT_INDEX_PATENT_DIR)).createOrReplaceTempView("patenttmp");
       // parquetDataAdapterImpl.loadData(spark, CommonConfig.getValue(Constants.ENT_INDEX_FINANCIAL_DIR)).createOrReplaceTempView("financial");
        parquetDataAdapterImpl.loadData(spark, CommonConfig.getValue(Constants.ENT_INDEX_LISTED_DIR)).createOrReplaceTempView("listed");


        logger.info("------------载入数据完成----------------------------");

        saveDataToEs(spark, date);
        spark.stop();

    }


    public static void saveDataToEs(SparkSession spark, String date) {
        Map cfg = new HashMap<String, String>();

        cfg.put(DatabaseValues.ES_INDEX_AUTO_CREATE, CommonConfig.getValue(DatabaseValues.ES_INDEX_AUTO_CREATE));
        cfg.put(DatabaseValues.ES_NODES, CommonConfig.getValue(DatabaseValues.ES_NODES));
        cfg.put(DatabaseValues.ES_PORT, CommonConfig.getValue(DatabaseValues.ES_PORT));
        //cfg.put("es.port","58200");
        cfg.put(DatabaseValues.ES_BATCH_SIZE_BYTES, CommonConfig.getValue(DatabaseValues.ES_BATCH_SIZE_BYTES));
        cfg.put(DatabaseValues.ES_BATCH_SIZE_ENTRIES, CommonConfig.getValue(DatabaseValues.ES_BATCH_SIZE_ENTRIES));
        String index = "ent_index_" + date + "/ent_index";
        cfg.put("es.mapping.id", "pripid");
        //cfg.put("es.resource.write", "ent_index_20171217/ENT_INDEX_20171217");
        cfg.put("es.resource.write", index);
        logger.info("------------查询合并数据到es----------------------------");
        ida.writeData(MergeDataETL.getMergeData(spark), cfg);

    }
}
