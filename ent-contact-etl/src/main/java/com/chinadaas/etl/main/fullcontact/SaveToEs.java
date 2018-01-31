package com.chinadaas.etl.main.fullcontact;

import com.chinadaas.common.common.CommonConfig;
import com.chinadaas.common.common.DatabaseValues;
import com.chinadaas.common.load.impl.EsDataAdapterImpl;
import com.chinadaas.common.load.impl.ParquetDataAdapterImpl;
import com.chinadaas.common.util.LogUtil;
import com.chinadaas.etl.sparksql.FullContactToEs;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;

public class SaveToEs {

    protected static LogUtil logger = LogUtil.getLogger(SaveToEs.class);
    static EsDataAdapterImpl eda = new EsDataAdapterImpl();
    static ParquetDataAdapterImpl parquetDataAdapter = new ParquetDataAdapterImpl();
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("save_full_to_es").enableHiveSupport().getOrCreate();
        logger.info("-------载入全量数据------");
        parquetDataAdapter.loadData(spark, "/next/contact/fullcontact/").createOrReplaceTempView("fullcontactinfo");
        logger.info("-------载入成功注册临时表------");


        logger.info("------开始将数据写入es-----");
        saveToEs(spark);
        logger.info("------数据写入es成功------");



    }


    public static void saveToEs(SparkSession spark) {
        HashMap<String, String> cfg = new HashMap<>();

        cfg.put(DatabaseValues.ES_INDEX_AUTO_CREATE, CommonConfig.getValue(DatabaseValues.ES_INDEX_AUTO_CREATE));
        cfg.put(DatabaseValues.ES_NODES, CommonConfig.getValue(DatabaseValues.ES_NODES));
        cfg.put(DatabaseValues.ES_PORT, CommonConfig.getValue(DatabaseValues.ES_PORT));
        cfg.put(DatabaseValues.ES_BATCH_SIZE_BYTES, CommonConfig.getValue(DatabaseValues.ES_BATCH_SIZE_BYTES));
        cfg.put(DatabaseValues.ES_BATCH_SIZE_ENTRIES, CommonConfig.getValue(DatabaseValues.ES_BATCH_SIZE_ENTRIES));
        String index = "ent_contactinfo_full/ENT_CONTACTINFO_FULL" ;
        cfg.put("es.mapping.id", "id");
        cfg.put("es.resource.write", index);

        logger.info("------------查询合并数据到es---------------");
        eda.writeData(FullContactToEs.getFullContactToEs(spark), cfg);
        logger.info("------------数据写入es成功----------");
    }
}
