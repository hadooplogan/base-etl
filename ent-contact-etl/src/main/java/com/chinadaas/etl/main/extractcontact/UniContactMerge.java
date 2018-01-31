package com.chinadaas.etl.main.extractcontact;

import com.chinadaas.common.common.CommonConfig;
import com.chinadaas.common.common.Constants;
import com.chinadaas.common.common.DatabaseValues;
import com.chinadaas.common.load.impl.EsDataAdapterImpl;
import com.chinadaas.common.load.impl.ParquetDataAdapterImpl;
import com.chinadaas.common.util.LogUtil;
import com.chinadaas.etl.sparksql.UniqueMerge;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;

/**
 * @author haoxing
 * <p>
 * unique数据Merge处理存入hdfs和es。
 **/
public class UniContactMerge {
    protected static LogUtil logger = LogUtil.getLogger(UniContactMerge.class);
    static EsDataAdapterImpl eda = new EsDataAdapterImpl();
    static ParquetDataAdapterImpl parquetDataAdapter = new ParquetDataAdapterImpl();


    public static void main(String[] args) {
        SparkSession spark = SparkSession.
                builder().
                appName("union-conatct-in-one").
                enableHiveSupport().
                getOrCreate();
        logger.info("-----LOADING DATA------");

        //hdfs文件存储日期目录。
        String datadate = args[0];

        logger.info("-----LOADING DATA------");
        parquetDataAdapter.loadData(spark,"/next/uniquecontact/base/"+datadate+"/").createOrReplaceTempView("tmpbase");
        parquetDataAdapter.loadData(spark,"/next/uniquecontact/tel/"+datadate+"/").createOrReplaceTempView("tmptel");
        parquetDataAdapter.loadData(spark,"/next/uniquecontact/email/"+datadate+"/").createOrReplaceTempView("tmpemail");
        parquetDataAdapter.loadData(spark, "/next/uniquecontact/address/"+datadate+"/").createOrReplaceTempView("tmpaddr");
        logger.info("-----LOAD COMPLETE------");

        logger.info("-----write data to hdfs ------");
        parquetDataAdapter.writeData(UniqueMerge.getUniqueContactMerge(spark), "/next/uniquecontact/merged/" + datadate+"/");
        logger.info("-----mission complete ------");

        logger.info("-----write data to es------");
        saveToEs(spark);
        logger.info("------es complete------");
    }


    public static void saveToEs(SparkSession spark) {
        HashMap<String, String> cfg = new HashMap<>();
        cfg.put(DatabaseValues.ES_INDEX_AUTO_CREATE, CommonConfig.getValue(DatabaseValues.ES_INDEX_AUTO_CREATE));
        cfg.put(DatabaseValues.ES_NODES, CommonConfig.getValue(DatabaseValues.ES_NODES));
        cfg.put(DatabaseValues.ES_PORT, CommonConfig.getValue(DatabaseValues.ES_PORT));
        cfg.put(DatabaseValues.ES_BATCH_SIZE_BYTES, CommonConfig.getValue(DatabaseValues.ES_BATCH_SIZE_BYTES));
        cfg.put(DatabaseValues.ES_BATCH_SIZE_ENTRIES, CommonConfig.getValue(DatabaseValues.ES_BATCH_SIZE_ENTRIES));
        cfg.put("es.mapping.id", "pripid");
        cfg.put("es.resource.write", "ent-contact-extract/ENT-CONTACT-EXTRACT");
        logger.info("------------查询合并数据到es---------------");
        eda.writeData(UniqueMerge.getUniqueContactMerge(spark), cfg);
        logger.info("------------数据写入es成功----------");
    }


}
