package com.chinadaas.etl.main.fullcontact;

import com.chinadaas.common.common.CommonConfig;
import com.chinadaas.common.common.Constants;
import com.chinadaas.common.common.DatabaseValues;
import com.chinadaas.common.load.impl.EsDataAdapterImpl;
import com.chinadaas.common.load.impl.ParquetDataAdapterImpl;
import com.chinadaas.common.register.RegisterTable;
import com.chinadaas.common.util.DataFrameUtil;
import com.chinadaas.common.util.LogUtil;
import com.chinadaas.common.util.MyFileUtil;
import com.chinadaas.etl.sparksql.ContactInfoMerge;
import com.chinadaas.etl.sparksql.FullContactToEs;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

/**
 * @author haoxing
 * <p>
 * 将全量数据存储到hdfs。
 * 全量数据在跑批理论上我们是只跑一次，后续都inc更新数据。
 *
 * @参数一 跑批日期
 * @参数二 全量文件路径
 */

public class SaveContactAsParquet {
    protected static LogUtil logger = LogUtil.getLogger(SaveContactAsParquet.class);
    static EsDataAdapterImpl eda = new EsDataAdapterImpl();
    static ParquetDataAdapterImpl parquetDataAdapter = new ParquetDataAdapterImpl();


    public static void main(String[] args) {
        SparkSession spark = SparkSession.
                builder().
                appName("save-contact-to-hdfs").
                enableHiveSupport().
                getOrCreate();
        //这个date与在跑出数据的date一样，是数据批次
        String datadate = args[0];
        String AllCfgPath = args[1];
        Map<String, String> allcfg = MyFileUtil.getFileCfg(AllCfgPath);


        registerTable(spark, datadate, allcfg);

        logger.info("------数据开始加载入------");
        parquetDataAdapter.loadData(spark,"/next/contact/annual/"+datadate+"/").createOrReplaceTempView("annulcontact");
        parquetDataAdapter.loadData(spark,"/next/contact/baseinfo/"+datadate+"/").createOrReplaceTempView("baseinfocontact");
        parquetDataAdapter.loadData(spark,"/next/contact/person/"+datadate+"/").createOrReplaceTempView("personinfocontact");
        parquetDataAdapter.loadData(spark,"/next/contact/tax/"+datadate+"/").createOrReplaceTempView("taxcontact");
        logger.info("------数据加载结束------");


        //先把全量数据存储到hdfs上保存一个全量数据
        logger.info("------将数据存入hdfs中------");
        saveasparquet(spark);
        logger.info("------数据写入成功------");



    }

    public static void saveasparquet(SparkSession spark) {

        parquetDataAdapter.writeData(ContactInfoMerge.getContactinfo(spark), "/next/contact/fullcontact/");
                                                   

    }

    //注册需要用到的全量表
    public static void registerTable(SparkSession spark, String date, Map<String, String> allcfg) {


        String entPath = null;


        entPath = allcfg.get("ENTERPRISEBASEINFOCOLLECT");


        //注册临时表
        RegisterTable.registerBaseInfoTable(spark, "enterprisebaseinfocollect", date, entPath);


    }

}




