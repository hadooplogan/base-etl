package com.chinadaas.etl.main.extractcontact;

import com.chinadaas.common.load.impl.ParquetDataAdapterImpl;
import com.chinadaas.common.register.RegisterTable;
import com.chinadaas.common.util.DataFrameUtil;
import com.chinadaas.common.util.LogUtil;
import com.chinadaas.common.util.MyFileUtil;
import com.chinadaas.etl.sparksql.*;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

/**
 * @author haoxing
 * 全量数据表中拿到各个数据的时间优先级最新且不为空的数据
 * 1.tel
 * 2.email
 * 3.address
 */
public class InStatusIndex {

    protected static LogUtil logger = LogUtil.getLogger(InStatusIndex.class);
    protected static ParquetDataAdapterImpl pda = new ParquetDataAdapterImpl();

    public static void main(String[] args) {

        SparkSession spark = SparkSession.
                builder().
                appName("Time-order-data").
                enableHiveSupport().
                getOrCreate();
        //参数一  数据批次日期
        String date = args[0];

        //参数二 table.list
        Map cfg = MyFileUtil.getFileCfg(args[1]);

        //注册临时表
        registerContactTable(spark, date, cfg);

        //load历史全量表
        logger.info("载入历史全量数据");
        pda.loadData(spark,"/next/contact/fullcontact/").registerTempTable("ent_contactinfo_full");
        logger.info("完成数据载入");

        //生成各个唯一数据文件
        execBase(spark,date);
        execTel(spark,date);
        execEmail(spark,date);
        execAddress(spark,date);

        spark.stop();
    }

    private static void execBase(SparkSession spark,String datadate){

        DataFrameUtil.saveAsParquetOverwrite(TimeInStatus.getInStatus(spark,datadate),"/next/uniquecontact/base/"+datadate+"/");

    }

    private static void execTel(SparkSession spark,String datadate){

        DataFrameUtil.saveAsParquetOverwrite(TimeTel.getExtractTel(spark),"/next/uniquecontact/tel/"+datadate+"/");
    }

    private static void execEmail(SparkSession spark,String datadate){

        DataFrameUtil.saveAsParquetOverwrite(TimeEmail.getExtracEmail(spark),"/next/uniquecontact/email/"+datadate+"/");

    }

    private static void execAddress(SparkSession spark,String datadate){

        DataFrameUtil.saveAsParquetOverwrite(TImeAddress.getExtracAddress(spark),"/next/uniquecontact/address/"+datadate+"/");

    }


    //注册表
    private static void registerContactTable(SparkSession spark, String date, Map<String, String> cfg) {

        RegisterTable.registerEpriPersonTable(spark, "person_full", date, cfg.get("E_PRI_PERSON"));
        RegisterTable.registerBaseInfoTable(spark, "base_full", date, cfg.get("ENTERPRISEBASEINFOCOLLECT"));


    }
}
