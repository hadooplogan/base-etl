package com.chinadaas.etl.main.fullcontact;

import com.chinadaas.common.common.CommonConfig;
import com.chinadaas.common.common.Constants;
import com.chinadaas.common.register.RegisterTable;
import com.chinadaas.common.util.DataFrameUtil;
import com.chinadaas.common.util.LogUtil;
import com.chinadaas.common.util.MyFileUtil;
import com.chinadaas.etl.sparksql.*;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

/**
 * @author haoxing
 * <p>
 * 全量数据的union
 * @args0 批次日期
 * @args1 全量文件配置文件路径
 *
 *
 *
 * 输出：四个部分的数据模块经过清洗和处理
 * 存储在hdfs上，存在于历史批次上的数据。
 */
public class ContactInfoAPP {
    protected static LogUtil logger = LogUtil.getLogger(ContactInfoAPP.class);

    public static void main(String[] args) {
        SparkSession spark = SparkSession.
                builder().
                appName("Deal-ContactInfo-APP").
                enableHiveSupport().
                getOrCreate();
        //批次日期
        String date = args[0];
        //全量配置文件路径
        String AllCfgPath = args[1];

        Map<String, String> allcfg = MyFileUtil.getFileCfg(AllCfgPath);
        //注册临时表
        registerTable(spark, date, allcfg);

        execAnnulContactETL(spark, date);
        execBaseinfoContactETL(spark, date);
        execPersonContactETL(spark, date);
        execTaxContactETL(spark, date);

        spark.stop();
    }

    private static void execAnnulContactETL(SparkSession spark, String datadate) {
        logger.info("-------开始处理年报联系信息------");
        DataFrameUtil.saveAsParquetOverwrite(AnnualContactETL.getAnnualContact(spark, datadate), "/next/contact/annual/"+datadate+"/");
        logger.info("-------结束处理年报联系信息------");
    }

    private static void execPersonContactETL(SparkSession spark, String datadate) {
        logger.info("------开始处理高管联系信息-------");
        DataFrameUtil.saveAsParquetOverwrite(PersonInfoContactETL.getPersonInfoContactETL(spark, datadate),"/next/contact/person/"+datadate+"/");
        logger.info("------结束处理高管联系信息-------");
    }

    private static void execBaseinfoContactETL(SparkSession spark, String datadate) {
        logger.info("------开始处理照面信息联系信息------");
        DataFrameUtil.saveAsParquetOverwrite(BaseinfoContactETL.getBaseinfoContact(spark, datadate), "/next/contact/baseinfo/"+datadate+"/");
        logger.info("------结束处理照面信息联系信息------");
    }

    private static void execTaxContactETL(SparkSession spark, String datadate) {
        logger.info("------开始处理税务登记信息------");
        DataFrameUtil.saveAsParquetOverwrite(TaxContactETL.getTaxContactETL(spark, datadate), "/next/contact/tax/"+datadate+"/");
        logger.info("------结束处理税务登记信息------");
    }


    public static void registerTable(SparkSession spark, String date, Map<String, String> allcfg) {

        String entPath = null;
        String personPath = null;
        String annreportPath = null;

        entPath = allcfg.get("ENTERPRISEBASEINFOCOLLECT");
        personPath = allcfg.get("E_PRI_PERSON");
        annreportPath = allcfg.get("E_ANNREPORT_BASEINFO");

        //注册临时表
        RegisterTable.registerBaseInfoTable(spark, "enterprisebaseinfocollect", date, entPath);
        RegisterTable.registerEpriPersonTable(spark, "e_pri_person", date, personPath);

        //缺失表，2016年报，2015年报，税务登记
        RegisterTable.registerAnnreportTable(spark, "annreportbaseinfo", annreportPath);
        //税务登记表不需要做解析，直接读表。sqoop已经抽取了数据，所以没有问题。

    }

}


