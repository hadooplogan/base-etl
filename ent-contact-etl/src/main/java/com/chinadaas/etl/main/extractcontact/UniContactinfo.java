package com.chinadaas.etl.main.extractcontact;

import com.chinadaas.common.common.CommonConfig;
import com.chinadaas.common.common.Constants;
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
 * 存入unique数据到hdfs
 * 1，处理照面信息拿出企业名称和社会统一信用代码
 * 2，按照优先级开始处理唯一性联系电话
 * 3，按照优先级处理唯一性联系地址
 * 4，按照唯一性处理联系email信息
 * 5，地址长度小于6的处理算法逻辑。
 */
public class UniContactinfo {
    static LogUtil logger = LogUtil.getLogger(UniContactinfo.class);
    static ParquetDataAdapterImpl pda = new ParquetDataAdapterImpl();

    public static void main(String[] args) {
        SparkSession spark = SparkSession.
                builder().
                enableHiveSupport().
                appName("uni-contact-infomation").
                getOrCreate();


        String datadate = args[0];

        Map cfg = MyFileUtil.getFileCfg(args[1]);

        //load历史更新前的全量数据
        logger.info("-------载入全量数据------");
        pda.loadData(spark, "/next/contact/fullcontact/").createOrReplaceTempView("ent_contactinfo_full");
        logger.info("-------载入成功注册临时表------");

        registerContactTable(spark, datadate, cfg);

        execBaseInfo(spark,datadate);
        execContactEmail(spark,datadate);
        execContactAddress(spark,datadate);
        execContactTel(spark,datadate);
    }

    private static void execBaseInfo(SparkSession spark, String datadate) {
        logger.info("------开始处理照面联系信息------");
        DataFrameUtil.saveAsParquetOverwrite(BaseInfo.getBaseInfo(spark, datadate), "/next/uniquecontact/base/"+datadate+"/");
        logger.info("------结束处理照面联系信息------");
    }

    private static void execContactTel(SparkSession spark,String datadate) {
        logger.info("——————————开始处理唯一电话数据——————————");
        DataFrameUtil.saveAsParquetOverwrite(ContactTel.getTel(spark),"/next/uniquecontact/tel/"+datadate+"/");
        logger.info("——————————结束处理唯一电话数据——————————");
    }

    private static void execContactAddress(SparkSession spark,String datadate) {
        logger.info("------开始处理地址信息------");
        DataFrameUtil.saveAsParquetOverwrite(ContactAddress.getAddress(spark), "/next/uniquecontact/address/"+datadate+"/");
        logger.info("------结束处理地址信息------");

    }

    private static void execContactEmail(SparkSession spark,String datadate) {
        logger.info("------开始处理邮箱信息------");
        DataFrameUtil.saveAsParquetOverwrite(ContactEmail.getEmail(spark), "/next/uniquecontact/email/"+datadate+"/");
        logger.info("------结束处理邮箱信息------");
    }


    //注册表
    public static void registerContactTable(SparkSession spark, String date, Map<String, String> cfg) {

        RegisterTable.registerEpriPersonTable(spark, "person_full", date, cfg.get("E_PRI_PERSON"));
        RegisterTable.registerBaseInfoTable(spark, "base_full", date, cfg.get("ENTERPRISEBASEINFOCOLLECT"));


    }


}


