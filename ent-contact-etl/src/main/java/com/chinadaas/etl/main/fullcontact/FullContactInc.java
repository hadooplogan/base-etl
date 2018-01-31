package com.chinadaas.etl.main.fullcontact;

import com.chinadaas.common.common.CommonConfig;
import com.chinadaas.common.common.DatabaseValues;
import com.chinadaas.common.load.impl.EsDataAdapterImpl;
import com.chinadaas.common.load.impl.ParquetDataAdapterImpl;
import com.chinadaas.common.register.RegisterTable;
import com.chinadaas.common.util.DataFrameUtil;
import com.chinadaas.common.util.LogUtil;
import com.chinadaas.common.util.MyFileUtil;
import com.chinadaas.etl.sparksql.BaseUpdateFull;
import com.chinadaas.etl.sparksql.DeleteES;
import com.chinadaas.etl.sparksql.PersonUpdateFull;
import com.chinadaas.etl.sparksql.UpdateES;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author haoxing
 * 增量模式
 * 增量数据处理没有做落地处理，直接在内存中处理，最后会落地数
 * del文件存储数据到hdfs上
 * @args0 增量文件批次日期
 * @args1 inc配置文件
 * @args2 full配置文件
 * @args3 历史批次日期
 */
public class FullContactInc {

    protected static LogUtil logger = LogUtil.getLogger(FullContactInc.class);
    public static EsDataAdapterImpl eda = new EsDataAdapterImpl();
    public static ParquetDataAdapterImpl pda = new ParquetDataAdapterImpl();

    public static void main(String[] args) {

        SparkSession spark = SparkSession.
                builder().
                appName("deal_inc_data").
                enableHiveSupport().
                getOrCreate();


        //参数0是数据批次日期，参数1是inc文件的path,2是全量表的path。
        String datadate = args[0];
        //正常读inc
        Map Icfg = MyFileUtil.getFileCfg(args[1]);
        //读文件在表名后加_del
        Map Dcfg = ChangeTableName(args[1]);
        //用到全量表的情况
        Map AllCfg = MyFileUtil.getFileCfg(args[2]);
        //上一批次数据的日期,这个date在这里作为我们load历史表文件的日期
        String befordate = args[3];
        /**更新数据base来源和person来源**/

        //注册需要使用的数据表
        registerContactTable(spark, datadate, Icfg, Dcfg, AllCfg);

        //load历史更新前的全量数据
        logger.info("-------载入全量数据------");
        pda.loadData(spark, "/next/contact/person/" + befordate+"/").createOrReplaceTempView("tmpperson");
        pda.loadData(spark, "/next/contact/baseinfo/" + befordate+"/").createOrReplaceTempView("tmpbase");
        logger.info("-------载入成功注册临时表------");


        //注册处理后增量数据的临时表（update与insert）,为es数据更新做准备
        execBaseIncAndChange(spark, datadate);
        execPersonIncAndChange(spark, datadate);

        //数据存入Es
        saveToEs(spark, datadate);

        //数据存储入hdfs
        execBaseHdfs(spark, datadate);
        execPersonHdfs(spark, datadate);

        //删除数据存入hdfs存成text文件
         execPersonDel(spark, datadate);
         execBaseDel(spark,datadate);

        logger.info("del文件开始存储到hdfs");
        DataFrameUtil.saveAsTextFileOverwrite(DeleteES.getDelete(spark), "/next/del/full/");
        logger.info("del文件存储成功");

        spark.stop();
    }

    //照面数据增量更新
    public static void execBaseIncAndChange(SparkSession spark, String datadate) {
        logger.info("------照面数据es更新数据开始处理-----");
        BaseUpdateFull.getBaseIncEs(spark, datadate).registerTempTable("tmpbaseinc");
        logger.info("------照面数据es更新数据处理成功-----");
    }

    //人员数据增量更新
    public static void execPersonIncAndChange(SparkSession spark, String datadate) {
        logger.info("------人员数据es更新数据开始处理-----");
        PersonUpdateFull.getEsPerson(spark, datadate).registerTempTable("tmppersoninc");
        logger.info("------照面数据es更新数据处理成功-----");
    }

    //新增和变更数据和历史数据merge 存储到hdfs
    public static void execBaseHdfs(SparkSession spark, String datadate) {
        logger.info("------开始把更新处理后的照面数据信息存储到hdfs中------");
        DataFrameUtil.saveAsParquetOverwrite(BaseUpdateFull.getBaseDataInc(spark, datadate), "/next/contact/baseinfo/" + datadate+"/");
        logger.info("------hdfs照面数据存储成功------");

    }

    public static void execPersonHdfs(SparkSession spark, String datadate) {

        logger.info("------开始把更新处理后的人员数据信息存储到hdfs中------");
        DataFrameUtil.saveAsParquetOverwrite(PersonUpdateFull.getPersonInc(spark, datadate), "/next/contact/person/" + datadate+"/");
        logger.info("------hdfs人员数据存储成功------");
    }

    //persondel 将照面和人员的需要删除的数据存储成textFile到hdfs上，仅有id
    public static void execPersonDel(SparkSession spark, String datadate) {
        logger.info("------人员待删除数据txt开处理存储到hdfs------");
        PersonUpdateFull.PerosonDel(spark, datadate).registerTempTable("fullpersondel");
        logger.info("------人员待删除数据处理成功------");
    }

    //basedel
    public static void execBaseDel(SparkSession spark,String datadate) {
        logger.info("------照面待删除数据txt开处理存储到hdfs------");
        BaseUpdateFull.FullBaseDel(spark,datadate).registerTempTable("fullbasedel");
        logger.info("------照面待删除数据处理成功------");
    }


    //注册表
    public static void registerContactTable(SparkSession spark, String date, Map<String, String> cfg, Map<String, String> dfg, Map<String, String> allcfg) {
        //_inc后缀的表包含全新数据和变更数据
        RegisterTable.registerBaseInfoTable(spark, "base_inc", date, cfg.get("ENTERPRISEBASEINFOCOLLECT"));
        RegisterTable.registerEpriPersonTable(spark, "person_inc", date, cfg.get("E_PRI_PERSON"));

        //全量base
        RegisterTable.registerBaseInfoTable(spark, "base_full", date, allcfg.get("ENTERPRISEBASEINFOCOLLECT"));

        //_del后缀,需要删除的表，存有要删除数据和变更的历史数据
        RegisterTable.registerBaseInfoTable(spark, "base_del", date, dfg.get("ENTERPRISEBASEINFOCOLLECT_DEL"));
        RegisterTable.registerEpriPersonTable(spark, "person_del", date, dfg.get("E_PRI_PERSON_DEL"));

    }


    //读取配置文件，并更改配置文件的表明的后缀,读取数据变成数据删除表。
    public static Map ChangeTableName(String file) {
        Map<String, String> cfg = new HashMap<String, String>();

        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(file)));
            String s = null;
            while ((s = br.readLine()) != null) {//使用readLine方法，一次读一行
                String s0 = s.substring(0, s.indexOf(","));
                String s1 = s.substring(s.lastIndexOf(",") + 1).replace("\\", "");
                String[] split = s1.split("/");
                StringBuffer sb = new StringBuffer();
                String change = split[7].replace("/", "") + "_del/";
                String append = String.valueOf(sb.append(split[0] + "/" + split[1] + "/" + split[2] + "/" + split[3] + "/" + split[4] + "/" + split[5] + "/" + split[6] + "/" + change + split[8]));
                cfg.put(s0 + "_DEL", append);
            }
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return cfg;
    }

    //存入es，存储的是增量和变更文件，对数据进行插入和覆盖
    public static void saveToEs(SparkSession spark, String date) {
        HashMap<String, String> cfg = new HashMap<>();
        cfg.put(DatabaseValues.ES_INDEX_AUTO_CREATE, CommonConfig.getValue(DatabaseValues.ES_INDEX_AUTO_CREATE));
        cfg.put(DatabaseValues.ES_NODES, CommonConfig.getValue(DatabaseValues.ES_NODES));
        cfg.put(DatabaseValues.ES_PORT, CommonConfig.getValue(DatabaseValues.ES_PORT1));
        cfg.put(DatabaseValues.ES_BATCH_SIZE_BYTES, CommonConfig.getValue(DatabaseValues.ES_BATCH_SIZE_BYTES));
        cfg.put(DatabaseValues.ES_BATCH_SIZE_ENTRIES, CommonConfig.getValue(DatabaseValues.ES_BATCH_SIZE_ENTRIES));
        String index = "ent_contactinfo_full/ENT_CONTACTINFO_FULL";
        cfg.put("es.mapping.id", "id");
        cfg.put("es.resource.write", index);
        logger.info("------------查询合并照面与人员数据到es---------------");
        eda.writeData(UpdateES.MergeToEs(spark), cfg);
        logger.info("------------数据写入es成功----------");
    }

}
