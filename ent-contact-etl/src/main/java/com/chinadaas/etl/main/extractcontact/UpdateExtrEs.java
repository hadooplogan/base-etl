package com.chinadaas.etl.main.extractcontact;

import com.chinadaas.common.common.CommonConfig;
import com.chinadaas.common.common.DatabaseValues;
import com.chinadaas.common.load.impl.EsDataAdapterImpl;
import com.chinadaas.common.load.impl.ParquetDataAdapterImpl;
import com.chinadaas.common.register.RegisterTable;
import com.chinadaas.common.util.DataFrameUtil;
import com.chinadaas.common.util.LogUtil;
import com.chinadaas.common.util.MyFileUtil;
import com.chinadaas.etl.sparksql.BaseUpdateExtra;
import com.chinadaas.etl.sparksql.UpdateExtraIndex;
import org.apache.spark.sql.SparkSession;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author haoxing
 * 读取的表是历史完成的表的数据，这里读取的历史文件是 单一主键的数据，我们按照日期进行存储。
 * @args1 批次日期
 * @args2 inc配置文件路径
 * @args3 上一批次数据日期
 */
public class UpdateExtrEs {
    static ParquetDataAdapterImpl pda = new ParquetDataAdapterImpl();
    protected static LogUtil logger = LogUtil.getLogger(UpdateExtrEs.class);
    static EsDataAdapterImpl eda = new EsDataAdapterImpl();

    public static void main(String[] args) {
        SparkSession spark = SparkSession.
                builder().
                enableHiveSupport().
                appName("operation-enterprise-update").
                getOrCreate();

        //数据跑批批次日期
        String datadate = args[0];
        //正常读inc
        Map cfg = MyFileUtil.getFileCfg(args[1]);
        //读取需要删除数据的del表
        Map Dcfg = ChangeTableName(args[1]);
        //上次数据跑批日期
        String beforedate = args[2];


        //注册增量数据表
        registerContactTable(spark, datadate, cfg, Dcfg);
        //load单一主键数据
        pda.loadData(spark, "/next/uniquecontact/merged/" + beforedate + "/").registerTempTable("entcontact_extract");

        //增量数据直接存储到es上
        baseinc(spark, datadate);
        saveToEs(spark);


        //需要删除的数据存储到hdfs上
        DataFrameUtil.saveAsTextFileOverwrite(BaseUpdateExtra.ExtraIndexDel(spark), "/next/del/extra");


        //存储新的hdfs文件到hdfs
        DataFrameUtil.saveAsParquetOverwrite(BaseUpdateExtra.ExtraHdfs(spark, datadate), "/next/uniquecontact/merged/" + datadate + "/");

        spark.stop();
    }

    //直接存储到es的增量数据
    private static void baseinc(SparkSession spark, String datadate) {

        BaseUpdateExtra.getEsUpdate(spark, datadate).registerTempTable("baseinc");

    }


    private static void saveToEs(SparkSession spark) {
        HashMap<String, String> cfg = new HashMap<>();
        cfg.put(DatabaseValues.ES_INDEX_AUTO_CREATE, CommonConfig.getValue(DatabaseValues.ES_INDEX_AUTO_CREATE));
        cfg.put(DatabaseValues.ES_NODES, CommonConfig.getValue(DatabaseValues.ES_NODES));
        cfg.put(DatabaseValues.ES_PORT, CommonConfig.getValue(DatabaseValues.ES_PORT));
        cfg.put(DatabaseValues.ES_BATCH_SIZE_BYTES, CommonConfig.getValue(DatabaseValues.ES_BATCH_SIZE_BYTES));
        cfg.put(DatabaseValues.ES_BATCH_SIZE_ENTRIES, CommonConfig.getValue(DatabaseValues.ES_BATCH_SIZE_ENTRIES));
        cfg.put("es.mapping.id", "pripid");
        cfg.put("es.resource.write", "ent-contact-extract/ENT-CONTACT-EXTRACT");
        logger.info("------------查询合并数据到es---------------");
        eda.writeData(UpdateExtraIndex.getUpdateExtraIndex(spark), cfg);
        logger.info("------------数据写入es成功----------");
    }


    //注册表
    public static void registerContactTable(SparkSession spark, String date, Map<String, String> cfg, Map<String, String> Dcfg) {

        RegisterTable.registerBaseInfoTable(spark, "base_inc", date, cfg.get("ENTERPRISEBASEINFOCOLLECT"));
        RegisterTable.registerBaseInfoTable(spark, "base_del", date, Dcfg.get("ENTERPRISEBASEINFOCOLLECT_DEL"));


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

}

