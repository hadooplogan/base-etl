package com.chinadaas.association.etl.main;

import com.chinadaas.association.etl.sparksql.Hdfs2EsETL;
import com.chinadaas.association.etl.sparksql.Hdfs2EsETL4ShangHai;
import com.chinadaas.association.etl.sparksql.ZuZhiJiGou;
import com.chinadaas.association.etl.table.EntBaseInfo4ShangHai;
import com.chinadaas.association.etl.table.EntBaseInfo4ShangHai4ICP;
import com.chinadaas.association.etl.table.EntBaseInfo4ShangHai4UsedName;
import com.chinadaas.association.etl.table.EntBaseInfoEO;
import com.chinadaas.association.etl.udf.RandomUDF;
import com.chinadaas.common.common.CommonConfig;
import com.chinadaas.common.common.DatabaseValues;
import com.chinadaas.common.load.impl.EsDataAdapterImpl;
import com.chinadaas.common.register.RegisterTable;
import com.chinadaas.common.udf.StringFormatUDF;
import com.chinadaas.common.util.DataFrameUtil;
import com.chinadaas.common.util.LogUtil;
import com.chinadaas.common.util.MyFileUtil;
import com.sun.prism.shader.Solid_TextureYV12_AlphaTest_Loader;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.elasticsearch.spark.sql.EsSparkSQL;
import org.spark_project.guava.collect.ImmutableMap;
import scala.collection.JavaConversions;

import java.io.File;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by hejianning on 2017/9/20.
 * 上海电信
 * 企业基本信息写入 ES
 * application 入口
 */
public class EntInfo2Es4ShangHaiTelecomApp {

    protected static LogUtil logger = LogUtil.getLogger(EntInfo2Es4ShangHaiTelecomApp.class);

    static EsDataAdapterImpl eda = new EsDataAdapterImpl();
    public static void main(String[] args) {

        //model=inc 增量方式 model=all 全量模式

        SparkConf conf = new SparkConf();
        conf.setAppName("EntInfo2Es4ShangHaiTelecomApp");
        conf.set(DatabaseValues.ES_INDEX_AUTO_CREATE, "false");
        conf.set(DatabaseValues.ES_NODES,"192.168.207.11,192.168.207.12,192.168.207.13");
        conf.set(DatabaseValues.ES_PORT,"59200");
            conf.set(DatabaseValues.ES_BATCH_SIZE_BYTES,"6m");
        conf.set(DatabaseValues.ES_BATCH_SIZE_ENTRIES,"50000");

        SparkSession spark = SparkSession
                .builder()
                .config(conf)
                .enableHiveSupport()
                .getOrCreate();

        String date = args[0];
       // String cfgPath = args[1];



       // Map<String,String> cfg = MyFileUtil.getFileCfg(cfgPath);
       // registerTable(spark,cfg,date);

        /**
         * 上海电信基础数据
         */
   //     getEntBaseShangHai(spark);
        /**
         * icp备案数据
         */
//        geticp(spark);
        /**
         * 企业曾用名信息
         */
        //曾用名path
     //   String usedname = cfg.get("S_EN_USEDNAME");
     //   getUsedName(spark,usedname,date);

        /**
         * 组织机构信息
         */
        saveToEs(spark,date);

        spark.close();
    }

    public static void getEntBaseShangHai(SparkSession spark){
        StringFormatUDF.stringHandle(spark);
        RandomUDF.genrandom(spark);
        Hdfs2EsETL4ShangHai hdfs = new Hdfs2EsETL4ShangHai();

//        System.out.println(EntBaseInfo4ShangHai.convertEntData(spark, hdfs).first().toString());


//        spark.createDataFrame(EntBaseInfo4ShangHai.convertEntData(spark, hdfs),EntBaseInfo4ShangHai.class).write().parquet("/tmp/spark_test/gxs");

//        EsSparkSQL.saveToEs(spark.read().load("/tmp/spark_test/gxs"),"shanghaientbaseinfo_index/SHANGHAIENTBASEINFO", JavaConversions.mapAsScalaMap(com.google.common.collect.ImmutableMap.of("es.mapping.id", "pripid")));

        JavaEsSpark.saveToEs(EntBaseInfo4ShangHai.convertEntData(spark, hdfs),"shanghaientbaseinfo_v2/SHANGHAIENTBASEINFO", ImmutableMap.of("es.mapping.id", "pripid"));


    }

    public static void getUsedName(SparkSession spark,String path,String datadate){

        JavaRDD<String> rdd = spark.sparkContext().textFile(path, 100).toJavaRDD();
        JavaRDD<String> flatMap = rdd.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                ArrayList<String> list = new ArrayList<>();
                String[] split = s.split("\u0001");
                if (split.length >= 4) {
                        list.add(s);
                }
                return list.iterator();
            }
        });
        JavaRDD<EntBaseInfo4ShangHai4UsedName> map = flatMap.map(new Function<String, EntBaseInfo4ShangHai4UsedName>() {
            @Override
            public EntBaseInfo4ShangHai4UsedName call(String v1) throws Exception {
                String[] split = v1.split("\u0001");
                String DATA_DATE = split[0];
                String NODENUM = split[1];
                String PRIPID = "";
                if (!split[2].equals("") && split[2] != null) {
                    PRIPID = split[2];
                }
                String USEDNAME = split[3];
                String JOBID = "";
                if (split.length == 5) {
                    JOBID = split[4];
                }
                EntBaseInfo4ShangHai4UsedName entBaseInfo4ShangHai4UsedName = new EntBaseInfo4ShangHai4UsedName(
                        PRIPID, DATA_DATE, NODENUM,  USEDNAME, JOBID
                );
                return entBaseInfo4ShangHai4UsedName;
            }
        });
        JavaEsSpark.saveToEs(map,"ent_usedname_"+datadate+"/ENT_USEDNAME");
        ImmutableMap.of("es.mapping.id", "pripid");
    }

    public static void geticp(SparkSession spark){
        JavaRDD<String> rdd = spark.sparkContext().textFile("/tmp/spark_test/c_gs_icp_website.txt", 1000).toJavaRDD();

        JavaRDD<EntBaseInfo4ShangHai4ICP> map = rdd.map(new Function<String, EntBaseInfo4ShangHai4ICP>() {
            @Override
            public EntBaseInfo4ShangHai4ICP call(String v1) throws Exception {
                String[] split = v1.split("#");
                String fba_zt_xkz = split[0];
                String fba_zt_dwmc = split[1];
                String fba_zt_tgsj = split[2];
                String fba_wz_dwxz = split[3];
                String fba_wz_xkz = split[4];
                String fba_wz_fzr = split[5];
                String fba_wz_mc = split[6];
                String fba_wz_spx = split[7];
                String fba_wz_wz = split[8];
                String fba_wz_ym = split[9];
                String fba_url = split[10];
                String fba_idt = split[11];
                String fba_udt = split[12];
                String fba_status = split[13];
                String fba_id = split[14];
                String fba_zt_dwmc_gllzd = "";
                if(split.length == 16){
                    fba_zt_dwmc_gllzd = split[15];
                }

                EntBaseInfo4ShangHai4ICP entBaseInfo4ShangHai4ICP = new EntBaseInfo4ShangHai4ICP(
                        fba_zt_xkz,fba_zt_dwmc,fba_zt_tgsj,fba_wz_dwxz,
                        fba_wz_xkz,fba_wz_fzr,fba_wz_mc,fba_wz_spx,
                        fba_wz_wz,fba_wz_ym,fba_url,fba_idt,
                        fba_udt,fba_status,fba_id,fba_zt_dwmc_gllzd
                );
                return entBaseInfo4ShangHai4ICP;
            }
        });
        JavaEsSpark.saveToEs(map,"entbaseshanghaiicp/ENTBASESHANGHAIICP", ImmutableMap.of("es.mapping.id", "fba_id"));
    }

    public static void registerTable(SparkSession spark,Map<String,String> allcfg,String date){

        String entPath = null;
        String invPath = null;
        String personPath = null;

        String gtEnt = null;
        String gtPerson = null;
        String alter = null;

            entPath =   allcfg.get("ENTERPRISEBASEINFOCOLLECT");
            personPath = allcfg.get("E_PRI_PERSON");
            invPath = allcfg.get("E_INV_INVESTMENT");
            gtEnt = allcfg.get("E_GT_BASEINFO");
            gtPerson = allcfg.get("E_GT_PERSON");
            alter = allcfg.get("E_ALTER_RECODER");

        System.out.println("e_gt_baseinfo"+gtEnt);
//        RegisterTable.regiserGtEntBaseInfoTable(spark,"e_gt_baseinfo",date,gtEnt);
//        RegisterTable.regiserEntBaseInfoTable(spark,"enterprisebaseinfocollect",date,entPath);
//        RegisterTable.regiserInvTable(spark,"e_inv_investment",date,invPath);

        spark.read().load("/relation/cachetable/e_gt_baseinfo").registerTempTable("e_gt_baseinfo");
        spark.read().load("/relation/cachetable/enterprisebaseinfocollect").registerTempTable("enterprisebaseinfocollect");
    }

    public static void saveToEs(SparkSession spark,String date) {
        HashMap<String, String> cfg = new HashMap<>();
        cfg.put(DatabaseValues.ES_INDEX_AUTO_CREATE, CommonConfig.getValue(DatabaseValues.ES_INDEX_AUTO_CREATE));
        cfg.put(DatabaseValues.ES_NODES, CommonConfig.getValue(DatabaseValues.ES_NODES));
        cfg.put(DatabaseValues.ES_PORT, CommonConfig.getValue(DatabaseValues.ES_PORT));
        cfg.put(DatabaseValues.ES_BATCH_SIZE_BYTES, CommonConfig.getValue(DatabaseValues.ES_BATCH_SIZE_BYTES));
        cfg.put(DatabaseValues.ES_BATCH_SIZE_ENTRIES, CommonConfig.getValue(DatabaseValues.ES_BATCH_SIZE_ENTRIES));
       // cfg.put("es.mapping.id", "pripid");
        cfg.put("es.resource.write", "ogzinfo-"+date+"/ogzinfo");
        logger.info("------------查询合并数据到es---------------");
        eda.writeData(ZuZhiJiGou.InformationGainStats(spark), cfg);
        logger.info("------------数据写入es成功----------");
    }


}
