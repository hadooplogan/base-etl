package com.chinadaas.association.etl.main;

import com.chinadaas.association.etl.common.CommonApp;
import com.chinadaas.association.etl.sparksql.Hdfs2EsETL;
import com.chinadaas.association.etl.table.EntBaseInfoEO;
import com.chinadaas.association.etl.table.EntShortNameEo;
import com.chinadaas.association.etl.table.MonitorIndex;
import com.chinadaas.association.etl.udf.RandomUDF;
import com.chinadaas.common.common.CommonConfig;
import com.chinadaas.common.common.Constants;
import com.chinadaas.common.common.DatabaseValues;
import com.chinadaas.common.register.RegisterTable;
import com.chinadaas.common.udf.StringFormatUDF;
import com.chinadaas.common.util.DataFrameUtil;
import com.chinadaas.common.util.MyFileUtil;
import com.google.common.collect.ImmutableMap;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.elasticsearch.spark.sql.EsSparkSQL;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;
import scala.collection.JavaConversions;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by gongxs01 on 2017/5/15.
 * ****************************************
 *
 *企业照面信息，股东信息，管理人员信息写入ES
 * entbaseinfo
 *企业变更数据写入ES ealterrecoder
 *
 * ****************************************
 */
public class EntInfo2EsApp {


    public static void main(String[] args) {
        //model=inc 增量方式 model=all 全量模式
        SparkConf conf = new SparkConf();
        String date = args[0];
        String model = args[1];
        String allCfgPath = args[2];
        String incPath = args[3];

        if(args!=null && args.length==4){

        }else{
            System.out.println("args erro");
            return;
        }

        conf.set(DatabaseValues.ES_INDEX_AUTO_CREATE, CommonConfig.getValue(DatabaseValues.ES_INDEX_AUTO_CREATE));
        conf.set(DatabaseValues.ES_NODES,CommonConfig.getValue(DatabaseValues.ES_NODES));
        conf.set(DatabaseValues.ES_PORT,CommonConfig.getValue(DatabaseValues.ES_PORT));
        //conf.set(DatabaseValues.ES_PORT,"58200");
        conf.set(DatabaseValues.ES_BATCH_SIZE_BYTES,CommonConfig.getValue(DatabaseValues.ES_BATCH_SIZE_BYTES));
        conf.set(DatabaseValues.ES_BATCH_SIZE_ENTRIES,CommonConfig.getValue(DatabaseValues.ES_BATCH_SIZE_ENTRIES));

        SparkSession spark = SparkSession
                .builder()
                .config(conf)
                .enableHiveSupport()
                .getOrCreate();

        Map<String,String> allcfg = MyFileUtil.getFileCfg(allCfgPath);
        Map<String,String> inccfg = MyFileUtil.getFileCfg(incPath);

        Hdfs2EsETL hdfs = new Hdfs2EsETL();

        StringFormatUDF.stringHandle(spark);
        RandomUDF.genrandom(spark);
        loadEntShortName(spark);

        registerTable(spark,date,model,allcfg,inccfg,hdfs);


        EsSparkSQL.saveToEs(hdfs.getV1Code(spark),"v1_code/V1_CODE", JavaConversions.mapAsScalaMap(ImmutableMap.of("es.mapping.id","zspid")));
        JavaEsSpark.saveToEs(EntBaseInfoEO.convertEntData(spark,hdfs),"entbaseinfo_20180125/ENTBASEINFO", ImmutableMap.of("es.mapping.id", "docid"));

        EsSparkSQL.saveToEs(hdfs.getAlterDataDF(spark),"ealterrecoder_20171219/EALTERRECODER", JavaConversions.mapAsScalaMap(ImmutableMap.of("es.mapping.id", "alter_id")));
        spark.close();
    }

    public static void registerTable(SparkSession spark,String date,String model,Map<String,String> allcfg,Map<String,String> inccfg,Hdfs2EsETL hdfs){

        String entPath = null;
        String invPath = null;
        String personPath = null;

        String gtEnt = null;
        String gtPerson = null;
        String alter = null;
        String v1_code = null;

        if("all".equals(model)){
            entPath =   allcfg.get("ENTERPRISEBASEINFOCOLLECT");
            personPath = allcfg.get("E_PRI_PERSON");
            invPath = allcfg.get("E_INV_INVESTMENT");
            gtEnt = allcfg.get("E_GT_BASEINFO");
            gtPerson = allcfg.get("E_GT_PERSON");
            alter = allcfg.get("E_ALTER_RECODER");
            v1_code = allcfg.get("S_CIF_INDMAP");

        }else{
            entPath =   inccfg.get("ENTERPRISEBASEINFOCOLLECT");
            personPath = inccfg.get("E_PRI_PERSON");
            invPath = inccfg.get("E_INV_INVESTMENT");
            gtEnt = inccfg.get("E_GT_BASEINFO");
            gtPerson = inccfg.get("E_GT_BASEINFO");
            alter = inccfg.get("E_ALTER_RECODER");
            v1_code = allcfg.get("S_CIF_INDMAP");
        }

        RegisterTable.regiserInMapTable(spark,"S_CIF_INDMAP_T",allcfg.get("S_CIF_INDMAP"));
        RegisterTable.regiserEntBaseInfoTable(spark,"enterprisebaseinfocollect",date,entPath);
        RegisterTable.regiserEpriPersonTable(spark,"e_pri_person",date,personPath);
        RegisterTable.regiserInvTable(spark,"e_inv_investment",date,invPath);
        RegisterTable.regiserInMapTable(spark,"S_CIF_INDMAP",allcfg.get("S_CIF_INDMAP"));

        //有的批次不会有增量更新
       if(gtEnt!=null){
        RegisterTable.regiserGtEntBaseInfoTable(spark,"e_gt_baseinfo",date,gtEnt);
        RegisterTable.regiserGtPersonTable(spark,"e_gt_person",date,gtPerson);
        JavaEsSpark.saveToEs(EntBaseInfoEO.convertGtEntData(spark,hdfs),"entbaseinfo_20180125/ENTBASEINFO",ImmutableMap.of("es.mapping.id", "docid"));

       }
       RegisterTable.regiserAlterRecoderTable(spark,"e_alter_recoder",date,alter);
    }

    /**
     * 企业简称
     * @param spark
     */
    private static void  loadEntShortName(SparkSession spark){
        JavaRDD<EntShortNameEo> entShortName =  spark.read().textFile(CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_SHORT_NAME_PATH)).toJavaRDD().
                map(new Function<String, EntShortNameEo>() {
                    @Override
                    public EntShortNameEo call(String line) throws Exception {
                        String[] parts = line.split(",");
                        return new EntShortNameEo(parts[0],parts[1]);
                    }
                });
        spark.createDataFrame(entShortName, EntShortNameEo.class).cache().registerTempTable("shortNameTmp01");
    }

}
