package com.chinadaas.association.etl.main;

import com.chinadaas.association.etl.table.MonitorIndex;
import com.chinadaas.common.common.CommonConfig;
import com.chinadaas.common.common.DatabaseValues;
import com.chinadaas.common.register.RegisterTable;
import com.chinadaas.common.util.MyFileUtil;
import com.chinadaas.common.util.TimeUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;
import org.jets3t.service.utils.RestUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Created by gongxs01 on 2017/12/19.
 */
public class MonitorIndexApp {

    private static long hf_entcount;
    private static long hf_invcount;
    private static long hf_personcount;
    private static long hf_gtentcount;
    private static long hf_gtpersoncount;

    public static void main(String[] args) {
        //model=inc 增量方式 model=all 全量模式
        SparkConf conf = new SparkConf();
        String date = args[0];
        String model = args[1];
        String allCfgPath = args[2];
        String incPath = args[3];

        if (args != null && args.length == 4) {

        } else {
            System.out.println("args erro");
            return;
        }

        SparkSession spark = SparkSession
                .builder()
                .config(conf)
                .enableHiveSupport()
                .getOrCreate();
        Map<String,String> allcfg = MyFileUtil.getFileCfg(allCfgPath);
        Map<String,String> inccfg = MyFileUtil.getFileCfg(incPath);

        registerTable(spark,date,model,allcfg,inccfg);
        saveMonitorIndex(spark,date);

        spark.stop();
    }


    public static void registerTable(SparkSession spark,String date,String model,Map<String,String> allcfg,Map<String,String> inccfg){

        String entpath = null;
        String invpath = null;
        String personpath = null;
        String gtent = null;
        String gtperson = null;
        String alter = null;

        if("all".equals(model)){
            personpath = allcfg.get("E_PRI_PERSON");
            invpath = allcfg.get("E_INV_INVESTMENT");
            gtent = allcfg.get("E_GT_BASEINFO");
            gtperson = allcfg.get("E_GT_PERSON");
            alter = allcfg.get("E_ALTER_RECODER");
            entpath = allcfg.get("ENTERPRISEBASEINFOCOLLECT");
        }else{
            personpath = inccfg.get("E_PRI_PERSON");
            invpath = inccfg.get("E_INV_INVESTMENT");
            gtent = inccfg.get("E_GT_BASEINFO");
            gtperson = inccfg.get("E_GT_BASEINFO");
            alter = inccfg.get("E_ALTER_RECODER");
            entpath =   inccfg.get("ENTERPRISEBASEINFOCOLLECT");
        }


        hf_entcount = RegisterTable.regiserEntBaseInfoTable(spark,"enterprisebaseinfocollect",date,entpath);

        hf_personcount = RegisterTable.regiserEpriPersonTable(spark,"e_pri_person",date,personpath);

        hf_invcount = RegisterTable.regiserInvTable(spark,"e_inv_investment",date,invpath);

        //有的批次不会有增量更新
        if(gtent!=null){
            hf_gtentcount =    RegisterTable.regiserGtEntBaseInfoTable(spark,"e_gt_baseinfo",date,gtent);
            hf_gtpersoncount =   RegisterTable.regiserGtPersonTable(spark,"e_gt_person",date,gtperson);
        }

    }


    private static void saveMonitorIndex(SparkSession spark, final String date){
        Map cfg = new HashMap<String,String>();
        cfg.put(DatabaseValues.ES_NODES, CommonConfig.getValue(DatabaseValues.ES_NODES));
        cfg.put(DatabaseValues.ES_PORT,"58200");
        cfg.put("es.query","?q=data_date:"+date);

        JavaEsSparkSQL.esDF(spark.sqlContext(),"entbaseinfo_20180125/ENTBASEINFO",cfg).registerTempTable("entbaseinfo");


        String hql = "select cast(a.entcount as int) as entcount, " +
                "            cast(a.invcount as int) as invcount, " +
                "            cast(a.personcount as int) as personcount, " +
                "            cast(b.gtcount as int) as gtcount, " +
                "            cast(b.gtperson as int) as  gtperson\n" +
                "  from (select '1' as id,\n" +
                "               count(*) as entcount,\n" +
                "               count(inv.inv) as invcount,\n" +
                "               count(person.name) as personcount\n" +
                "          from entbaseinfo\n" +
                "         where entitytype = '1') a\n" +
                "  left join (select '1' as gtid,\n" +
                "               count(*) as gtcount,\n" +
                "               count(person.name) as gtperson\n" +
                "          from entbaseinfo\n" +
                "         where entitytype = '2') b\n" +
                "    on a.id = b.gtid";

        JavaRDD<Row> dt =  spark.sql(hql).javaRDD();

        List<MonitorIndex> monitor = null;
        if(dt.isEmpty()){
            monitor = new ArrayList<MonitorIndex>();
            monitor.add(new MonitorIndex(hf_entcount,hf_invcount,hf_personcount,
                    0,0,0,0,0,date, TimeUtil.getNowStr()));
        }else{
            monitor = dt.map(new Function<Row, MonitorIndex>() {
                @Override
                public MonitorIndex call(Row row) throws Exception {

                    long es_entcount = row.getInt(0);
                    long es_invcount = row.getInt(1);
                    long es_personcount = row.getInt(2);
                    long es_gtcount= row.getInt(3);;
                    long es_gtpersoncount = row.getInt(4);

                    return new MonitorIndex(hf_entcount,hf_invcount,hf_personcount,
                            es_entcount,es_invcount,es_personcount,es_gtcount,es_gtpersoncount,date, TimeUtil.getNowStr());
                }

            }).collect();

        }

       spark.createDataFrame(monitor,MonitorIndex.class).createOrReplaceTempView("monitor");

        Map cfg2 = new HashMap<String,String>();
        cfg2.put(DatabaseValues.ES_NODES,"192.168.207.11,192.168.207.12,192.168.207.13");
        cfg2.put(DatabaseValues.ES_PORT,"58200");
        cfg2.put("es.resource","monitorindex/MONITORINDEX");

        JavaEsSparkSQL.saveToEs(spark.sql("select * from monitor"),cfg2);
    }



}
