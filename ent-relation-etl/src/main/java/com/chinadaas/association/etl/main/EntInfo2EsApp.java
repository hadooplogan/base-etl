package com.chinadaas.association.etl.main;

import com.chinadaas.association.etl.sparksql.Hdfs2EsETL;
import com.chinadaas.association.etl.table.EntBaseInfoEO;
import com.chinadaas.association.etl.table.EntShortNameEo;
import com.chinadaas.association.etl.udf.RandomUDF;
import com.chinadaas.common.common.CommonConfig;
import com.chinadaas.common.common.Constants;
import com.chinadaas.common.common.DatabaseValues;
import com.chinadaas.common.udf.StringFormatUDF;
import com.chinadaas.common.util.DataFrameUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.hive.HiveContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.elasticsearch.spark.sql.EsSparkSQL;

import java.util.List;

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
    /*    if(!DataFrameUtil.checkFlag(Constants.ASSOCIATION_FLAG_PATh)){
            return ;
        }
        DataFrameUtil.deleteFlag(Constants.ASSOCIATION_FLAG_PATh);
*/
        String date = args[0];
        SparkConf conf = new SparkConf().setAppName("Chinadaas Hdfs2Es ETL APP");
        String parquetPath = CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_PARQUET_TMP);
        conf.set(DatabaseValues.ES_INDEX_AUTO_CREATE, CommonConfig.getValue(DatabaseValues.ES_INDEX_AUTO_CREATE));
        conf.set(DatabaseValues.ES_NODES,CommonConfig.getValue(DatabaseValues.ES_NODES));
        conf.set(DatabaseValues.ES_PORT,CommonConfig.getValue(DatabaseValues.ES_PORT));
        conf.set(DatabaseValues.ES_BATCH_SIZE_BYTES,CommonConfig.getValue(DatabaseValues.ES_BATCH_SIZE_BYTES));
        conf.set(DatabaseValues.ES_BATCH_SIZE_ENTRIES,CommonConfig.getValue(DatabaseValues.ES_BATCH_SIZE_ENTRIES));
        SparkContext sc = new SparkContext(conf);
        HiveContext sqlContext = new HiveContext(sc);
        StringFormatUDF.stringHandle(sc,sqlContext);
        RandomUDF.genrandom(sc,sqlContext);
        loadEntShortName(sc,sqlContext);
        Hdfs2EsETL hdfs = new Hdfs2EsETL();
        hdfs.setDate(date);

       JavaEsSpark.saveToEs(EntBaseInfoEO.convertEntData(sqlContext,hdfs),"entbaseinfo/ENTBASEINFO");
       JavaEsSpark.saveToEs(EntBaseInfoEO.convertGtEntData(sqlContext,hdfs),"entbaseinfo/ENTBASEINFO");
       EsSparkSQL.saveToEs(hdfs.getAlterDataDF(sqlContext),"ealterrecoder/EALTERRECODER");

//        DataFrameUtil.saveAsFlag(Constants.ASSOCIATION_FLAG_PATh);
        sqlContext.clearCache();
        sc.stop();
    }

    private static void  loadEntShortName(SparkContext sc,HiveContext sqlContext){
        JavaRDD<EntShortNameEo> entShortName =  sc.textFile("/tmp/ent-relation/ent_shortname/shortname.csv",10).toJavaRDD().
                map(new Function<String, EntShortNameEo>() {
                    @Override
                    public EntShortNameEo call(String line) throws Exception {
                        String[] parts = line.split(",");
                        return new EntShortNameEo(parts[0],parts[1]);
                    }
                });
        sqlContext.createDataFrame(entShortName, EntShortNameEo.class).cache().registerTempTable("shortNameTmp01");
    }

}
