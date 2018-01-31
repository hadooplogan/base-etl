package com.chinadaas.association.etl.main;

import com.chinadaas.association.etl.table.EntInvRelation;
import com.chinadaas.association.etl.table.Hold;
import com.chinadaas.association.etl.table.LeafNodeRelation;
import com.chinadaas.common.util.DataFrameUtil;
import org.apache.avro.ipc.DatagramServer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

/**
 * Created by gongxs01 on 2018/1/5.
 *
 * 反洗钱主程序
 *
 */
public class AMLApp {


    public static void main(String args[]) {

        SparkConf conf = new SparkConf();

        conf.setAppName("Chinadaas AMLApp ETL APP");

        SparkSession spark = SparkSession
                .builder()
                .config(conf)
                .enableHiveSupport()
                .getOrCreate();

        //personinvmerge  entinvmerge orginvmerge


        //10
        final JavaRDD<LeafNodeRelation> leafNode =  spark.read().textFile("/relation/dstpath/relation_data20171212/orginvmerge").toJavaRDD().filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String line) throws Exception {
                String [] bzxr = line.split("\\|",-1);
                if(bzxr[0].equals(bzxr[2])){
                    return false;
                }
                return true;
            }
        }).map(new Function<String, LeafNodeRelation>() {
            @Override
            public LeafNodeRelation call(String line) throws Exception {
                String [] bzxr = line.split("\\|",-1);
                String conprop = "";

                if(bzxr[1]==null || "".equals(bzxr[1])){
                    conprop=bzxr[9];
                }else{
                    conprop=bzxr[1];
                }
                return new LeafNodeRelation(bzxr[0],bzxr[2],conprop);
            }
        });
        spark.createDataFrame(leafNode,LeafNodeRelation.class).registerTempTable("leaf");

        final JavaRDD<EntInvRelation> entInv =  spark.read().textFile("/relation/dstpath/relation_data20171212/entinvmerge").toJavaRDD().filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String line) throws Exception {
                String [] bzxr = line.split("\\|",-1);
                if(bzxr[0].equals(bzxr[2])){
                    return false;
                }
                return true;
            }
        }).map(new Function<String, EntInvRelation>() {
            @Override
            public EntInvRelation call(String line) throws Exception {
                String [] bzxr = line.split("\\|",-1);
                String conprop = "";

                if(bzxr[1]==null || "".equals(bzxr[1])){
                    conprop=bzxr[9];
                }else{
                    conprop=bzxr[1];
                }
                return new EntInvRelation(bzxr[2],conprop,bzxr[0]);
            }
        });

        spark.createDataFrame(entInv,EntInvRelation.class).registerTempTable("entInv");


        spark.sql("select * from entInv a where not exists(select 1 from entInv b where a.pripid=b.invid and a.invid=b.pripid)").registerTempTable("entInv2");


        DataFrameUtil.getDataFrame(spark,"select * from entInv","aml_entinv",DataFrameUtil.CACHETABLE_PARQUET);

        DataFrameUtil.getDataFrame(spark,"select * from leaf","aml_leaf",DataFrameUtil.CACHETABLE_PARQUET);


        String hql = "";


        spark.close();

    }





}
