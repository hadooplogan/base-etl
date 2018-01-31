package com.chinadaas.refresh.main;

import com.chinadaas.refresh.convert.IConvert;
import com.chinadaas.refresh.convert.impl.RefreshMasterConverImpl;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

/**
 * Created by gongxs01 on 2017/11/8.
 */
public class RefreshMasterApp {



    final static IConvert iConvert = new RefreshMasterConverImpl();


    public static void main(String[] args) {
        //model=inc 增量方式 model=all 全量模式


        String date = args[0];
        String model = args[1];
        String allCfgPath = args[2];
        String incPath = args[3];


        if(args!=null && args.length==4){

        }else{
            System.out.println("args erro");
            return;
        }


        SparkConf conf = new SparkConf().setAppName("RefresheMaster App");
        SparkSession spark = SparkSession
                .builder()
                .appName("ReFreshMasterApp")
                .config(conf)
                .enableHiveSupport()
                .getOrCreate();
        iConvert.convertData(spark,date,model,allCfgPath,incPath);

        spark.stop();
    }

}
