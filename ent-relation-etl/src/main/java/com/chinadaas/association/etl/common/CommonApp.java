package com.chinadaas.association.etl.common;

import com.chinadaas.association.etl.main.FlushConpropApp;
import com.chinadaas.association.etl.sparksql.CommonETL;
import com.chinadaas.common.common.CommonConfig;
import com.chinadaas.common.common.DatabaseValues;
import com.chinadaas.common.register.RegisterTable;
import com.chinadaas.common.util.DataFrameUtil;
import com.chinadaas.common.util.MyFileUtil;
import org.apache.spark.SparkConf;

import org.apache.spark.sql.SparkSession;
import java.io.Serializable;
import java.util.Map;


/**
 *
 * Created by gongxs01 on 2017/7/25.
 *
 */
public class CommonApp implements Serializable {

    private static final String parquetPath = CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_PARQUET_TMP);

    public static String ENT_INFO = "entInfoTmp03";
    public static String ENT_INV_INFO = "e_inv_investment_parquet";
    public static String ENT_PERSON_INFO = "entPersonTmp";

    public static final String ABNORMITY = "abnormity";
    public static final String BREAKLAW = "breaklaw";
    public static final String BZXR = "bzxr";
    public static final String SXBZR = "sxbzr";

    public static void main(String[] args) {
        String date = args[0];
        String model = "";
        //默认为全量模式
        if(args.length>1){
            model = args[1];
        }else{
            model = "all";
        }
        String cfgPath = args[2];


        SparkConf conf = new SparkConf();
        SparkSession spark = SparkSession
                .builder()
                .appName("Chinadaas Common ETL APP")
                .config(conf)
                .enableHiveSupport()
                .getOrCreate();

        Map<String,String> cfg = MyFileUtil.getFileCfg(cfgPath);

        saveDataFrameAsParquet(spark,date,model,cfg);

//        FlushConpropApp.flushConprop(spark,date);

        spark.stop();
    }


    public static void saveDataFrameAsParquet(SparkSession spark,String date,String model,Map<String,String> cfgPath){

        RegisterTable.regiserEntBaseInfoTable(spark,"enterprisebaseinfocollect",date,cfgPath.get("ENTERPRISEBASEINFOCOLLECT"));
        RegisterTable.regiserEpriPersonTable(spark,"e_pri_person",date,cfgPath.get("E_PRI_PERSON"));
        RegisterTable.regiserInvTable(spark,"e_inv_investment",date,cfgPath.get("E_INV_INVESTMENT"));

        DataFrameUtil.saveAsParquetOverwrite(CommonETL.getEntInfo(spark,date), parquetPath + ENT_INFO);
        DataFrameUtil.saveAsParquetOverwrite(CommonETL.getEntPersonInfo(spark,date),parquetPath + ENT_PERSON_INFO);
        DataFrameUtil.saveAsParquetOverwrite(CommonETL.getEntInvInfo(spark,date), parquetPath + ENT_INV_INFO);
    }


    public static void loadAndRegiserTable(SparkSession sqlContext, String[] tableNames){
        for(int i=0;i<tableNames.length;i++){
            System.out.println(parquetPath + tableNames[i]);
            sqlContext.read().parquet(parquetPath + tableNames[i]).registerTempTable(tableNames[i]);
        }
    }


}
