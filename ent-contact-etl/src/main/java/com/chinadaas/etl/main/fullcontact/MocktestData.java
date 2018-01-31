package com.chinadaas.etl.main.fullcontact;

import com.chinadaas.common.load.impl.EsDataAdapterImpl;
import com.chinadaas.common.load.impl.ParquetDataAdapterImpl;
import com.chinadaas.common.util.DataFrameUtil;
import com.chinadaas.etl.sparksql.MockData;
import org.apache.spark.sql.SparkSession;

/**
 * @author haoxing
 * Mock for dongdong
 *
 */
public class MocktestData {


    static EsDataAdapterImpl eda = new EsDataAdapterImpl();
    static ParquetDataAdapterImpl pda = new ParquetDataAdapterImpl();
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("Mock-data").enableHiveSupport().getOrCreate();

        pda.loadData(spark, "/next/uniquecontact/merged/20180112").registerTempTable("entcontact_extract");
        execMOckData(spark);

    }

    public static void execMOckData(SparkSession spark){


        DataFrameUtil.saveAsTextFileOverwrite(MockData.getMockData(spark),"/next/del/zhangdong/");

    }






}
