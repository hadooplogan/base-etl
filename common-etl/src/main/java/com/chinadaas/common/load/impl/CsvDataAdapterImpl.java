package com.chinadaas.common.load.impl;

import com.chinadaas.common.load.IDataAdapter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

/**
 * Created by gongxs01 on 2017/6/12.
 */
public class CsvDataAdapterImpl implements IDataAdapter<Dataset,SparkSession,Map<String,String>> {

    @Override
    public Dataset loadData(SparkSession spark, Map<String,String> cfg) {
        return spark.read().options(cfg).format("com.databricks.spark.csv").load();
    }

    @Override
    public void writeData(Dataset ds, Map<String,String> path) {

    }

    public void reginsterCsvTable(SparkSession spark,String path){
        spark.read().format("com.databricks.spark.csv").load(path);
    }

}
