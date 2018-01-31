package com.chinadaas.common.load.impl;

import com.chinadaas.common.load.IDataAdapter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

/**
 * Created by gongxs01 on 2017/6/17.
 */
public class OracleDataAdapterImpl implements IDataAdapter<Dataset,SparkSession,Map<String,String>> {

    public Dataset loadData(SparkSession spark, Map<String, String> params) {
        return loadOracleData(spark,params);
    }

    public void writeData(Dataset ds, Map<String, String>  path) {

    }

    private Dataset loadOracleData(SparkSession spark,Map<String,String> params){

        return spark.read().format("jdbc").options(params).load();
    }
}
