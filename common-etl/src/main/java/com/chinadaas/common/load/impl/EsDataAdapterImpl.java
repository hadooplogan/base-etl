package com.chinadaas.common.load.impl;

import com.chinadaas.common.load.IDataAdapter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;


import java.util.Map;

/**
 * Created by gongxs01 on 2017/6/17.
 */
public class EsDataAdapterImpl implements IDataAdapter<Dataset,SparkSession,Map<String,String>> {

    @Override
    public Dataset loadData(SparkSession spark, Map<String, String> params) {

        return JavaEsSparkSQL.esDF(spark,params);
    }

    @Override
    public void writeData(Dataset ds, Map<String,String> path) {
        JavaEsSparkSQL.saveToEs(ds,path);
    }


}

