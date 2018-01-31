package com.chinadaas.common.load.impl;

import com.chinadaas.common.load.IDataAdapter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * Created by gongxs01 on 2017/6/12.
 */
public class JsonDataAdapterImp implements IDataAdapter<Dataset,SparkSession,String> {


    public Dataset loadData(SparkSession spark, String params) {
        return spark.read().json(params);
    }

    public void writeData(Dataset ds, String path) {

    }

}
