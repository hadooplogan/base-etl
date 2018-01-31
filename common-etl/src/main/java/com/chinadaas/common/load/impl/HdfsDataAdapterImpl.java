package com.chinadaas.common.load.impl;

import com.chinadaas.common.load.IDataAdapter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * Created by gongxs01 on 2017/6/12.
 */
public class HdfsDataAdapterImpl implements IDataAdapter<Dataset,SparkSession,String> {


    public Dataset loadData(SparkSession spark, String path) {
        return spark.read().text(path);
    }

    public void writeData(Dataset ds, String path) {

        ds.write().text(path);
    }
}
