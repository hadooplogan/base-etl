package com.chinadaas.common.load.impl;

import com.chinadaas.common.load.IDataAdapter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * Created by gongxs01 on 2017/6/17.
 */
public class ParquetDataAdapterImpl implements IDataAdapter<Dataset, SparkSession, String> {


    private static Dataset loadParquet(SparkSession spark, String path) {

        return spark.read().load(path);

    }

    @Override
    public Dataset loadData(SparkSession spark, String params) {

        return loadParquet(spark, params);

    }

    @Override
    public void writeData(Dataset ds, String path) {
        ds.write().mode(SaveMode.Overwrite).parquet(path);
    }

    public void writeData(Dataset ds, String path,SaveMode saveMode) {
        ds.write().mode(saveMode).parquet(path);
    }

}
