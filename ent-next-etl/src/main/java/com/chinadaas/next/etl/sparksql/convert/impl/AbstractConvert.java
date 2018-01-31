package com.chinadaas.next.etl.sparksql.convert.impl;

import com.chinadaas.common.common.CommonConfig;
import com.chinadaas.common.common.DatabaseValues;
import com.chinadaas.common.load.IDataAdapter;
import com.chinadaas.common.load.impl.CsvDataAdapterImpl;
import com.chinadaas.common.load.impl.ParquetDataAdapterImpl;
import com.chinadaas.common.register.RegisterTable;
import com.chinadaas.next.etl.sparksql.convert.IConvert;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

/**
 * Created by gongxs01 on 2017/9/12.
 */
public abstract class AbstractConvert implements IConvert {


    final IDataAdapter<Dataset,SparkSession,Map<String,String>>  adapter = new CsvDataAdapterImpl();

    final IDataAdapter<Dataset,SparkSession,String>  adapterParquet = new ParquetDataAdapterImpl();

    final static String DST_PATH = CommonConfig.getValue(DatabaseValues.CHINADAAS_ASSOCIATION_DSTPATH_TMP);


    protected void registerTableTmp(SparkSession spark){

        RegisterTable.loadAndRegiserTable(spark,new String[]{RegisterTable.ENT_INFO,RegisterTable.ENT_INV_INFO,RegisterTable.ENT_PERSON_INFO});
    }

}
