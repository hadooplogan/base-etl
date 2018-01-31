package com.chinadaas.common.etl.sql;

import com.chinadaas.common.util.DataFrameUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * Created by gongxs01 on 2017/10/20.
 */
public class RegisterTableETL {


    //增量数据
    public static Dataset getRegiserTable(SparkSession spark, String tableName){
            String  hql = "select * from "+tableName+"_tmp";
        return DataFrameUtil.getDataFrame(spark, hql,tableName);
    }

    //全量数据
    public static Dataset getRegiserFullTable(SparkSession spark, String tableName,String srcTable,String date){

        String  hql = "select *,CAST ("+date+" as string)  as data_date  from "+srcTable+"";
        return DataFrameUtil.getDataFrame(spark, hql,tableName);
    }
}
