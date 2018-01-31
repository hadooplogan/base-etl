package com.chinadaas.etl.sparksql;

import com.chinadaas.common.util.DataFrameUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * @author haoxing
 */
public class BaseInfo {

    //拿到在营企业的pripid，企业名称，统一社会信用代码，
    public static Dataset getBaseInfo(SparkSession spark,String datadate){
       String hql = "select pripid,credit_code,entname,"+datadate+" as date from base_full where pripid <> '' and entstatus = '1'";

      return DataFrameUtil.getDataFrame(spark,hql,"tmpbase");

    }
}
