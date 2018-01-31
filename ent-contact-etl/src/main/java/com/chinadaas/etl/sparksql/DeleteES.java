package com.chinadaas.etl.sparksql;

import com.chinadaas.common.util.DataFrameUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * @author haoxing
 * 把person删除数据和base删除数据merge
 */
public class DeleteES {


    public static Dataset getDelete(SparkSession spark){

        String hql = "select * from fullbasedel union all select * from fullpersondel ";

        return DataFrameUtil.getDataFrame(spark,hql,"tmpfulldel");
    }
}
