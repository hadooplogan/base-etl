package com.chinadaas.association.etl.sparksql;

import com.chinadaas.common.util.DataFrameUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * @author haoxing
 */
public class usedNameInformation {

    public static Dataset getusedName(SparkSession spark) {
        String hql = "select \n" +
                "nodenum,  \n" +
                "pripid,   \n" +
                "stringhandle(usedname) as usedname , \n" +
                "jobid,    \n" +
                "stringhandle(usedname) as usedname_index\n" +
                " from usednametable";

        return DataFrameUtil.getDataFrame(spark, hql, "tmpusedname");
    }
}
