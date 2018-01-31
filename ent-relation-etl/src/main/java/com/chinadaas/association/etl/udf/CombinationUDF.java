package com.chinadaas.association.etl.udf;

import com.chinadaas.common.util.IDUtil;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static com.chinadaas.association.etl.udf.RandomUDF.ALLCHAR;
import static com.chinadaas.association.etl.udf.RandomUDF.LETTERCHAR;

/**
 * Created by gongxs01 on 2017/8/22.
 * 将集合进行两两排列组合
 *
 */
public class CombinationUDF {

    private static String SPLIT=" ";

    /*
     * generate random number
     */
    public static void combination(SparkSession spark) {

        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("dom",DataTypes.StringType,true));
        fields.add(DataTypes.createStructField("fromid",DataTypes.StringType,true));
        fields.add(DataTypes.createStructField("toid",DataTypes.StringType,true));
        spark.udf().register("combination", new UDF2<String, String, List<String[]>>() {
            @Override
            public List<String[]> call(String s, String s2) throws Exception {
                List<String[]> returnValue = new ArrayList<>();

                String[] result = s.split(SPLIT);
                returnValue.add(result);
                return returnValue;
            }
        }, DataTypes.createStructType(fields));
    }

}
