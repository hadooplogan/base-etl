package com.chinadaas.common.udf;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;


/**
 * Created by gongxs01 on 2017/12/8.
 */
public class NullValueDealUDF {

    public static void registerNUllUDF(SparkSession spark) {

        spark.udf().register("emptydeal", new UDF1<String,String>() {
            /**
             *
             */
            private static final long serialVersionUID = -6709911270475566751L;

            public String call(String inv1) {
                return getNullDeal(inv1);
            }
        }, DataTypes.StringType);
    }


    private static String getNullDeal(String plainText) {
        try {
            if("".equals(plainText)||"null".equalsIgnoreCase(plainText)){
                return "";
            }else{

                return plainText;
            }

        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

    }
}
