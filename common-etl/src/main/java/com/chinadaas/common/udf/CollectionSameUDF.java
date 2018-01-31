package com.chinadaas.common.udf;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;
import scala.collection.mutable.WrappedArray;


/**
 * Created by gongxs01 on 2017/6/13.
 */
public class CollectionSameUDF {

    public static void collectSame(SparkSession spark) {

        spark.udf().register("collectsame", new UDF2<WrappedArray, WrappedArray,String>() {
            /**
             *
             */
            private static final long serialVersionUID = -6709911270475566751L;
            @Override
            public String call(WrappedArray inv1 ,WrappedArray inv2) {
                return collectSameHandle(inv1,inv2);
            }
        }, DataTypes.StringType);
    }


    /**
     * 比较两个集合是否相等
     */
    public static String collectSameHandle(WrappedArray inv1 ,WrappedArray inv2) {
        if (inv1 == null |inv2==null ) {
            return "";
        }

        if(inv1.size()!=inv2.size()){
            return "0";
        }

       for(int i=0;i<inv1.size();i++){
            if(!inv2.contains(inv1.apply(i))){
                return "0";
            }
       }
        return "1";
    }

}
