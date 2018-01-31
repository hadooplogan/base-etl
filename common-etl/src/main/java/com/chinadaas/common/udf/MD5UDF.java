package com.chinadaas.common.udf;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Created by gongxs01 on 2017/10/31.
 */
public class MD5UDF {

    public static void getMD5(SparkSession spark) {

        spark.udf().register("MD5", new UDF1<String,String>() {
            /**
             *
             */
            private static final long serialVersionUID = -6709911270475566751L;

            public String call(String inv1) {
                return getMd5(inv1);
            }
        }, DataTypes.StringType);
    }


    private static String getMd5(String plainText) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(plainText.getBytes());
            byte b[] = md.digest();

            int i;

            StringBuffer buf = new StringBuffer("");
            for (int offset = 0; offset < b.length; offset++) {
                i = b[offset];
                if (i < 0)
                    i += 256;
                if (i < 16)
                    buf.append("0");
                buf.append(Integer.toHexString(i));
            }
            // 32位加密
            return buf.toString();
            // 16位的加密
            // return buf.toString().substring(8, 24);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            return null;
        }

    }
}
