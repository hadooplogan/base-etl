package com.chinadaas.common.udf;

import com.chinadaas.common.util.IDUtil;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;

/**
 * Created by gongxs01 on 2017/6/2.
 */
public class StringFormatUDF {
    public static void stringHandle(SparkContext sc, HiveContext sqlContext) {
        sqlContext.udf().register("stringhandle", new UDF1<String, String>() {
            /**
             *
             */
            private static final long serialVersionUID = -6709911270475566751L;

            public String call(String str) {
                return formatEsString(str);
            }
        }, DataTypes.StringType);
    }


    /**
     * 特殊字符替换 单个企业
     */
    public static String formatEsString(String str) {
        if (str == null) {
            return "";
        }
        return format2en(format2DBC(str.trim().toUpperCase()));
    }
    /**
     * 中文符号转英文
     *
     * @param str
     * @return
     */
    public static String format2en(String str) {
        return str.replace("（", "(").replace("）", ")") // 替换中文括号（）
                .replace("“", "") // 替换中文双引号
                .replace("”", "") // 替换中文双引号
                .replace("\"", "") // 替换英文双引号
                .replace("‘", "'") // 替换中文单引号 ‘
                .replace("’", "'") // 替换中文单引号  ’
                .replace("，", ",") // 替换中文逗号 ，
                .replace("：", ":") // 替换中文冒号：
                .replace("。", "."); // 替换中文句号
    }

    public static String replaceChart(String ch){
        return ch.replaceAll("[\\|]*[']*", "").trim();
    }

    /**
     * 全角转半角
     *
     * @param input
     * @return
     */
    public static String format2DBC(String input) {
        char c[] = input.toCharArray();
        for (int i = 0; i < c.length; i++) {
            if (c[i] == '\u3000') {
                c[i] = ' ';
            } else if (c[i] > '\uFF00' && c[i] < '\uFF5F') {
                c[i] = (char) (c[i] - 65248);

            }
        }
        String returnString = new String(c);
        return returnString;
    }

}
