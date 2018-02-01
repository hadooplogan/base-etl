package com.chinadaas.association.etl.sparksql;

import com.chinadaas.common.util.DataFrameUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class ZuZhiJiGou {
    public static Dataset InformationGainStats(SparkSession spark) {
        String sql = "select pripid,          \n" +
                "credit_code     ,\n" +
                "ogz_code        ,\n" +
                "cert_no         ,\n" +
                "nodenum         ,\n" +
                "stringhandle(ogz_name) as ogz_name,\n" +
                "stringhandle(ogz_name) as ogz_name_index,\n"+
                "ogz_type        ,\n" +
                "ischaritable    ,\n" +
                "ogz_regorg_code ,\n" +
                "ogz_regorg_name ,\n" +
                "ogz_reg_date    ,\n" +
                "ogz_holdorg_code,\n" +
                "ogz_holdorg_name,\n" +
                "ogz_status      ,\n" +
                "ogz_from        ,\n" +
                "ogz_to          ,\n" +
                "ogz_addr        ,\n" +
                "ogz_scope       ,\n" +
                "tel             ,\n" +
                "fax             ,\n" +
                "email           ,\n" +
                "website         ,\n" +
                "regcap          ,\n" +
                "regcap_unit     ,\n" +
                "regcap_curr     ,\n" +
                "ogz_lerep       ,\n" +
                "get_datetime    ,\n" +
                "data_status     ,\n" +
                "data_date       ,\n" +
                "idt             ,\n" +
                "udt from s_ogz_baseinfo";

        return DataFrameUtil.getDataFrame(spark, sql, "tmpogz");
    }

}
