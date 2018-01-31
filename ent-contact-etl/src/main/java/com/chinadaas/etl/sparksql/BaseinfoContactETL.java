package com.chinadaas.etl.sparksql;

import com.chinadaas.common.util.DataFrameUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * @author haoxing
 * 添加物理主键 id我们使用'base'+pripid+sequence数据。
 */
public class BaseinfoContactETL {


    private static Dataset filter(SparkSession spark, String datadate)
    {

        String hql = "select concat_ws('','base',pripid,s_ext_sequence) as id,\n" +
                     "pripid,\n" +
                     "case when tel regexp '^(\\\\d)\\\\1{6,}$'\n " +
                     "or tel regexp '^[1][3,4,5,7,8]\\\\D{6}\\\\d{3}$'\n"+
                     "or tel regexp '^[1][3,4,5,7,8][0-9]\\\\D{5}\\\\d{3}$'\n"+
                     "or tel regexp '^\\\\d((?<=0)1|(?<=1)2|(?<=2)3|(?<=3)4|(?<=4)5|(?<=5)6|(?<=6)7|(?<=7)8|(?<=8)9){7}$'"+
                     "or tel regexp '^\\\\d((?<=0)1|(?<=1)2|(?<=2)3|(?<=3)4|(?<=4)5|(?<=5)6|(?<=6)7|(?<=7)8|(?<=8)9){6}$'"+
                     "or tel regexp '^[\\\\u4e00-\\\\u9fa5]{5,}$'\n"+
                     "or (tel regexp '(13[0-9]|14[5|7]|15[0|1|2|3|5|6|7|8|9]|17[0|1|2|3|5|6|7|8|9]|18[0|1|2|3|5|6|7|8|9])\\\\d{8}|[1]\\\\d{3}-\\\\d{8}|\\\\d{4}-\\\\d{7}|\\\\d{5,8}') = false \n"+
                     "then 'null' \n" +
                     "when tel is null or tel = '' then 'null'\n"+
                     "when length(tel) < 5 then 'null'\n"+
                     "else tel end as tel,\n"+
                     "case when email regexp '^(\\\\d)\\\\1{3,}@\\\\w+([-.]\\\\w+)*\\\\.\\\\w+([-.]\\\\w+)*$'\n" +
                     "or email regexp '^(\\\\D)\\\\1{3,}@\\\\w+([-.]\\\\w+)*\\\\.\\\\w+([-.]\\\\w+)*$'\n"+
                     "or (email regexp '^\\\\w+([-+.]\\\\w+)*@\\\\w+([-.]\\\\w+)*\\\\.\\\\w+([-.]\\\\w+)*$') = false\n"+
                     "then 'null' \n"+
                     "else email end as email,\n"+
                     "case when dom is null then 'null' when dom = '' then 'null' when length(dom) <6 then 'null' else dom end as address,\n" +
                     "'null' as position,\n" +
                     "'base' as source,\n" +
                     "regexp_replace(substr(s_ext_timestamp,1,10),'-','') as udt,\n" +
                     ""+datadate+" as date\n" +
                     "from enterprisebaseinfocollect";



        return DataFrameUtil.getDataFrame(spark, hql, "tmpbaseinfocontact");
    }

    private static Dataset NoNull(SparkSession spark){

        String hql = "select * from tmpbaseinfocontact where tel <> 'null' or email <> 'null' or address <> 'null'";

        return DataFrameUtil.getDataFrame(spark,hql,"tmpnonull");
    }

     public static Dataset getBaseinfoContact (SparkSession spark,String datadate){
        filter(spark,datadate);

        return NoNull(spark);
     }

}
