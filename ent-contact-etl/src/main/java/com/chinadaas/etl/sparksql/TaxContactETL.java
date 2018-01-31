package com.chinadaas.etl.sparksql;

import com.chinadaas.common.util.DataFrameUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * @author haoxing
 * 添加物理主键
 */
public class TaxContactETL {

   private static Dataset filter(SparkSession spark,String datadate)
   {
       String hql =  "select concat_ws('',tax_archives_no,pripid) as id,\n" +
                     "pripid,\n" +
                     "case when tax_tel regexp '^(\\\\d)\\\\1{6,}$'\n " +
                     "or tax_tel regexp '^[1][3,4,5,7,8]\\\\D{6}\\\\d{3}$'\n"+
                     "or tax_tel regexp '^[1][3,4,5,7,8][0-9]\\\\D{5}\\\\d{3}$'\n"+
                     "or tax_tel regexp '^\\\\d((?<=0)1|(?<=1)2|(?<=2)3|(?<=3)4|(?<=4)5|(?<=5)6|(?<=6)7|(?<=7)8|(?<=8)9){7}$'"+
                     "or tax_tel regexp '^\\\\d((?<=0)1|(?<=1)2|(?<=2)3|(?<=3)4|(?<=4)5|(?<=5)6|(?<=6)7|(?<=7)8|(?<=8)9){6}$'"+
                     "or tax_tel regexp '^[\\\\u4e00-\\\\u9fa5]{5,}$'\n"+
                     "or (tax_tel regexp '(13[0-9]|14[5|7]|15[0|1|2|3|5|6|7|8|9]|17[0|1|2|3|5|6|7|8|9]|18[0|1|2|3|5|6|7|8|9])\\\\d{8}|[1]\\\\d{3}-\\\\d{8}|\\\\d{4}-\\\\d{7}|\\\\d{5,8}') = false \n"+
                     "then 'null' \n" +
                     "when tax_tel is null or tax_tel = '' then 'null'\n"+
                     "when length(tax_tel) < 5 then 'null'\n "+
                     "else tax_tel end as tel,\n"+
                     "'null' as email,\n"+
                     "case when tax_addr is null then 'null' when tax_addr = '' then 'null' else tax_addr end as address,\n" +
                     "'null' as position,\n" +
                     "'tax' as source,\n" +
                     "regexp_replace(substr(udt,1,10),'-','') as udt,\n" +
                     ""+datadate+" as date\n" +
                     "from s_tax_baseinfo";

       return DataFrameUtil.getDataFrame(spark, hql, "taxcontactetl");
   }

    private static Dataset NoNull(SparkSession spark){

        String hql = "select * from taxcontactetl where tel <> 'null' or email <> 'null' or address <> 'null'";

        return DataFrameUtil.getDataFrame(spark,hql,"tmpnonull");
    }


    public static Dataset getTaxContactETL (SparkSession spark,String datadte){
        filter(spark,datadte);

        return NoNull(spark);

    }
}
