package com.chinadaas.etl.sparksql;

import com.chinadaas.common.util.DataFrameUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * @author haoxing
 * 添加唯一主键 id 'person'+pripid+s_ext_sequence
 * <p>
 * 这里没有对身兼数职位 做去重 因为 数据可能会存在 互补 对三个职位进行最大保留数据。
 * 加上了timestamp 是数据本身的更新时间 加上 date 作为数据的跑批日期。
 */
public class PersonInfoContactETL {
    //重复数据按照分组最新批次拿数据。
    private static Dataset LegalPerson(SparkSession spark, String datadate) {
        //String hql = "select pripid,tel,'null' as email,case when dom is null then 'null' when dom = '' then 'null' else dom end as address,'lerepsign' as position,'person' as source from e_pri_person_full_20171116 where lerepsign = 1 ";
        String hql = " select concat_ws('','person',pripid,s_ext_sequence) as id,\n" +
                "pripid,\n" +
                "tel,\n" +
                "'null' as email,\n" +
                "case when dom is null then 'null' when dom = '' then 'null' else dom end as address,\n" +
                "'lerepsign' as position,\n" +
                "'person' as source, \n" +
                "regexp_replace(substr(s_ext_timestamp,1,10),'-','') as udt\n" +
                "from (select pripid,\n" +
                "tel,\n" +
                "dom,\n" +
                "s_ext_sequence,\n" +
                "s_ext_timestamp,\n" +
                "row_number () over (partition by pripid ORDER BY s_ext_timestamp desc) rn \n" +
                "from  e_pri_person where lerepsign = '1' ) t1\n" +
                "where t1.rn = 1 ";

        return DataFrameUtil.getDataFrame(spark, hql, "tmplegalperson");
    }

    private static Dataset ChairMan(SparkSession spark, String datadate) {
        //String hql = "select pripid,tel,'null' as email, case when dom is null then 'null' when dom = '' then 'null' else dom end as address,'431A' as position,'person' as source from e_pri_person_full_20171116 where position = '431A' ";
        String hql = " select concat_ws('','person',pripid,s_ext_sequence) as id,\n" +
                "pripid,\n" +
                "tel,\n" +
                "'null' as email,\n" +
                "case when dom is null then 'null' when dom = '' then 'null' else dom end as address,\n" +
                "'431A' as position,\n" +
                "'person' as source,\n" +
                "regexp_replace(substr(s_ext_timestamp,1,10),'-','') as udt\n" +
                "from (select pripid,\n" +
                "tel,\n" +
                "dom,\n" +
                "s_ext_sequence,\n" +
                "s_ext_timestamp,\n" +
                "row_number () over (partition by pripid ORDER BY s_ext_timestamp desc) rn \n" +
                "from  e_pri_person where position = '431A' ) t1\n" +
                "where t1.rn = 1 ";
        return DataFrameUtil.getDataFrame(spark, hql, "tmpchairman");
    }

    private static Dataset GeneralManager(SparkSession spark, String datadate) {
        // String hql = "select pripid,tel,'null' as email,case when dom is null then 'null' when dom = '' then 'null' else dom end as address,'434Q' as position,'person' as source from e_pri_person_full_20171116 where position = '434Q' ";
        String hql = " select concat_ws('','person',pripid,s_ext_sequence) as id,\n" +
                " pripid,\n" +
                "tel,\n" +
                "'null' as email,case when dom is null then 'null' when dom = '' then 'null' else dom end as address,\n" +
                "'434Q' as position,\n" +
                "'person' as source,\n" +
                "regexp_replace(substr(s_ext_timestamp,1,10),'-','') as udt\n" +
                "from (select pripid,\n" +
                "tel,\n" +
                "dom,\n" +
                "s_ext_sequence,\n" +
                "s_ext_timestamp,\n" +
                "row_number () over (partition by pripid ORDER BY s_ext_timestamp desc) rn \n" +
                "from  e_pri_person where position = '434Q'  ) t1\n" +
                "where t1.rn = 1 ";
        return DataFrameUtil.getDataFrame(spark, hql, "tmpgeneralmanager");
    }


    //拿出所有上述三个职位的企业的
    private static Dataset unionPersonInfo(SparkSession spark) {
        String hql = "select * from (select * from tmplegalperson union all select * from tmpchairman union all select * from tmpgeneralmanager) a";

        return DataFrameUtil.getDataFrame(spark, hql, "tmpunionpersoninfo");
    }


    private static Dataset Filter(SparkSession spark, String datadate) {

        String hql = "select id,\n" +
                " pripid,\n" +
                "case when tel regexp '^(\\\\d)\\\\1{6,}$'\n " +
                "or tel regexp '^[1][3,4,5,7,8]\\\\D{6}\\\\d{3}$'\n" +
                "or tel regexp '^[1][3,4,5,7,8][0-9]\\\\D{5}\\\\d{3}$'\n" +
                "or tel regexp '^\\\\d((?<=0)1|(?<=1)2|(?<=2)3|(?<=3)4|(?<=4)5|(?<=5)6|(?<=6)7|(?<=7)8|(?<=8)9){7}$'" +
                "or tel regexp '^\\\\d((?<=0)1|(?<=1)2|(?<=2)3|(?<=3)4|(?<=4)5|(?<=5)6|(?<=6)7|(?<=7)8|(?<=8)9){6}$'" +
                "or tel regexp '^[\\\\u4e00-\\\\u9fa5]{5,}$'\n" +  //过滤汉字
                "or (tel regexp '(13[0-9]|14[5|7]|15[0|1|2|3|5|6|7|8|9]|17[0|1|2|3|5|6|7|8|9]|18[0|1|2|3|5|6|7|8|9])\\\\d{8}|[1]\\\\d{3}-\\\\d{8}|\\\\d{4}-\\\\d{7}|\\\\d{5,8}') = false \n" +//过滤非正常电话号码
                "then 'null' \n" +
                "when tel is null or tel = '' then 'null'\n" +
                "when length(tel) < 5 then 'null'\n " +
                "else tel end as tel,\n" +
                "email,\n" +
                "address,\n" +
                "position,\n" +
                "source,\n" +
                "udt,\n" +
                "" + datadate + " as date\n" +
                "from tmpunionpersoninfo ";

        return DataFrameUtil.getDataFrame(spark, hql, "persondata");
    }

    private static Dataset NoNull(SparkSession spark){

        String hql = "select * from tmpbaseinfocontact where tel <> 'null' or email <> 'null' or address <> 'null'";

        return DataFrameUtil.getDataFrame(spark,hql,"tmpnonull");
    }




    public static Dataset getPersonInfoContactETL(SparkSession spark, String datadate) {

        LegalPerson(spark, datadate);
        ChairMan(spark, datadate);
        GeneralManager(spark, datadate);
        unionPersonInfo(spark);

        Filter(spark, datadate);

        return NoNull(spark);
    }


}