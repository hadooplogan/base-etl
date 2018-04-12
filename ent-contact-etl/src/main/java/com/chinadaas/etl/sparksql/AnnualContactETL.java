package com.chinadaas.etl.sparksql;

import com.chinadaas.common.util.DataFrameUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * @author haoxing
 * 对全量数据如果需要增量更新的话
 * 需要确认唯一主键，确认数据，且唯一主键必须来自于数据
 * 年报的主键使用了年份加pripid
 * <p>
 * 生产环境上的年报数据是15年和16年在一起的数据，表名一样，限制时间。
 */
public class AnnualContactETL {

    public static Dataset getAnnualContact(SparkSession spark, String datadate) {
        getfifteenYearContact(spark);
        getSixeenYearContact(spark);
        UnionAnnualReport(spark);

        Filter(spark, datadate);
        return NoNull(spark);
    }


    //15年年报数据(并且在营)，且15年年报没有pripid，所以使用企业名称关联 拿到pripid，设置数据的更新时间为20160330.
    private static Dataset getfifteenYearContact(SparkSession spark) {
        String hql = "select concat_ws('',b.pripid,'2015') as id,\n" +
                "b.pripid,\n" +
                "a.tel,\n" +
                "a.email,\n" +
                "case when a.addr is null then 'null' when a.addr = '' then 'null' else a.addr end as address,\n" +
                "'null' as position,\n" +
                "'2015' as source,\n" +
                "'20160630' as udt" +
                " from (select * from annreportbaseinfo where ancheyear = '2015' and entname <> '') a \n" +
                "join enterprisebaseinfocollect b \n" +
                "on a.entname = b.entname and b.entstatus = '1'";
        return DataFrameUtil.getDataFrame(spark, hql, "tmpfifteen");
    }

    //16年年报数据，设置数据的更新时间udt为20170630
    private static Dataset getSixeenYearContact(SparkSession spark) {
        String hql = "select concat_ws('',pripid,'2016') as id,\n" +
                " pripid,\n" +
                "tel,\n" +
                "email,\n" +
                "case when addr is null then 'null' when addr = '' then 'null' else addr end as address,\n" +
                "'null' as position,\n" +
                "'2016' as source, \n" +
                "'20170730' as udt\n" +
                "from annreportbaseinfo where ancheyear = '2016' ";

        return DataFrameUtil.getDataFrame(spark, hql, "tmpsixteen");
    }

    //merge数据15年和16年数据
    private static Dataset UnionAnnualReport(SparkSession spark) {
        String hql = "select * from (select * from tmpfifteen union all select * from tmpsixteen) annul";

        return DataFrameUtil.getDataFrame(spark, hql, "tmpannul");
    }

    // 过滤数据，确认可用性
    private static Dataset Filter(SparkSession spark, String datadate) {
        String hql = "select id,pripid,case when tel regexp '(\\\\d)\\\\1{6,}'\n " +//过滤了7位以上连续出现的数字
                "or tel regexp '^[1][3,4,5,7,8]\\\\D{6}\\\\d{3}$'\n" + //过滤了13******789
                "or tel regexp '^[1][3,4,5,7,8][0-9]\\\\D{5}\\\\d{3}$'\n" + //过滤了137*****098
                "or tel regexp '^\\\\d((?<=0)1|(?<=1)2|(?<=2)3|(?<=3)4|(?<=4)5|(?<=5)6|(?<=6)7|(?<=7)8|(?<=8)9){7}$'" +
                "or tel regexp '^\\\\d((?<=0)1|(?<=1)2|(?<=2)3|(?<=3)4|(?<=4)5|(?<=5)6|(?<=6)7|(?<=7)8|(?<=8)9){6}$'" +
                "or tel regexp '^[\\\\u4e00-\\\\u9fa5]{5,}$'\n" +
                "or (tel regexp '(13[0-9]|14[5|7]|15[0|1|2|3|5|6|7|8|9]|17[0|1|2|3|5|6|7|8|9]|18[0|1|2|3|5|6|7|8|9])\\\\d{8}|[1]\\\\d{3}-\\\\d{8}|\\\\d{4}-\\\\d{7}|\\\\d{5,8}') = false\n" +
                "then 'null' \n" +
                "when tel is null or tel = '' then 'null'\n" +
                "when length(tel) < 5 then 'null'\n " +      //过滤了长度小于5的数据
                "else tel end as tel,\n" +
                "case when email regexp '^(\\\\d)\\\\1{3,}@\\\\w+([-.]\\\\w+)*\\\\.\\\\w+([-.]\\\\w+)*$'\n" +
                "or email regexp '^(\\\\D)\\\\1{3,}@\\\\w+([-.]\\\\w+)*\\\\.\\\\w+([-.]\\\\w+)*$'\n" +
                "or (email regexp '^\\\\w+([-+.]\\\\w+)*@\\\\w+([-.]\\\\w+)*\\\\.\\\\w+([-.]\\\\w+)*$') = false\n" +
                "then 'null' \n" +
                "else email end as email,\n" +
                "address,position,source,udt," + datadate + " as date\n" +
                "from tmpannul";


        return DataFrameUtil.getDataFrame(spark, hql, "annualdata");
    }

    private static Dataset NoNull(SparkSession spark){

        String hql = "select * from annualdata where tel <> 'null' or email <> 'null' or address <> 'null'";

        return DataFrameUtil.getDataFrame(spark,hql,"tmpnonull");
    }
}
