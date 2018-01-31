package com.chinadaas.etl.sparksql;

import com.chinadaas.common.util.DataFrameUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * @author haoxing
 * person的数据的update
 * 需要删除的数据，把id存储到hdfs上
 */
public class PersonUpdateFull {

    //那都增量表数据中的person 过滤处理后的数据，传入增量更新的date
    private static Dataset getIncFilter(SparkSession spark, String datadate) {

        String hql = "select concat_ws('','person',pripid,s_ext_sequence) as id,\n" +
                "pripid,\n" +
                "case when tel regexp '^(\\\\d)\\\\1{6,}$'\n" +
                "or tel regexp '^[1][3,4,5,7,8]\\\\D{6}\\\\d{3}$'\n" +
                "or tel regexp '^[1][3,4,5,7,8][0-9]\\\\D{5}\\\\d{3}$'\n" +
                "or tel regexp '^\\\\d((?<=0)1|(?<=1)2|(?<=2)3|(?<=3)4|(?<=4)5|(?<=5)6|(?<=6)7|(?<=7)8|(?<=8)9){7}$'\n" +
                "or tel regexp '^\\\\d((?<=0)1|(?<=1)2|(?<=2)3|(?<=3)4|(?<=4)5|(?<=5)6|(?<=6)7|(?<=7)8|(?<=8)9){6}$'\n" +
                "or tel regexp '^[\\\\u4e00-\\\\u9fa5]{5,}$'\n" +
                "or (tel regexp '(13[0-9]|14[5|7]|15[0|1|2|3|5|6|7|8|9]|18[0|1|2|3|5|6|7|8|9])\\\\d{8}|\\\\d{3}-\\\\d{8}|\\\\d{4}-\\\\d{7}|\\\\d{5,8}') = false \n" +
                "then 'null' \n" +
                "when tel is null or tel = '' then 'null'\n" +
                "when length(tel) < 5 then 'null'\n" +
                "else tel end as tel,\n" +
                "'null' as email,\n" +
                "case when dom is null then 'null' when dom = '' then 'null' when length(dom) <6 then 'null' else dom end as address,\n" +
                "lerepsign,\n" +
                "position,\n" +
                "" + datadate + " as date\n" +
                " from person_inc";

        return DataFrameUtil.getDataFrame(spark, hql, "tmpincperson");
    }

    //通过自定义的唯一主键确认出全新数据(这里确认数据唯一性只能用id)，这里没有添加credit_code 和 entname
    private static Dataset getNewData(SparkSession spark) {

        String hql = "select a.id,\n" +
                "a.pripid,\n" +
                "a.tel,\n" +
                "a.email,\n" +
                "a.address,\n" +
                "case when a.lerepsign = '1' then 'lerepsign' \n" +
                "else a.position end as position,\n" +
                "'person' as source ,\n" +
                "date from \n" +
                "tmpincperson a \n" +
                "where \n" +
                "not exists(select 1 from tmpperson b where a.id = b.id)";

        return DataFrameUtil.getDataFrame(spark, hql, "tmpnewdata");
    }

    //拿出数据是tel,email,address其中之一不为'null'的，这三个字段都为null的数据删除

    private static Dataset getConfirmNewData(SparkSession spark) {

        String hql = "select * from tmpnewdata where tel <> 'null' or email <> 'null' or address <> 'null'";

        return DataFrameUtil.getDataFrame(spark, hql, "tmprealnew");


    }

    //拿出历史变更数据（整串已经更新数据） 没有限制职位的增量数据 (这个地方使用的new data数据是没有过滤的全新数据)
    private static Dataset getChange(SparkSession spark) {

        String hql = "select a.id,\n" +
                "a.pripid,\n" +
                "a.tel,\n" +
                "a.email,\n" +
                "a.address,\n" +
                "a.position ,a.date from \n" +
                "tmpincperson a \n" +
                "where \n" +
                "not exists(select 1 from tmpnewdata b where a.id = b.id)";
        return DataFrameUtil.getDataFrame(spark, hql, "tmpchange");
    }

    //拿出历史变更数据（更新前数据）历史变更数据 这个变更数据(是职位已经限制过了的)
    private static Dataset getHiChange(SparkSession spark) {

       /** String hql = "select id,\n" +
                "pripid,\n" +
                "tel,\n" +
                "email,\n" +
                "address,\n" +
                "position,\n" +
                "source ,date from tmpperson \n" +
                "where id in (select id from tmpchange)"; **/

       String hql = "select a.id,\n" +
               "a.pripid,\n" +
               "a.tel,\n" +
               "a.email,\n" +
               "a.address,\n" +
               "a.position,\n" +
               "a.source,\n" +
               "a.date\n" +
               "from tmpperson a left semi join tmpchange b on (a.id = b.id)";

        return DataFrameUtil.getDataFrame(spark, hql, "tmphichange");
    }

    //数据进行比对，拼出字符串确认数据，并拿出确实更新的数据，这个数据更新后确实是更新的数据，因为是从历史数据作为基准比对。
    private static Dataset getAllDiff(SparkSession spark) {

        String hql = "select a.id,\n" +
                "a.pripid,\n" +
                "case when b.tel <> 'null' then b.tel else a.tel end as tel,\n" +
                "'null' as email," +
                "case when b.address <> 'null' then b.address else a.address end as address,\n" +
                "a.position,\n" +
                "a.source,\n" +
                "case when b.tel <> 'null' or b.address <> 'null' then b.date else a.date end as date\n" +
                "from tmphichange a\n" +
                "left join tmpchange b on a.id = b.id \n" +
                "where concat_ws('-',a.tel,a.email,a.address) <> concat_ws('-',b.tel,b.email,b.address)";
        return DataFrameUtil.getDataFrame(spark, hql, "tmpalldiff");

    }

    //拿出符合职位的全新数据

    private static Dataset getAllNew(SparkSession spark) {

        String hql = "select id,\n" +
                "pripid,\n" +
                "tel,\n" +
                "email,\n" +
                "address,\n" +
                "position,\n" +
                "source,\n" +
                "date\n" +
                "from tmprealnew \n" +
                "where position = 'lerepsign' \n" +
                "or (position <> 'lerepsign' and position = '431A' )\n" +
                "or (position <> 'lerepsign' and position = '434Q' )";

        return DataFrameUtil.getDataFrame(spark, hql, "tmpallnew");

    }

    //组装更新和需要插入的数据
    private static Dataset getUpdate(SparkSession spark) {

        String hql = "select * from tmpallnew union all select * from tmpalldiff";

        return DataFrameUtil.getDataFrame(spark, hql, "tmpupdate");

    }

    //加上credit_code和entname需要和最新的enterprisecollect表的pripid关联拿到entname和credit_code
    private static Dataset getCode(SparkSession spark) {
     //这个地方的数据存在一个bug就是我们的base数据没有的企业的时候，这个企业的的entname和credit_code会没有。
        String hql = "select a.id,\n" +
                "a.pripid,\n" +
                "b.credit_code,\n" +
                "b.entname,\n" +
                "a.tel,\n" +
                "a.email,\n" +
                "a.address,\n" +
                "a.position,\n" +
                "a.source,\n" +
                "date\n" +
                " from tmpupdate a \n" +
                "left join base_full b \n" +
                "on a.pripid = b.pripid";
        return DataFrameUtil.getDataFrame(spark, hql, "tmpgetcode");
    }


    //现在拿出del文件，进行唯一主键的组装。
    private static Dataset getPersonID(SparkSession spark) {

        String hql = "select concat_ws('','person',pripid,s_ext_sequence) as id \n" +
                "from person_del";


        return DataFrameUtil.getDataFrame(spark, hql, "tmppersonid");

    }

    //没有出现在变更数据里的del数据是需要删除的数据
    private static Dataset getPersonDel(SparkSession spark) {

        String hql = "select id from tmppersonid a \n" +
                "where\n" +
                "not exists\n" +
                "(select 1 from tmpchange b where a.id = b.id)";

        return DataFrameUtil.getDataFrame(spark, hql, "tmpersonpdelid");
    }

    //确认要删除数据存在于历史数据里，只删除存在于历史的id
    private static Dataset ConfirmPersonDel(SparkSession spark) {

        //String hql = "select id from tmpersonpdelid where id in(select id from tmpperson)";

        String hql = "select a.id from tmpersonpdelid a left semi join tmpperson b on(a.id = b.id)";
        return DataFrameUtil.getDataFrame(spark, hql, "tmprealpersondel");
    }

    //对id进行去重操作

    private static Dataset DistinctDelId(SparkSession spark) {

        String hql = "select distinct id from tmprealpersondel";

        return DataFrameUtil.getDataFrame(spark, hql, "tmpdistinctdel");
    }

    // 组成发生变化的数据集(变更和删除)
    private static Dataset NotIn(SparkSession spark) {
        String hql = "select id from tmpalldiff union select id from tmprealpersondel";
        return DataFrameUtil.getDataFrame(spark, hql, "tmpnotin");

    }

    private static Dataset getPersonCode(SparkSession spark) {

        String hql = "select a.id,\n" +
                "a.pripid,\n" +
                "b.credit_code,\n" +
                "b.entname,\n" +
                "a.tel,\n" +
                "a.email,\n" +
                "a.address,\n" +
                "a.position,\n" +
                "a.source,\n" +
                "a.date \n" +
                "from tmpperson a \n" +
                "left join base_full b \n" +
                "on a.pripid = b.pripid";

        return DataFrameUtil.getDataFrame(spark, hql, "persongetcode");
    }

    //历史数据中完全没有发生变化的数据  使用的确实存在组装后的全量数据中的变化数据集。
    private static Dataset getNoChange(SparkSession spark) {

        String hql = "select id,\n" +
                "a.pripid,\n" +
                "a.credit_code,\n" +
                "a.entname,\n" +
                "a.tel,\n" +
                "a.email,\n" +
                "a.address,\n" +
                "a.position,\n" +
                "a.source,\n" +
                "a.date\n" +
                "from\n" +
                "persongetcode a \n" +
                "where not exists\n" +
                "(select 1 from tmpnotin b where a.id = b.id)";

        return DataFrameUtil.getDataFrame(spark, hql, "tmpnochange");
    }

    //全新要存储到hdfs上的数据
    private static Dataset gettrueperson(SparkSession spark) {

        String hql = "select * from tmpnochange union select * from tmpgetcode";

        return DataFrameUtil.getDataFrame(spark, hql, "tmptrueperson");
    }

    /**
     * 删除逻辑：我们del文件中有历史变更前的数据，和股东变化后需要删除股东的数据，
     * 数据进行比对后吧del文件中的变更数据除去后就是垃圾数据和变更要删除的股东。
     * 把id存储到hdfs，text格式，读取hdfs文件，删除es索引中这些id。
     */


    //person表的需要删除的数据
    public static Dataset PerosonDel(SparkSession spark, String datadate) {


        //比对出变更的数据的id，除了变更的数据就是需要删除的数据。

        getIncFilter(spark, datadate);
        getNewData(spark);
        getChange(spark);
        getPersonID(spark);
        getPersonDel(spark);
        ConfirmPersonDel(spark);
        return DistinctDelId(spark);
    }

    //准好的person的更新数据，可以直接写入的数据。
    public static Dataset getEsPerson(SparkSession spark, String datadate) {


        getIncFilter(spark, datadate);
        getNewData(spark);
        getConfirmNewData(spark);
        getChange(spark);
        getHiChange(spark);
        getAllDiff(spark);
        getAllNew(spark);
        getUpdate(spark);

        return getCode(spark);
    }


    //存储到hdfs上更新后的数据模块数据

    public static Dataset getPersonInc(SparkSession spark, String datadate) {


        getIncFilter(spark, datadate);
        getNewData(spark);
        getConfirmNewData(spark);
        getChange(spark);
        getHiChange(spark);
        getAllDiff(spark);
        getAllNew(spark);
        getUpdate(spark);
        getPersonID(spark);
        getPersonDel(spark);
        ConfirmPersonDel(spark);
        getCode(spark);
        NotIn(spark);
        getPersonCode(spark);
        getNoChange(spark);

        return gettrueperson(spark);


    }

}
