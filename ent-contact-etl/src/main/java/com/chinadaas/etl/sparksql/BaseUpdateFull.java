package com.chinadaas.etl.sparksql;

import com.chinadaas.common.util.DataFrameUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * @author haoxing
 * base数据源的数据的增量更新
 * 所有es的更新都是建立在hdfs每次的更新的基础上
 * base的数据我们使用pripird来确认数据比较方便，base里面的数据pripid不相同。
 * hdfs覆盖是文件(新文件)
 * del数据存储到hdfs上，再使用java来读取hdfs文件，es的api来删除数据。
 */
public class BaseUpdateFull {


    //先拿到我们的base增量数据,对数据进行过滤,过滤电话规则，过滤email，判断地址是否长度大于6

    public static Dataset getBaseInc(SparkSession spark, String datadate) {
        String hql = "select concat_ws('','base',pripid,s_ext_sequence) as id,\n" +
                "pripid,\n" +
                "credit_code,\n" +
                "entname,\n" +
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
                "case when email regexp '^(\\\\d)\\\\1{3,}@\\\\w+([-.]\\\\w+)*\\\\.\\\\w+([-.]\\\\w+)*$'\n" +
                "or email regexp '^(\\\\D)\\\\1{3,}@\\\\w+([-.]\\\\w+)*\\\\.\\\\w+([-.]\\\\w+)*$'\n" +
                "or (email regexp '^\\\\w+([-+.]\\\\w+)*@\\\\w+([-.]\\\\w+)*\\\\.\\\\w+([-.]\\\\w+)*$') = false\n" +
                "then 'null' \n" +
                "else email end as email,\n" +
                "case when dom is null then 'null' \n" +
                "when dom = '' then 'null' when length(dom) <6 then 'null' \n" +
                "else dom end as address," + datadate + " as date from base_inc";

        return DataFrameUtil.getDataFrame(spark, hql, "tmpgetbaseinc");
    }


    //给tmpbase更新code
    private static Dataset addCodeToBase(SparkSession spark) {

        String hql = "select a.id,\n" +
                "a.pripid,\n" +
                "b.credit_code,\n" +
                "b.entname,\n" +
                "a.tel,\n" +
                "a.email,\n" +
                "a.address,\n" +
                "a.position,\n" +
                "a.source,\n" +
                "a.date from \n" +
                "tmpbase a left join base_full b\n" +
                "on a.pripid = b.pripid";
        return DataFrameUtil.getDataFrame(spark, hql, "tmpcodebase");
    }

    //相比历史在营企业，拿出纯粹更新的企业数据（inc数据不在历史数据中存在的数据）
    public static Dataset getRealNew(SparkSession spark) {
        String hql = "select id,\n" +
                "credit_code,\n" +
                "entname,\n" +
                "pripid,\n" +
                "tel,\n" +
                "email,\n" +
                "address,\n" +
                "'null' as position,\n" +
                "'base' as source,\n" +
                "date\n" +
                "from tmpgetbaseinc a \n" +
                "where not exists\n" +
                "(select 1 from tmpbase b where a.id = b.id)";

        return DataFrameUtil.getDataFrame(spark, hql, "tmpgetbasenew");
    }


    //纯粹更新数据中不能存在联系方式都为空的数据
    public static Dataset getConfirmNew(SparkSession spark) {

        String hql = "select * from tmpgetbasenew \n" +
                "where tel <> 'null' and email <> 'null' and address <> 'null'";


        return DataFrameUtil.getDataFrame(spark, hql, "tmpConfirm");
    }

    //拿到已经变更数据（inc中的数据除了纯粹更新的新数据,未确认数据）
    public static Dataset getHiC(SparkSession spark) {
        String hql = "select id,\n" +
                "pripid,\n" +
                "credit_code,\n" +
                "entname,\n" +
                "tel,\n" +
                "email,\n" +
                "address,\n" +
                "date\n" +
                "from tmpgetbaseinc a where not exists\n" +
                "(select 1 from tmpgetbasenew b where a.id = b.id)";

        return DataFrameUtil.getDataFrame(spark, hql, "tmphic");

    }


    //拿到变更前的历史数据（整条发生变更的那一部分,tmpbase 是历史的base数据，tmphic是发生了变更的数据。）
    public static Dataset getHidata(SparkSession spark) {


        /** String hql = "select id,\n" +
         "pripid,\n" +
         "tel,\n" +
         "email,\n" +
         "address,\n" +
         "date \n" +
         "from tmpbase \n" +
         "where id in(select id from tmphic)";*/


        String hql = "select a.id,\n" +
                "a.pripid,\n" +
                "a.tel,\n" +
                "a.email,\n" +
                "a.address,\n" +
                "a.date \n" +
                "from tmpbase a left semi join tmphic b on (a.id = b.id)";

        return DataFrameUtil.getDataFrame(spark, hql, "tmphidataraw");
    }


    //历史数据进行比对拿出不相同的数据,把经过校验且不为空的数据进行更新，确认出更新数据集。(在这里的确实是历史变更的数据,比对的数据是整个所有字段拼成的串)。
    public static Dataset getDiff(SparkSession spark) {

        String hql = "select a.id,\n" +
                "a.pripid,\n" +
                "a.credit_code,\n" +
                "a.entname,\n" +
                "case when a.tel = 'null' then b.tel else a.tel end as tel,\n" +
                "case when a.email = 'null' then b.email else a.email end as email,\n" +
                "case when a.address = 'null' then b.address else a.address end as address,\n" +
                "'null' as position,\n" +
                "'base' as source,\n" +
                "case when b.tel <> 'null'\n" +
                " or b.email <> 'null' or \n" +
                "b.address <> 'null' \n" +
                "then b.date else a.date end as date\n" +
                " from tmphic \n" +
                "a join tmphidataraw b on a.pripid = b.pripid \n" +
                "where concat_ws('-',a.pripid,a.tel,a.email,a.address) \n" +
                "<> concat_ws('-',b.pripid,b.tel,b.email,b.address)";

        return DataFrameUtil.getDataFrame(spark, hql, "tmpdiff");
    }


    //union纯更新数据，拿到了在联系方式这里真实更新的数据.（new base 是需要更新覆盖的数据，全新可更新数据是tmpConfirm）

    public static Dataset getUnionBase(SparkSession spark) {

        String hql = "select * from tmpdiff union all select * from tmpConfirm";

        return DataFrameUtil.getDataFrame(spark, hql, "Unionbase");

    }


    //这里是我们确实发生了变化的数据id
    private static Dataset NotIn(SparkSession spark) {

        String hql = "select id from tmpdiff union all select id from tmprealdel";
        return DataFrameUtil.getDataFrame(spark, hql, "tmpnotin");

    }

    //拿到历史未变动数据(历史数据没有发生变化的差集数据)   【这一步已经删除了要删除的数据，因为del的数据已经归类在del数据中了】
    private static Dataset getNoChange(SparkSession spark) {
        String hql = "select id,\n" +
                "pripid,\n" +
                "credit_code,\n" +
                "entname,\n" +
                "tel,\n" +
                "email,\n" +
                "address,\n" +
                "position,\n" +
                "source,\n" +
                "date\n" +
                "from tmpcodebase a \n" +
                "where not exists\n" +
                "(select 1 from tmpnotin b where a.id = b.id)";
        return DataFrameUtil.getDataFrame(spark, hql, "tmpnochange");
    }


    //更新后的base数据加历史未删除未改动数据加上全新更新的数据。 tmpdiff变更更新的历史数据，tmpnochange未发生变化数据，tmpConfirm 纯粹更新数据。
    private static Dataset gettruebase(SparkSession spark) {

        String hql = "select * from tmpnochange union select * from tmpdiff union select * from tmpConfirm";

        return DataFrameUtil.getDataFrame(spark, hql, "tmptruenewbase");

    }


    //全新的base数据（存入hdfs方法）

    public static Dataset getBaseDataInc(SparkSession spark, String datadate) {

        getBaseInc(spark, datadate);
        addCodeToBase(spark);
        getRealNew(spark);
        getConfirmNew(spark);
        getHiC(spark);
        getHidata(spark);
        getDiff(spark);
        getUnionBase(spark);
        BaseDel(spark);
        ConfirmDel(spark);
        NotIn(spark);
        getNoChange(spark);

        return gettruebase(spark);


    }

    //给es更新的数据（不需要union历史数据，只徐亚那一部分数据）

    public static Dataset getBaseIncEs(SparkSession spark, String datadate) {

        getBaseInc(spark, datadate);
        getRealNew(spark);
        getConfirmNew(spark);
        getHiC(spark);
        getHidata(spark);
        getDiff(spark);


        return getUnionBase(spark);


    }

    /**
     * 需要删除的数据拿出读取del 然后组装主键 base这里的关联条件就是pripid，
     * 且因为我们的del文件包含历史变更前的数据和一些要彻底删除的数据。
     * 拿到del里面的彻底删除数据(这部分数据在es里面没有办法去覆盖掉，所以是直接删除，历史变更数据是可以覆盖掉的)
     */


    //全量数据的更新不需要考虑企业是否在营的问题，所以只要不是在历史变更数据中的数据 就认为是要删除的数据。
    private static Dataset BaseDel(SparkSession spark) {

        String hql = "select concat_ws('','base',pripid,s_ext_sequence) as id\n" +
                "from base_del a \n" +
                "where " +
                "not exists \n" +
                "(select 1 from tmphic b where a.pripid = b.pripid) ";

        return DataFrameUtil.getDataFrame(spark, hql, "tmpbasedel");

    }

    private static Dataset ConfirmDel(SparkSession spark) {

        //String hql = "select id from tmpbasedel where id in (select id from tmpbase)";

        String hql = "select a.id from tmpbasedel a left semi join tmpbase b on(a.id = b.id)";

        return DataFrameUtil.getDataFrame(spark, hql, "tmprealdel");

    }

    private static Dataset distinctDel(SparkSession spark) {

        String hql = "select distinct id from tmprealdel";

        return DataFrameUtil.getDataFrame(spark, hql, "tmpdistinctdel");
    }


    public static Dataset FullBaseDel(SparkSession spark, String datadate) {
        getBaseInc(spark, datadate);
        getRealNew(spark);
        getHiC(spark);
        BaseDel(spark);
        ConfirmDel(spark);

        return distinctDel(spark);
    }


}
