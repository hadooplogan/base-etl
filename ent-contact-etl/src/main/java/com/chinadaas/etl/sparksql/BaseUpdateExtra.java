package com.chinadaas.etl.sparksql;

import com.chinadaas.common.util.DataFrameUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * @author haoxing
 * 读取的hdfs历史文件数据的表名称可以用脚本建立好，也可以每次把parquet文件注册成临时表使用。
 * 针对在营企业的联系信息，我们采取的方式不同。
 * 主键就是pripid，判断当前企业当前在营与否。
 * 更新在营企业发生的信息变更，插入全新的数据。
 * 这里的删除逻辑就是，和调数据表更后非在营了，就删除这条数据。
 * 还有del种工厂需要删除的错误的数据。
 *
 *
 */
public class BaseUpdateExtra {
    //从增量表中拿到更新的  在营  的企业信息
    private static Dataset getIncIn(SparkSession spark,String datadate){
        String hql = "select\n" +
                "pripid,\n" +
                "entname,\n" +
                "credit_code,\n" +
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
                "case when dom is null then 'null' when dom = '' then 'null' when length(dom) < 6 then 'null' else dom end as address,"+datadate+" as date\n" +
                "from base_inc\n" +
                "where entstatus = 1"; //在营企业
        return DataFrameUtil.getDataFrame(spark,hql,"tmpIncIn");

    }



    //用这部分在营数据确认数据是变更数据还是全新数据

    private static Dataset getAllNew(SparkSession spark){

        String hql =" select pripid,\n" +
                "entname,\n" +
                "credit_code,\n" +
                "tel,\n" +
                "email,\n"+
                "address,\n" +
                "date\n" +
                "from tmpIncIn a \n" +
                "where not exists\n" +
                "(select 1 from entcontact_extract b where a.pripid = b.pripid)";


        return  DataFrameUtil.getDataFrame(spark,hql,"tmpallnew");

    }


    //删选数据 拿出新增数据中 tel,email,address,中其中有一条数据不为'null'的数据

    private static Dataset ConfirmNewData(SparkSession spark){

        String hql = "select * from tmpallnew where tel <> 'null' or email <> 'null' or address <> 'null'";

        return DataFrameUtil.getDataFrame(spark,hql,"tmprealnew");

    }

    //在营的变更数据,更新后的数据(确认变更，使用的是没有过滤的新增数据做比对)

    private static Dataset getChangeData(SparkSession spark){

        String hql = "select pripid,\n" +
                "entname,\n" +
                "credit_code,\n" +
                "tel,\n" +
                "email,\n"+
                "address,\n" +
                "date\n" +
                "from tmpIncIn a \n" +
                "where not exists\n" +
                "(select 1 from tmpallnew b where a.pripid = b.pripid)";

        return DataFrameUtil.getDataFrame(spark,hql,"tmpchangedata");

    }

    //在营的变更前历史待变更数据数据，变更之前的历史内容

    private static Dataset getHiChange(SparkSession spark){

    /** String hql = "select pripid,\n" +
              "entname,\n" +
              "credit_code,\n" +
              "tel,\n" +
              "email,\n"+
              "address,\n" +
              "date\n" +
              "from \n" +
              "entcontact_extract \n" +
              "where pripid in\n " +
              "(select pripid from tmpchangedata)";*/


     String hql = "select a.pripid,\n" +
             "a.entname,\n" +
             "a.credit_code,\n" +
             "a.tel,\n" +
             "a.email,\n" +
             "a.address,\n" +
             "a.date\n" +
             "from \n" +
             "entcontact_extract a left semi join tmpchangedata b on (a.pripid = b.pripid)";

        return DataFrameUtil.getDataFrame(spark,hql,"tmphichange");


    }

    //拿到在营企业的在这三个字段中确实发生变更的数据

    private static Dataset getRealChange(SparkSession spark){

        String hql = "select a.pripid,\n" +
                "a.entname,\n" +
                "a.credit_code,\n" +
                "case when b.tel <> 'null' then b.tel else a.tel end as tel,\n" +
                "case when b.email <> 'null' then b.email else a.email end as email,\n" +
                "case when b.address <> 'null' then b.address else a.address end as address,\n" +
                "case when b.tel <> 'null' or b.email <> 'null' or b.address <> 'null' then b.date else a.date end as date\n"+
                "from tmphichange a\n" +
                "left join tmpchangedata b \n" +
                "on a.pripid = b.pripid\n" +
                "where concat_ws('-',a.tel,a.email,a.address) <> concat_ws('-',b.tel,b.email,b.address)";

        return DataFrameUtil.getDataFrame(spark,hql,"tmprealchange");

    }

    //merge数据，更新的数据

    private static Dataset getUpdate(SparkSession spark){

        String hql = "select * from tmprealnew union select * from tmprealchange";

        return DataFrameUtil.getDataFrame(spark,hql,"tmpextraupdate");


    }

    private static Dataset NotIn(SparkSession spark){

        String hql = "select pripid from tmprealchange union all select * from delconfirm";

       return DataFrameUtil.getDataFrame(spark,hql,"tmpnotin");
    }

    //拿到完全没有改变的数据 顺便把删除的数据在此处删除
    private static Dataset getNoChanged(SparkSession spark){

        String hql = "select a.pripid,\n" +
                "a.entname,\n" +
                "a.credit_code,\n" +
                "a.tel,\n" +
                "a.email,\n"+
                "a.address,\n" +
                "a.date\n" +
                " from \n" +
                "entcontact_extract a where not exists\n" +
                "(select 1 from tmpnotin b where a.pripid = b.pripid)";

        return DataFrameUtil.getDataFrame(spark,hql,"tmpnochange");
    }


     //得到存储到hdfs上的全量数据
    private static Dataset getBaseHdfs(SparkSession spark){

        String hql = "select * from tmpnochange union  all select * from tmpextraupdate";

        return DataFrameUtil.getDataFrame(spark,hql,"tmpbasehdfs");
    }


    //拿到Es需要更新的数据

    public static Dataset getEsUpdate(SparkSession spark,String datadate){
        getIncIn(spark,datadate);

        getAllNew(spark);
        ConfirmNewData(spark);
        getChangeData(spark);
        getHiChange(spark);
        getRealChange(spark);

       return getUpdate(spark);


    }

    //


    //单一主键的数据我们要考虑到在营的情况,得和inc文件里面需要变更的数据进行比对，因为只有这个里面的数据是变更的才可以确认不需要删除。
    //1，这一步是拿出了无效数据，就是错误数据或者彻底不需要的数据
    private static Dataset getDel(SparkSession spark){
        String hql = "select a.pripid\n" +
                "from base_del a \n" +
                "where not exists\n" +
                "(select 1 from base_inc b where a.pripid = b.pripid)";

        return DataFrameUtil.getDataFrame(spark,hql,"tmpextradel");

    }
    //2，这部分删除数据是 企业变为了非在营企业(inc里面也和del里面都有变更的数据，这两个数据的主将相同，内容不同，变更项也可能是企业经营状态变更，entstatus在这不过量数据中变为了 非 '1'，在此就是要删除的数据)
    private static Dataset getEntstatusChanged(SparkSession spark){

      String hql = "select a.pripid from base_inc a join base_del b \n" +
              "on a.pripid = b.pripid where a.entstatus<> 1 and b.entstatus = '1'";

       return DataFrameUtil.getDataFrame(spark,hql,"tmpstatuschange");

    }


    private static Dataset getExtraDel(SparkSession spark){

        String hql = "select * from tmpextradel union all select * from tmpstatuschange";

        return DataFrameUtil.getDataFrame(spark,hql,"tmpturedel");

    }

    //确认删除数据存在于历史数据中

    private static Dataset confirmDel(SparkSession spark){

       // String hql = "select pripid from tmpturedel where pripid in (select pripid from entcontact_extract)";

        String hql = "select a.pripid from tmpturedel a left semi join entcontact_extract b on (a.pripid = b.pripid)";

        return DataFrameUtil.getDataFrame(spark,hql,"delconfirm");
    }

    //对删除id进行去重
    private static Dataset disctinctDelId(SparkSession spark){

        String hql = "select distinct pripid from delconfirm";

        return DataFrameUtil.getDataFrame(spark,hql,"distinctid");
    }
    //存入hdfs需要删除的数据
    public static Dataset ExtraIndexDel(SparkSession spark){
        getDel(spark);
        getEntstatusChanged(spark);
         getExtraDel(spark);
        confirmDel(spark);
        return  disctinctDelId(spark);
    }

    //存入hdfs
    public static Dataset ExtraHdfs(SparkSession spark,String datedate){
        getIncIn(spark,datedate);

        getAllNew(spark);
        ConfirmNewData(spark);
        getChangeData(spark);
        getHiChange(spark);
        getRealChange(spark);
        getUpdate(spark);
        getDel(spark);
        getEntstatusChanged(spark);
        getExtraDel(spark);
        NotIn(spark);
        getNoChanged(spark);

        return getBaseHdfs(spark);




    }

}
