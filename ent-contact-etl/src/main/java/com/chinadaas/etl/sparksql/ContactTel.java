package com.chinadaas.etl.sparksql;

import com.chinadaas.common.util.DataFrameUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * @author haoxing
 * 通过优先级去拿去电话
 * 优先级一是年报数据
 * 优先级二是照面数据
 * 优先级三是税务登记数据
 * 优先级四人员登记信息
 * 最后对进过这个优先级筛选后仍然存在号码为空的电话进行一次相对补全。
 * 这里的数据暂时没有选择date字段，三个字段的date会在与baseinfo关联的时候配上
 */
public class ContactTel {

    public  static Dataset Tel(SparkSession spark){
        String hql = "select pripid,tel ,position,source from ent_contactinfo_full";

         return DataFrameUtil.getDataFrame(spark,hql,"Tel");
    }


    public static Dataset getTelSix(SparkSession spark){


       String hql = "select pripid,tel from Tel where source = '2016' and tel <> 'null'";


       return DataFrameUtil.getDataFrame(spark,hql,"tmpsix");
    }


    public static Dataset getTelfive(SparkSession spark){

        String hql = "select pripid,tel from Tel where source = '2015' and tel <> 'null'";
        return DataFrameUtil.getDataFrame(spark,hql,"tmpfivetel");
    }



    public static Dataset getTelInfive(SparkSession spark){

        String hql = "select a.pripid,a.tel from tmpfivetel a where not exists(select 1 from tmpsix b where a.pripid = b.pripid )";
        return DataFrameUtil.getDataFrame(spark,hql,"tmpinfive");


    }
    //拿到了pripid具有唯一性的年报电话号码。
    public static Dataset getAnnulTel(SparkSession spark){
        String hql = "select * from (select * from tmpsix union select * from tmpinfive)";
        return DataFrameUtil.getDataFrame(spark,hql,"tmpannualtel");
    }

    //拿出全量照面信息电话
    public static Dataset getBaseTel(SparkSession spark){


       String hql = "select pripid,tel from Tel where source = 'base' and tel <> 'null'";

        return DataFrameUtil.getDataFrame(spark,hql,"tmpbasetel");
    }


    //拿出所有年报电话pripid 不在base表中的数据

    public static Dataset getBase(SparkSession spark){
        String hql = "select a.pripid,a.tel from tmpbasetel a where not exists(select 1 from tmpannualtel b where a.pripid = b.pripid)";

        return DataFrameUtil.getDataFrame(spark,hql,"tmpbase");
    }


    //拿到年报和照面的所有电话信息
    public static Dataset getAnBase(SparkSession spark){
       String hql = "select * from (select * from tmpannualtel union select * from tmpbase)";
       return DataFrameUtil.getDataFrame(spark,hql,"tmpanbase") ;

    }

    //拿到税务电话全量

    public static Dataset getTax(SparkSession spark){

       String hql = "select pripid,tel from Tel where source = 'tax' and tel <> 'null'";
        return DataFrameUtil.getDataFrame(spark,hql,"tmptax");
    }


    //拿出有效的税务电话

    public static Dataset getTaxTel(SparkSession spark){
        String hql = "select a.pripid,a.tel FROM tmptax a where not exists(select 1 from tmpanbase b where a.pripid = b.pripid)";

        return DataFrameUtil.getDataFrame(spark,hql,"tmptaxtel");
    }

    //合并上税务电话

    public static Dataset getAnBaTA(SparkSession spark){
        String hql = "select * from (select * from tmpanbase union select * from tmptaxtel )";

        return DataFrameUtil.getDataFrame(spark,hql,"tmpanbata");

    }

    //拿出人员电话
    //法人
    public static Dataset getlegalperson(SparkSession spark){


        String hql = "select pripid,tel from Tel where position = 'lerepsign' and tel <> 'null'";

        return DataFrameUtil.getDataFrame(spark,hql,"tmplegal");
    }


    //拿出法人优先级有效电话

    public static Dataset getlegaltel(SparkSession spark){
        String hql = "select a.pripid,a.tel from tmplegal a where not exists(select 1 from tmpanbata b where  a.pripid = b.pripid)";
        return DataFrameUtil.getDataFrame(spark,hql,"tmplegaltel");
    }

    //union法人电话
    public static Dataset getAnBaTaLe(SparkSession spark){
        String hql = "select * from (select * from tmpanbata union select * from tmplegaltel)";
       return DataFrameUtil.getDataFrame(spark,hql,"tmpanbatale");
    }

    //拿出董事长电话
    public static Dataset getChirMan(SparkSession spark){

       String hql = "select pripid,tel from Tel where position = '431A' and tel <> 'null'";
        return DataFrameUtil.getDataFrame(spark,hql,"tmpchiarman");
    }

    //拿出没有并集的董事长电话
    public static Dataset getChirManTel(SparkSession spark){
        String hql = "select a.pripid,a.tel from tmpchiarman a where not exists(select 1 from tmpanbatale b where a.pripid =b.pripid)";

        return DataFrameUtil.getDataFrame(spark,hql,"tmpchiarmantel");

    }

    //union董事长电话
    public static Dataset getAnBaTaLeCh(SparkSession spark){
        String hql = "select * from (select * from tmpanbatale union select * from tmpchiarmantel)";

         return DataFrameUtil.getDataFrame(spark,hql,"tmpanbatalech");
    }

    //拿出总经理电话

    public static Dataset getGeneralManager(SparkSession spark){

        String hql = "select pripid,tel from TEl where position = '434Q' and tel <> 'null'";

        return DataFrameUtil.getDataFrame(spark,hql,"tmpgenneralmanager");
    }

    //拿出有效的总经理电话

    public static Dataset getGeneralManagerTel(SparkSession spark){
        String hql = "select a.pripid,a.tel from tmpgenneralmanager a where not exists(select 1 from tmpanbatalech b where a.pripid =b.pripid)";
        return DataFrameUtil.getDataFrame(spark,hql,"tmpgeneralmanagertel");
    }

    //union总经理电话

    public static Dataset getAnBaTaLeChGm(SparkSession spark){
        String hql = "select * from (select * from tmpgeneralmanagertel union select * from tmpanbatalech)";

        return DataFrameUtil.getDataFrame(spark,hql,"tmpanbatalechgm");

    }

    //和在营企业的pripid关联后

    public static Dataset getrela(SparkSession spark) {
        String hql = "select a.pripid,b.tel from (select pripid from base_full where entstatus = '1')  a \n" +
                     "left join  tmpanbatalechgm b \n"+
                     "on a.pripid = b.pripid";

        return DataFrameUtil.getDataFrame(spark,hql,"tmprela");

    }

    //判断有多少个企业的电话是空的

    public static Dataset gettelpripid(SparkSession spark){
        String hql = "select pripid from tmprela where tel is null";

        return DataFrameUtil.getDataFrame(spark,hql,"othertel");

    }

    //关联这些电话

    public static Dataset getthesetel(SparkSession spark){
        String hql = "select a.pripid,\n" +
                "case when c.tel regexp '^(\\\\d)\\\\1{6,}$'\n " +
                "or (c.tel regexp '(13[0-9]|14[5|7]|15[0|1|2|3|5|6|7|8|9]|17[0|1|2|3|5|6|7|8|9]|18[0|1|2|3|5|6|7|8|9])\\\\d{8}|[1]\\\\d{3}-\\\\d{8}|\\\\d{4}-\\\\d{7}|\\\\d{5,8}') = false \n" +//过滤非正常电话号码
                "then 'null' \n" +
                "else c.tel end as tel\n" +
                "from othertel a \n" +
                "join person_full b on a.pripid = b.pripid \n" +
                "join person_full c on b.zspid = c.zspid\n" +
                "and b.zspid <> 'null' and b.zspid <> '' and b.zspid is not null\n" +
                "and c.zspid <> 'null' and c.zspid <> '' and c.zspid is not null\n" +
                "where c.tel <> ''";

   return DataFrameUtil.getDataFrame(spark,hql,"otherteldata");
    }

    public static Dataset getpuretel(SparkSession spark){
        String hql = "select pripid,tel from otherteldata where tel <> 'null'";
        return DataFrameUtil.getDataFrame(spark,hql,"tmppuretel");

    }


    //拿出有在这些id里的电话

    public static Dataset gethistel(SparkSession spark){
        String hql = "select pripid,tel from tmprela where tel is not null";

        return  DataFrameUtil.getDataFrame(spark,hql,"tmphistory");


    }

    public static Dataset getUnionreal(SparkSession spark){
        String hql = "select * from tmphistory union all select * from tmppuretel";


        return DataFrameUtil.getDataFrame(spark,hql,"tmpUnionrea");
    }


    public static Dataset getTel(SparkSession spark){
        Tel(spark);
        getTelSix(spark);
        getTelfive(spark);
        getTelInfive(spark);
        getAnnulTel(spark);
        getBaseTel(spark);
        getBase(spark);
        getAnBase(spark);
        getTax(spark);
        getTaxTel(spark);
        getAnBaTA(spark);
        getlegalperson(spark);
        getlegaltel(spark);
        getAnBaTaLe(spark);
        getChirMan(spark);
        getChirManTel(spark);
        getAnBaTaLeCh(spark);
        getGeneralManager(spark);
        getGeneralManagerTel(spark);
        getAnBaTaLeChGm(spark);

        getrela(spark);
        gettelpripid(spark);
        getthesetel(spark);
        getpuretel(spark);
        gethistel(spark);

        return getUnionreal(spark);




    }
}



