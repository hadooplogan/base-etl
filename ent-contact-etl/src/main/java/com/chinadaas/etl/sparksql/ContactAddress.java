package com.chinadaas.etl.sparksql;

import com.chinadaas.common.util.DataFrameUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * @author haoxing
 * 按照优先级来拿去地址数据
 *
 * 添加过滤地址长度小于6的算法，仍然按照优先级去选择。
 *
 */
public class ContactAddress {
    private static Dataset Address(SparkSession spark){

        String hql = "select pripid,address,position,source from ent_contactinfo_full where address <> 'null'";

        return DataFrameUtil.getDataFrame(spark,hql,"Address");
    }


    private static Dataset getTelSix(SparkSession spark){

       String hql = "select pripid,address from Address where source = '2106' and length(address) > 6 ";

        return DataFrameUtil.getDataFrame(spark,hql,"tmpsix");
    }


    private static Dataset getTelfive(SparkSession spark){


        String hql = "select pripid,address from Address where source = '2015' and length(address) > 6 ";
        return DataFrameUtil.getDataFrame(spark,hql,"tmpfivetel");
    }



    private static Dataset getTelInfive(SparkSession spark){

        String hql = "select a.pripid,a.address from tmpfivetel a where not exists(select 1 from tmpsix b where a.pripid = b.pripid )";
        return DataFrameUtil.getDataFrame(spark,hql,"tmpinfive");


    }

    private static Dataset getAnnulTel(SparkSession spark){
        String hql = "select * from (select * from tmpsix union select * from tmpinfive)";
        return DataFrameUtil.getDataFrame(spark,hql,"tmpannualtel");
    }


    private static Dataset getBaseTel(SparkSession spark){

String hql = "select pripid,address from Address where source = 'base' and length(address) > 6 ";
        return DataFrameUtil.getDataFrame(spark,hql,"tmpbasetel");
    }




    private static Dataset getBase(SparkSession spark){
        String hql = "select a.pripid,a.address from tmpbasetel a where not exists(select 1 from tmpannualtel b where a.pripid = b.pripid)";

        return DataFrameUtil.getDataFrame(spark,hql,"tmpbase");
    }



    private static Dataset getAnBase(SparkSession spark){
        String hql = "select * from (select * from tmpannualtel union select * from tmpbase)";
        return DataFrameUtil.getDataFrame(spark,hql,"tmpanbase") ;

    }



    private static Dataset getTax(SparkSession spark){
      //  String hql = "select pripid,tax_addr from s_tax_baseinfo where tax_addr is not null and tax_addr <> 'null' and tax_addr <> '' ";

        String hql = "select pripid,address from Address where source = 'tax' and length(address) > 6 ";
        return DataFrameUtil.getDataFrame(spark,hql,"tmptax");
    }




    private static Dataset getTaxTel(SparkSession spark){
        String hql = "select a.pripid,a.address FROM tmptax a where not exists(select 1 from tmpanbase b where a.pripid = b.pripid)";

        return DataFrameUtil.getDataFrame(spark,hql,"tmptaxtel");
    }



    private static Dataset getAnBaTA(SparkSession spark){
        String hql = "select * from (select * from tmpanbase union select * from tmptaxtel )";

        return DataFrameUtil.getDataFrame(spark,hql,"tmpanbata");

    }


    private static Dataset getlegalperson(SparkSession spark){

       String hql = "select pripid,address from Address where position = 'lerepsign' and length(address) > 6 ";

        return DataFrameUtil.getDataFrame(spark,hql,"tmplegal");
    }




    private static Dataset getlegaltel(SparkSession spark){
        String hql = "select a.pripid,a.address from tmplegal a where not exists(select 1 from tmpanbata b where  a.pripid = b.pripid)";
        return DataFrameUtil.getDataFrame(spark,hql,"tmplegaltel");
    }


    private static Dataset getAnBaTaLe(SparkSession spark){
        String hql = "select * from (select * from tmpanbata union select * from tmplegaltel)";
        return DataFrameUtil.getDataFrame(spark,hql,"tmpanbatale");
    }


    private static Dataset getChirMan(SparkSession spark){

        String hql = "select pripid,address from Address where position = '431A' and length(address) > 6 ";
        return DataFrameUtil.getDataFrame(spark,hql,"tmpchiarman");
    }


    private static Dataset getChirManTel(SparkSession spark){
        String hql = "select a.pripid,a.address from tmpchiarman a where not exists(select 1 from tmpanbatale b where a.pripid =b.pripid)";

        return DataFrameUtil.getDataFrame(spark,hql,"tmpchiarmantel");

    }


    private static Dataset getAnBaTaLeCh(SparkSession spark){
        String hql = "select * from (select * from tmpanbatale union select * from tmpchiarmantel)";

        return DataFrameUtil.getDataFrame(spark,hql,"tmpanbatalech");
    }



    private static Dataset getGeneralManager(SparkSession spark){

        String hql = "select pripid,address from Address where position = '434Q' and length(address) > 6 ";

        return DataFrameUtil.getDataFrame(spark,hql,"tmpgenneralmanager");
    }



    private static Dataset getGeneralManagerTel(SparkSession spark){
        String hql = "select a.pripid,a.address from tmpgenneralmanager a where not exists(select 1 from tmpanbatalech b where a.pripid =b.pripid)";
        return DataFrameUtil.getDataFrame(spark,hql,"tmpgeneralmanagertel");
    }



    private static Dataset getAnBaTaLeChGm(SparkSession spark){
        String hql = "select * from (select * from tmpgeneralmanagertel union select * from tmpanbatalech)";

        return DataFrameUtil.getDataFrame(spark,hql,"tmpanbatalechgm");

    }

    private static Dataset getUngotId(SparkSession spark){

        String hql = "select a.pripid from (select pripid from base_full where entstatus = '1') a \n" +
                     "where not exists(select 1 from tmpanbatalechgm b where a.pripid =b.pripid) ";

        return DataFrameUtil.getDataFrame(spark,hql,"tmpgetungotid");
    }

    private static Dataset getanless(SparkSession spark){

        String hql = "select a.pripid,a.address from (select pripid,address from Address where source = '2106') a where a.pripid in(select pripid from tmpgetungotid)";

        return DataFrameUtil.getDataFrame(spark,hql,"getanless");
    }

   public static Dataset getaddress(SparkSession spark){

        String hql = "select * from(select * from tmpanbatalechgm union all select * from getanless)";

        return DataFrameUtil.getDataFrame(spark,hql,"getaddress");

   }


    public static Dataset getAddress(SparkSession spark){
        Address(spark);
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
        getUngotId(spark);
        getanless(spark);

        return getaddress(spark);
    }
}
