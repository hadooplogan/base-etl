package com.chinadaas.etl.sparksql;

import com.chinadaas.common.util.DataFrameUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * @author haoxing
 */
public class ContactEmail {
    public static Dataset Email(SparkSession spark){
        String hql = "select pripid,\n"+
                " email,position,source from ent_contactinfo_full ";

         return DataFrameUtil.getDataFrame(spark,hql,"Email");
    }

    public static Dataset getTelSix(SparkSession spark){

        String hql = "select pripid,email from Email where source = '2016' and email <> 'null'";

        return DataFrameUtil.getDataFrame(spark,hql,"tmpsix");
    }


    public static Dataset getTelfive(SparkSession spark){

        String hql = "select pripid,email from Email where source = '2015' and email <> 'null'";
        return DataFrameUtil.getDataFrame(spark,hql,"tmpfivetel");
    }



    public static Dataset getTelInfive(SparkSession spark){

        String hql = "select a.pripid,a.email from tmpfivetel a where not exists(select 1 from tmpsix b where a.pripid = b.pripid )";
        return DataFrameUtil.getDataFrame(spark,hql,"tmpinfive");


    }

    public static Dataset getAnnulTel(SparkSession spark){
        String hql = "select * from (select * from tmpsix union select * from tmpinfive)";
        return DataFrameUtil.getDataFrame(spark,hql,"tmpannualtel");
    }


    public static Dataset getBaseTel(SparkSession spark){

        String hql = "select pripid,email from Email where source = 'base' and email <> 'null'";
        return DataFrameUtil.getDataFrame(spark,hql,"tmpbasetel");
    }




    public static Dataset getBase(SparkSession spark){
        String hql = "select a.pripid,a.email from tmpbasetel a where not exists(select 1 from tmpannualtel b where a.pripid = b.pripid)";

        return DataFrameUtil.getDataFrame(spark,hql,"tmpbase");
    }



    public static Dataset getAnBase(SparkSession spark){
        String hql = "select * from (select * from tmpannualtel union select * from tmpbase)";
        return DataFrameUtil.getDataFrame(spark,hql,"tmpanbase") ;

    }





    public static Dataset getEmail(SparkSession spark){
        Email(spark);
        getTelSix(spark);
        getTelfive(spark);
        getTelInfive(spark);
        getAnnulTel(spark);
        getBaseTel(spark);
        getBase(spark);
       return getAnBase(spark);


    }
}
