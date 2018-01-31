package com.chinadaas.preprocess.etl.sql;

import com.chinadaas.common.util.DataFrameUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * Created by gongxs01 on 2017/11/29.
 */
public class EntInvETL {


    /**
     * 企业对外投资变更
     */

    public static Dataset getEntInvChange(SparkSession spark,String model){
        getEntInvChange01(spark,model);
        getEntInvChange02(spark,model);
        getEntInvChange03(spark,model);
        return  getEntInvChange04(spark,model);
    }

    private static Dataset getEntInvChange01(SparkSession spark,String model){
        String hql = "select en.pripid,en.s_ext_sequence,hd.pripid as ent_pripid \n" +
                "  from (select distinct pripid, entname,s_ext_sequence\n" +
                "          from entbaseinfo_"+model+"\n" +
                "         where entname <> ''\n" +
                "           and entname <> 'null') en,\n" +
                "       (select inv,pripid \n" +
                "          from e_inv_investment_"+model+"\n" +
                "         where inv <> '') hd\n" +
                " where hd.inv = en.entname";

        return DataFrameUtil.getDataFrame(spark, hql, "invRelaTmp001");
    }

    private static Dataset getEntInvChange02(SparkSession spark,String model){
        String hql = "select en.pripid,en.s_ext_sequence,hd.pripid as ent_pripid \n" +
                "  from (select distinct pripid, credit_code,s_ext_sequence\n" +
                "          from entbaseinfo_"+model+"\n" +
                "         where credit_code <> ''\n" +
                "           and credit_code <> 'null') en,\n" +
                "       (select inv, blicno, pripid\n" +
                "          from e_inv_investment_"+model+"\n" +
                "         where blicno <> ''\n" +
                "           and length(blicno) > 17) hd\n" +
                " where hd.blicno = en.credit_code\n" +
                "   and en.pripid <> hd.pripid";
        return DataFrameUtil.getDataFrame(spark, hql, "invRelaTmp003");
    }


    private static Dataset getEntInvChange03(SparkSession sqlContext,String model){
        String hql = "select en.pripid ,en.s_ext_sequence,hd.pripid as ent_pripid\n" +
                "  from (select distinct pripid, entname_old,s_ext_sequence\n" +
                "          from entbaseinfo_"+model+"\n" +
                "         where entname_old <> ''\n" +
                "           and entname_old <> 'null') en,\n" +
                "       (select inv,pripid\n" +
                "          from e_inv_investment_"+model+"\n" +
                "         where inv <> '') hd\n" +
                " where hd.inv = en.entname_old";

        return DataFrameUtil.getDataFrame(sqlContext, hql, "invRelaTmp005");
    }

    private static Dataset getEntInvChange04(SparkSession sqlContext,String model){
        String hql = "select distinct pripid,s_ext_sequence,ent_pripid \n" +
                "        from (select *\n" +
                "                from invRelaTmp001\n" +
                "              union\n" +
                "              select *\n" +
                "                from invRelaTmp003\n" +
                "              union\n" +
                "              select *\n" +
                "                from invRelaTmp005)";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "invRelaTmp03");

    }

    private static Dataset getEntInvChange05(SparkSession sqlContext,String model){
        String hql = " select a.pripid,\n" +
                "       b.entname,\n" +
                "       b.regno,\n" +
                "       b.enttype,\n" +
                "       b.regcap,\n" +
                "       b.regcapcur,\n" +
                "       b.entstatus,\n" +
                "       b.regorg,\n" +
                "       a.subconam,\n" +
                "       a.currency,\n" +
                "       a.fundedratio,\n" +
                "       b.esdate,\n" +
                "       b.name,\n" +
                "       b.candate,\n" +
                "       b.revdate,\n" +
                "       b.conform,\n" +
                "       b.regorgcode\n" +
                "  from invRelaTmp03 a\n" +
                "  join entbaseinfo_model b\n" +
                "    on a.ent_pripid = b.pripid";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "invRelaTmp10");

    }


}
