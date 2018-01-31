package com.chinadaas.refresh.etl.sql;

import com.chinadaas.common.util.DataFrameUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * Created by gongxs01 on 2017/11/10.
 */
public class RefreshMasterETL {


    public static Dataset refreshMaster(SparkSession spark){

        //法人对外投资变更
        getLerepInvChange(spark);
        //企业对外投资变更
        getEntInvChange(spark);
        //法人对外任职变更
        getLerepPositionChange(spark);

        getAllChange(spark);
        return getAllChange01(spark);
    }

    /**
     * 法人对外投资变更
     *
     */

    public static Dataset getLerepInvChange(SparkSession spark){
        getLerepInvChange01(spark);
        return  getLerepInvChange02(spark);
    }


    private static Dataset getLerepInvChange01(SparkSession spark){
        String hql = "select distinct a.name, a.person_id, a.pripid, b.s_ext_sequence\n" +
                "  from e_pri_person a\n" +
                "  join entbaseinfo b\n" +
                "    on a.pripid = b.pripid\n" +
                " where (a.name <> '' and a.person_id <> '')\n" +
                "   and a.lerepsign = '1'" ;

        return DataFrameUtil.getDataFrame(spark,hql,"lerep_invchange01");
    }

    private static Dataset getLerepInvChange02(SparkSession spark){
        String hql = "select distinct en.pripid,en.s_ext_sequence\n" +
                "   from lerep_invchange01 en\n" +
                "   join e_inv_investment hd\n" +
                "     on hd.invid = en.person_id\n" +
                "    and en.name = hd.inv\n" +
                "  where hd.invid <> '' ";

        return DataFrameUtil.getDataFrame(spark,hql,"lerep_invchange02");
    }

    /**
     * 企业对外投资变更
     */

    public static Dataset getEntInvChange(SparkSession spark){
        getEntInvChange01(spark);
        getEntInvChange02(spark);
        getEntInvChange03(spark);
        return  getEntInvChange04(spark);
    }

    private static Dataset getEntInvChange01(SparkSession spark){
        String hql = "select en.pripid,en.s_ext_sequence \n" +
                "  from (select distinct pripid, entname,s_ext_sequence\n" +
                "          from entbaseinfo\n" +
                "         where entname <> ''\n" +
                "           and entname <> 'null') en,\n" +
                "       (select inv \n" +
                "          from e_inv_investment\n" +
                "         where inv <> '') hd\n" +
                " where hd.inv = en.entname";

        return DataFrameUtil.getDataFrame(spark, hql, "invRelaTmp001");
    }

    private static Dataset getEntInvChange02(SparkSession spark){
        String hql = "select en.pripid,en.s_ext_sequence \n" +
                "  from (select distinct pripid, credit_code,s_ext_sequence\n" +
                "          from entbaseinfo\n" +
                "         where credit_code <> ''\n" +
                "           and credit_code <> 'null') en,\n" +
                "       (select inv, blicno, pripid\n" +
                "          from e_inv_investment\n" +
                "         where blicno <> ''\n" +
                "           and length(blicno) > 17) hd\n" +
                " where hd.blicno = en.credit_code\n" +
                "   and en.pripid <> hd.pripid";
        return DataFrameUtil.getDataFrame(spark, hql, "invRelaTmp003");
    }


    private static Dataset getEntInvChange03(SparkSession sqlContext){
        String hql = "select en.pripid ,en.s_ext_sequence\n" +
                "  from (select distinct pripid, entname_old,s_ext_sequence\n" +
                "          from entbaseinfo\n" +
                "         where entname_old <> ''\n" +
                "           and entname_old <> 'null') en,\n" +
                "       (select inv\n" +
                "          from e_inv_investment\n" +
                "         where inv <> '') hd\n" +
                " where hd.inv = en.entname_old";

        return DataFrameUtil.getDataFrame(sqlContext, hql, "invRelaTmp005");
    }

    private static Dataset getEntInvChange04(SparkSession sqlContext){
        String hql = "select distinct pripid,s_ext_sequence \n" +
                "        from (select pripid,s_ext_sequence\n" +
                "                from invRelaTmp001\n" +
                "              union\n" +
                "              select pripid,s_ext_sequence\n" +
                "                from invRelaTmp003\n" +
                "              union\n" +
                "              select pripid,s_ext_sequence\n" +
                "                from invRelaTmp005)";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "invRelaTmp03");

    }

    /**
     * 法人对外任职表更
     */
    public static Dataset getLerepPositionChange(SparkSession spark){
        String hql = "select distinct b.pripid,c.s_ext_sequence \n" +
                "    from lerep_invchange01 a\n" +
                "    join e_pri_person b\n" +
                "      on a.person_id = b.person_id" +
                "    join entbaseinfo c" +
                "      on b.pripid=c.pripid ";

       return DataFrameUtil.getDataFrame(spark,hql,"lerep_positionchange");
    }

    public static Dataset getAllChange(SparkSession spark){
        String hql = " select pripid,'ENTERPRISEBASEINFOCOLLECT' as model ,s_ext_sequence from entbaseinfo" +
                "      union " +
                "      select pripid ,'E_GT_BASEINFO' as model,s_ext_sequence  from gtentbaseinfo" +
                "      union" +
                "      select pripid , ' ' as model,s_ext_sequence  from invRelaTmp03 " +
                "      union " +
                "      select pripid , 'E_PRI_PERSON_POSITION' as model,s_ext_sequence from lerep_positionchange" +
                "      union " +
                "      select pripid ,'E_PRI_PERSON_INV' as model,s_ext_sequence from lerep_invchange02 ";

        return DataFrameUtil.getDataFrame(spark,hql,"all_change");
    }

    private static Dataset getAllChange01(SparkSession spark){
        String hql = " select distinct pripid,collect_set(model) as model,s_ext_sequence,from_unixtime(unix_timestamp(), 'yyyy-MM-dd') as data_date from all_change group by pripid,s_ext_sequence";

        return DataFrameUtil.getDataFrame(spark,hql,"all_change01");
    }


    public static Dataset df2Text(SparkSession spark){

        refreshMaster(spark);

        String hql ="select concat_ws('|',\n" +
                "                 a.userid,\n" +
                "                 concat_ws('\\u0001', a.node, a.pripid, b.s_ext_sequence),\n" +
                "                 concat_ws(',',b.model) ) as text\n" +
                "  from order a\n" +
                "  join all_change01 b\n" +
                "    on a.pripid = b.pripid " +
                "    where a.monitorStatus='ACTIVE' \n";
        return DataFrameUtil.getDataFrame(spark,hql,"dfText");
    }



}
