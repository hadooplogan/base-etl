package com.chinadaas.preprocess.etl.sql;

import com.chinadaas.common.util.DataFrameUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * Created by gongxs01 on 2017/11/29.
 */
public class LegalInvETL {

    /**
     * 法人对外投资变更
     *
     */

    public static Dataset getLerepInvChange(SparkSession spark,String model){
        getLerepInvChange01(spark,model);
        return  getLerepInvChange02(spark,model);
    }


    private static Dataset getLerepInvChange01(SparkSession spark,String model){
        String hql = "select distinct a.name, a.person_id, a.pripid, b.s_ext_sequence\n" +
                "  from e_pri_person_"+model+" a\n" +
                "  join entbaseinfo_"+model+" b\n" +
                "    on a.pripid = b.pripid\n" +
                " where (a.name <> '' and a.person_id <> '')\n" +
                "   and a.lerepsign = '1'" ;

        return DataFrameUtil.getDataFrame(spark,hql,"lerep_invchange01");
    }

    private static Dataset getLerepInvChange02(SparkSession spark,String model){
        String hql = "select  en.pripid,ba.entname,ba.regno," +
                "             ba.enttype,ba.regcap,ba.regcapcur,ba.entstatus,ba.regorg," +
                "             hd.subconam,hd.currency,hd.conprop," +
                "             ba.esdate,ba.name,ba.candate,ba.revdate,hd.conform,en.s_ext_sequence\n" +
                "   from lerep_invchange01 en\n" +
                "   join e_inv_investment_"+model+" hd\n" +
                "     on hd.invid = en.person_id" +
                "    and en.name = hd.inv\n" +
                "   join entbaseinfo_"+model+" ba\n" +
                "    on hd.pripid=ba.pripid\n" +
                "  where hd.invid <> '' ";

        return DataFrameUtil.getDataFrame(spark,hql,"lerep_invchange02");
    }



}
