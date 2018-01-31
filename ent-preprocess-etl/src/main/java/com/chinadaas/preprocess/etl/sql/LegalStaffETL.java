package com.chinadaas.preprocess.etl.sql;

import com.chinadaas.common.util.DataFrameUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * Created by gongxs01 on 2017/11/29.
 */
public class LegalStaffETL {


    public static Dataset getLegalStaff(SparkSession spark,String model){

        getLerepInvChange(spark,model);

        return getLerepPositionChange(spark,model);
    }

    /**
     *  企业法人
     * @param spark
     * @return
     */
    private static Dataset getLerepInvChange(SparkSession spark,String model){
        String hql = "select distinct a.name, a.person_id, a.pripid, b.s_ext_sequence\n" +
                "  from e_pri_person_"+model+" a\n" +
                "  join entbaseinfo_"+model+" b\n" +
                "    on a.pripid = b.pripid\n" +
                " where a.name <> '' and  a.person_id<>''\n" +
                "   and a.lerepsign = '1'" ;

        return DataFrameUtil.getDataFrame(spark,hql,"lerep_invchange01"+model);
    }

    /**
     * 法人对外任职表更
     */
    private static Dataset getLerepPositionChange(SparkSession spark,String model){
        String hql = "select a.pripid ,a.person_id,c.entname, c.regno,\n" +
                "      c.enttype,c.regcap,c.regcapcur,c.entstatus, \n" +
                "      c.regorg,b.position,b.lerepsign,c.esdate,b.name,\n" +
                "      c.candate,c.revdate,b.pripid as ent_pripid,c.s_ext_sequence\n" +
                "    from lerep_invchange01"+model+" a\n" +
                "    join e_pri_person_"+model+" b\n" +
                "      on a.person_id = b.person_id" +
                "      and a.name=b.name " +
                "      and a.pripid <> b.pripid" +
                "    join entbaseinfo_"+model+" c" +
                "      on b.pripid=c.pripid " ;

        return DataFrameUtil.getDataFrame(spark,hql,"lerep_positionchange"+model);
    }
}
