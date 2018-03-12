package com.chinadaas.next.etl.sparksql.sql;

import com.chinadaas.common.util.DataFrameUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * 企业上市信息
 */
public class ListedInformation {

    private static Dataset tmp01(SparkSession spark) {

        String hql = "select distinct pripid ,\n" +
                "  compcode\n" +
                "  from ((select a.pripid ,b.compcode,b.islist \n" +
                "  from enterprisebaseinfocollect a \n" +
                "  left join \n" +
                "  TQ_COMP_INFO b on a.entname = b.compname where b.islist = '1' and compname <> 'null' and compname is not null)\n" +
                "  union all\n" +
                "  (select a.pripid ,b.compcode,b.islist \n" +
                "  from enterprisebaseinfocollect a \n" +
                "  left join \n" +
                "  TQ_COMP_INFO b on a.regno = b.bizlicenseno where b.islist = '1' and bizlicenseno <> 'null' and bizlicenseno is not null)\n" +
                "  union all \n" +
                "  (select a.pripid ,b.compcode,b.islist\n" +
                "  from enterprisebaseinfocollect a \n" +
                "  join \n" +
                "  TQ_COMP_INFO b on  a.licid = regexp_replace(b.orgcode,'-','') where b.islist = '1' and orgcode <> 'null' and orgcode is not null)) a ";


        return DataFrameUtil.getDataFrame(spark, hql, "tmp01");
    }

    private static Dataset tmp02(SparkSession spark) {

        String hql = "select pripid,\n" +
                "'是' as ee0033,\n" +
                "case when substr(trim(b.symbol),1,3) = '002' then '02' \n" +
                "when substr(trim(b.symbol),1,3) = '300' then '03'\n" +
                "when substr(trim(b.symbol),1,1) = '0' \n" +
                "or substr(trim(b.symbol),1,1) = '2' \n" +
                "or substr(trim(b.symbol),1,1) = '6' \n" +
                "or substr(trim(b.symbol),1,1) = '9' \n" +
                "then '01'\n" +
                "when substr(trim(b.symbol),1,1) = '4'\n" +
                "or substr(trim(b.symbol),1,1) = '8'\n" +
                "then '04'" +
                "else '-9'end as ee0034 ,\n" +
                "case when  b.`exchange` <> '' \n" +
                "and b.`exchange` is not null \n" +
                "and b.`exchange` <> ' null' \n" +
                "then b.`exchange` else '-9' end as ee0036,\n" +
                "case when  b.setype <> '' \n" +
                "and b.setype is not null \n" +
                "and b.setype <> ' null' \n" +
                "then b.setype else '-9' end as ee0037\n" +
                "from tmp01 a \n" +
                "join \n" +
                "TQ_SK_BASICINFO b \n" +
                "on a.compcode = b.compcode \n" +
                "where b.liststatus = 1";

        return DataFrameUtil.getDataFrame(spark, hql, "tmp02");


    }

    public static Dataset getListedInformation(SparkSession spark) {

        tmp01(spark);

        return tmp02(spark);
    }


}

