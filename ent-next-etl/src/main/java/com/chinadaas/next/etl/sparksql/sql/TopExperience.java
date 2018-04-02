package com.chinadaas.next.etl.sparksql.sql;

import com.chinadaas.common.util.DataFrameUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class TopExperience {
    public static Dataset tmp01(SparkSession spark, String datadate) {

        String hql = "select distinct a.pripid,d.pripid y,d.entstatus\n" +
                "from enterprisebaseinfocollect a\n" +
                "join e_pri_person b on a.pripid = b.pripid and b.lerepsign <> '1'\n" +
                "join e_pri_person c on b.zspid = c.zspid \n" +
                "and b.zspid is not null and b.zspid <> '' and b.zspid <> 'null'\n" +
                "and c.zspid is not null and c.zspid <> '' and c.zspid <> 'null' and c.name = b.name\n" +
                "join enterprisebaseinfocollect d on c.pripid = d.pripid\n" +
                "where d.entname <> a.entname";

        return DataFrameUtil.getDataFrame(spark, hql.replace("datadate", datadate), "tmp01");

    }

    public static Dataset tmp02(SparkSession spark, String datadate) {

        String hql = "select pripid,\n" +
                "case when count(y) > 0 then 'y' when count(y) = 0 then 'n' else 'n' end as eb0066,\n" +
                "sum(case when entstatus = '1' then 1 else 0 end) as eb0067,\n" +
                "sum(case when entstatus <> '1' \n" +
                "then 1 else 0 end) as eb0068\n" +
                "from tmp01\n" +
                "group by pripid";

        return DataFrameUtil.getDataFrame(spark, hql.replace("datadate", datadate), "tmp02");

    }

    public static Dataset getTopExperienceKpi(SparkSession spark, String datadate) {

        tmp01(spark, datadate);
        return tmp02(spark, datadate);

    }


}
