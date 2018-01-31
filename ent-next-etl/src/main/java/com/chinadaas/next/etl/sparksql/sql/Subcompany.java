package com.chinadaas.next.etl.sparksql.sql;

import com.chinadaas.common.util.DataFrameUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * @author haoxing
 * 子公司的指标计算，优化merge
 */
public class Subcompany {
    public static Dataset Subcompany(SparkSession spark,String datadate){

        String hql = "select a.pripid,a.eb0022 as eb0022,b.eb0023 as eb0023 from zgs a left join nowsubsidiarykpitemp b on a.pripid = b.pripid";

        return DataFrameUtil.getDataFrame(spark,hql.replaceAll("datadate",datadate),"tmpsubconpany");


    }
    public static Dataset Subsidiary(SparkSession spark,String datadate){

        String hql = "select c.pripid,count(1) as eb0022 from enterprisebaseinfocollect a\n" +
                "join e_inv_investment b\n" +
                "on a.pripid = b.pripid\n" +
                "join enterprisebaseinfocollect c\n" +
                "on c.entname = b.inv\n" +
                "where a.regcap = b.subconam\n" +
                "and nvl(b.subconam,0)!=0\n" +
                "group by c.pripid";

        return DataFrameUtil.getDataFrame(spark,hql.replaceAll("datadate",datadate),"zgs");
    }

    public static Dataset getNowSubsidiaryKpiE (SparkSession spark,String datadate){
        String hql = "select c.pripid,count(1) as eb0023 from enterprisebaseinfocollect a\n" + //enterprisebaseinfocollect_full_datadate
                "join e_inv_investment b\n" +
                "on a.pripid = b.pripid\n" +
                "join enterprisebaseinfocollect c\n" +//enterprisebaseinfocollect_full_datadate
                "on c.entname = b.inv\n" +
                "where a.regcap = b.subconam\n" +
                "and nvl(b.subconam,0)!=0\n" +
                "and a.entstatus = '1'\n" + //状态为在营
                "group by c.pripid";

        return DataFrameUtil.getDataFrame(spark,hql.replaceAll("datadate",datadate),"nowsubsidiarykpitemp");

    }


    public static Dataset getSubcompany(SparkSession spark,String datadate){
        Subsidiary(spark,datadate);
        getNowSubsidiaryKpiE(spark, datadate);



        return Subcompany(spark,datadate);


    }
}
