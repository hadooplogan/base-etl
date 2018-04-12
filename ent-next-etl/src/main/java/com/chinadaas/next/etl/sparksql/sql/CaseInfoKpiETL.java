package com.chinadaas.next.etl.sparksql.sql;

import com.chinadaas.common.util.DataFrameUtil;
import com.chinadaas.common.util.TimeUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * @Auther: zhouzhen@chinadaas.com
 * @description:
 * @Date: 10:19 2017/9/12
 *  行政处罚标签
 *  增加eb0107,就是判断eb0101是否大于0，如果大于0 则为是，否则为否
 */
public class CaseInfoKpiETL {

    public static Dataset getCaseInfoKpi(SparkSession spark, String datadate){
        String hql = "select d.pripid, d.total as eb0101,case when d.total > 0 then 'y' else 'n' end as eb0107,  e.threetotal as eb0100 from " +
                "(select c.pripid,count(1) as total from" +
                "  (select a.PUBLICDATE,b.pripid from s_en_casebaseinfo a " +
                "     left join s_en_casepartyinfo b on a.CASEID = b.CASEID where pripid <> '' " +
                "   )c group by c.pripid" +
                ")d left join " +
                "(select c.pripid,count(1) as threetotal from" +
                "  (select a.PUBLICDATE,b.pripid from s_en_casebaseinfo a " +
                "    left join s_en_casepartyinfo b on a.CASEID = b.CASEID where pripid <> '' and regexp_replace(substr(a.PUBLICDATE,1,10),'-','') >= regexp_replace(" + TimeUtil.getYearAgo(3) +",'-','')"+
                "   )c group by c.pripid" +
                ")e on d.pripid = e.pripid";

        return DataFrameUtil.getDataFrame(spark, hql.replaceAll("datadate",datadate),"CaseKpiTmp");
    }
}

