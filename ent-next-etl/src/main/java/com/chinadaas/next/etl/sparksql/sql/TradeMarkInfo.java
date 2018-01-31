package com.chinadaas.next.etl.sparksql.sql;

import com.chinadaas.common.util.DataFrameUtil;
import com.chinadaas.common.util.TimeUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * @author haoxing
 * 公司商标指标的计算。优化merge
 */
public class TradeMarkInfo {

    private static Dataset tmp01(SparkSession spark, String datadate) {
        String hql = "select \n" +
                "b.pripid,\n" +
                "count(1) as eb0049\n" +
                "from enterprisebaseinfocollect b\n" +//enterprisebaseinfocollect_full_datadate
                "inner join s_en_trademarkinfo a ON b.pripid=a.pripid_1 AND b.s_ext_nodenum=a.nodenum_1\n" +
                "group by b.pripid";

        return DataFrameUtil.getDataFrame(spark, hql.replaceAll("datadate", datadate), "tmp01");
    }

    private static Dataset tmp02(SparkSession spark, String datadate) {
        String hql = "select \n" +
                "b.s_ext_nodenum,b.pripid,\n" +
                "count(1) as eb0050,\n" +
                "sum(case when regexp_replace(substr(a.begindate,1,10),'-','')>regexp_replace('" + TimeUtil.getYearAgo(1) + "','-','') then 1 else 0 end) as eb0047,\n" +
                "sum(case when regexp_replace(substr(a.begindate,1,10),'-','')>regexp_replace('" + TimeUtil.getYearAgo(3) + "','-','') then 1 else 0 end) as eb0048\n" +
                "from enterprisebaseinfocollect b \n" +
                "inner join s_en_trademarkinfo a ON b.pripid=a.pripid_1 AND b.s_ext_nodenum=a.nodenum_1\n" +
                "where a.begindate is not null \n" +
                "and nvl(floor(regexp_replace(substr('" + TimeUtil.getNowStr() + "',1,10),'-','') - regexp_replace(substr(a.begindate,1,10),'-','')),0)>=0 \n" +
                "and a.pripid_1 is not null\n" +
                "and a.isinvalid = '0' \n" +
                "and (\n" +
                "mark_reg_status='变更商标代理人完成'\n" +
                "or mark_reg_status='变更商标申请人/注册人名义/地址完成'\n" +
                "or mark_reg_status='变更完成'\n" +
                "or mark_reg_status='驳回复审完成'\n" +
                "or mark_reg_status='驳回复审注册公告排版完成'\n" +
                "or mark_reg_status='补变转续证明完成'\n" +
                "or mark_reg_status='补发变更/转让/续展证明申请完成'\n" +
                "or mark_reg_status='补发商标注册证完成'\n" +
                "or mark_reg_status='补发注册证完成'\n" +
                "or mark_reg_status='撤回撤销三年不使用申请完成'\n" +
                "or mark_reg_status='撤销注册复审完成'\n" +
                "or mark_reg_status='撤销注册商标复审完成'\n" +
                "or mark_reg_status='出具商标注册证明完成'\n" +
                "or mark_reg_status='更正商标申请/注册事项完成'\n" +
                "or mark_reg_status='更正审查完成'\n" +
                "or mark_reg_status='国际变更完成'\n" +
                "or mark_reg_status='国际部分转让完成'\n" +
                "or mark_reg_status='国际合并完成'\n" +
                "or mark_reg_status='国际领土延伸完成'\n" +
                "or mark_reg_status='国际删减完成'\n" +
                "or mark_reg_status='国际续展完成'\n" +
                "or mark_reg_status='解冻完成'\n" +
                "or mark_reg_status='开具注册证明完成'\n" +
                "or mark_reg_status='领土延伸完成'\n" +
                "or mark_reg_status='删减商品/服务项目申请完成'\n" +
                "or mark_reg_status='商标变更完成'\n" +
                "or mark_reg_status='商标更正审查完成'\n" +
                "or mark_reg_status='商标使用许可备案完成'\n" +
                "or mark_reg_status='商标续展完成'\n" +
                "or mark_reg_status='商标注册申请完成'\n" +
                "or mark_reg_status='商标注册申请注册公告排版完成'\n" +
                "or mark_reg_status='许可合同备案完成'\n" +
                "or mark_reg_status='许可合同变更完成'\n" +
                "or mark_reg_status='续展完成'\n" +
                "or mark_reg_status='异议完成'\n" +
                "or mark_reg_status='争议完成')\n" +
                "group by b.s_ext_nodenum,b.pripid";

        return DataFrameUtil.getDataFrame(spark, hql.replaceAll("datadate", datadate), "tmp02");


    }

    private static Dataset tmp03(SparkSession spark, String datadate) {

        String hql = "select a.pripid,a.eb0049 as eb0049,b.eb0050 as eb0050,b.eb0047 as eb0047,b.eb0048 as eb0048 from tmp01 a left join tmp02 b on a.pripid = b.pripid";

        return DataFrameUtil.getDataFrame(spark, hql.replaceAll("datadate", datadate), "tmp03");


    }

    public static Dataset getTradeMarkInfo(SparkSession spark,String datadate) {

        tmp01(spark, datadate);
        tmp02(spark, datadate);


        return tmp03(spark, datadate);

    }


}
