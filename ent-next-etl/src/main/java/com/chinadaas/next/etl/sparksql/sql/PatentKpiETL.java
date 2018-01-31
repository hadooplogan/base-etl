package com.chinadaas.next.etl.sparksql.sql;

import com.chinadaas.common.util.DataFrameUtil;
import com.chinadaas.common.util.TimeUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * @author haoxing
 * 2018—1-5号逻辑
 * 专利相关标签计算。
 */
public class PatentKpiETL {


    //历史申请专利(含无效)
    private static Dataset HistoryApplicant(SparkSession spark) {

        String hql = "select a.pripid ,\n" +
                "count(1) as eb0061 \n" +
                "from enterprisebaseinfocollect a \n" +
                "join s_sipo_patent_copyright b \n" +
                "on a.pripid = b.pt_reg_group_pripid \n" +
                "join s_sipo_patent_info c \n" +
                "on b.pt_reg_no = c.pt_reg_no and b.pt_typeno = c.pt_typeno\n" +
                "group by a.pripid ";

        return DataFrameUtil.getDataFrame(spark, hql, "tmphistorypatent");

    }
    //历史累计专利申请数（有效数量）经过多重逻辑筛选

    private static Dataset getAll(SparkSession spark) {

        String hql = "select a.pripid,\n" +
                "c.PT_TYPENO,\n" +
                "regexp_replace(substr(c.pt_reg_date,1,10),'-','') as pt_reg_date ,\n" +
                "c.PT_REG_NO from enterprisebaseinfocollect a \n" +
                "join s_sipo_patent_copyright b on a.pripid = b.pt_reg_group_pripid \n" +
                "join s_sipo_patent_info c on b.pt_reg_no = c.pt_reg_no and b.pt_typeno = c.pt_typeno";

        return DataFrameUtil.getDataFrame(spark, hql, "tmpall");

    }

    private static Dataset getDateFit(SparkSession spark) {

        String hql = "select pripid,\n" +
                "pt_typeno,\n" +
                "pt_reg_date,\n" +
                "pt_reg_no,\n" +
                "case when pt_typeno = '11' and (pt_reg_date + 200000) > regexp_replace('" + TimeUtil.getNowYMDStr() + "','-','') then '1' \n" +
                "when pt_typeno = '12' and (pt_reg_date + 200000) >regexp_replace('" + TimeUtil.getNowYMDStr() + "','-','') then '1'\n" +
                "when pt_typeno = '20' and (pt_reg_date + 100000) >regexp_replace('" + TimeUtil.getNowYMDStr() + "','-','') then '1'\n" +
                "when pt_reg_date = '30' and (pt_reg_date + 100000) >regexp_replace('" + TimeUtil.getNowYMDStr() + "','-','') then '1'\n" +
                "else '0' end as tag from tmpall";

        return DataFrameUtil.getDataFrame(spark, hql, "tmpdatefit");

    }


    private static Dataset getShouQuan(SparkSession spark) {

        String hql = "select a.pripid,\n" +
                "a.pt_typeno,\n" +
                "a.pt_reg_date,\n" +
                "a.pt_reg_no,\n" +
                "b.lawstate,\n" +
                "b.lawstate_date \n" +
                "from tmpdatefit a \n" +
                "join s_sipo_patent_lawstate b \n" +
                "on a.pt_reg_no = b.pt_reg_no \n" +
                "where b.lawstate = '授权' and a.tag = '1'";

        return DataFrameUtil.getDataFrame(spark, hql, "tmpshouquan");

    }

    private static Dataset getRanked(SparkSession spark) {

        String hql = "select *,\n" +
                " row_number() over(partition by pt_reg_no,pt_typeno order by lawstate_date) as rank \n" +
                "from tmpshouquan";

        return DataFrameUtil.getDataFrame(spark, hql, "tmpranked");

    }

    private static Dataset getNew(SparkSession spark) {

        String hql = "select * from tmpranked where rank = 1";

        return DataFrameUtil.getDataFrame(spark, hql, "tmpnew");
    }

    private static Dataset getOk(SparkSession spark) {

        String hql = "select * from tmpnew \n" +
                "where lawstate <>'避免重复授权放弃专利权' and \n" +
                "lawstate <>'避免重复授予专利权' and \n" +
                "lawstate <>'其他有关事项(避免重复授权放弃专利权)'and\n" +
                "lawstate <>'其他有关事项(专利权全部撤销)'and\n" +
                "lawstate <>'其他有关事项被视为放弃专利权的权利'and\n" +
                "lawstate <>'其他有关事项避免重复授权放弃专利权'and\n" +
                "lawstate <>'其他有关事项视为放弃专利权及自行放弃专利权'and\n" +
                "lawstate <>'其他有关事项视为放弃专利权及自行放弃专利权公布内容'and\n" +
                "lawstate <>'其他有关事项专利权全部撤销'and\n" +
                "lawstate <>'专利权的撤销(专利权的全部撤销)'and\n" +
                "lawstate <>'专利权的撤销(专利权全部撤销)'and\n" +
                "lawstate <>'专利权的撤销专利权全部撤销'and\n" +
                "lawstate <>'专利权的视为放弃'and\n" +
                "lawstate <>'专利权的无效宣告(①专利权全部无效)'and\n" +
                "lawstate <>'专利权的终止'and\n" +
                "lawstate <>'专利权的终止  未缴年费专利权终止'and\n" +
                "lawstate <>'专利权的终止((1)未缴年费专利权终止)'and\n" +
                "lawstate <>'专利权的终止((2)专利权有效期届满)'and\n" +
                "lawstate <>'专利权的终止(①未缴年费专利权终止)'and\n" +
                "lawstate <>'专利权的终止(①专利权的自动放弃)'and\n" +
                "lawstate <>'专利权的终止(①专利权有效期届满)'and\n" +
                "lawstate <>'专利权的终止(②未缴年费专利权终止)'and\n" +
                "lawstate <>'专利权的终止(②专利权有效期届满)'and\n" +
                "lawstate <>'专利权的终止(③专利权有效期届满)'and\n" +
                "lawstate <>'专利权的终止(未缴纳年费专利权终止)'and\n" +
                "lawstate <>'专利权的终止(未缴年费专利权终止)'and\n" +
                "lawstate <>'专利权的终止(专利权的主动放弃)'and\n" +
                "lawstate <>'专利权的终止(专利权有效期届满)'and\n" +
                "lawstate <>'专利权的终止未缴纳年费专利权终止'and\n" +
                "lawstate <>'专利权的终止未缴年费专利权终止'and\n" +
                "lawstate <>'专利权的终止主动放弃专利权的专利'and\n" +
                "lawstate <>'专利权的终止专利权的主动放弃'and\n" +
                "lawstate <>'专利权的终止专利权的自动放弃'and\n" +
                "lawstate <>'专利权的终止专利权人放弃专利权'and\n" +
                "lawstate <>'专利权的终止专利权有效期届满'and\n" +
                "lawstate <>'专利权的主动放弃'";

        return DataFrameUtil.getDataFrame(spark, hql, "tmpok");
    }

    private static Dataset getHistoryApplicant(SparkSession spark) {

        String hql = "select pripid, count(1) as eb0062 from tmpok\n" +
                "group by pripid having count(1) > 0";

        return DataFrameUtil.getDataFrame(spark, hql, "tmpcount");

    }

    //筛选出一年内的的申请专利
    private static Dataset getAll1(SparkSession spark) {

        String hql = "select a.pripid,\n" +
                "c.PT_TYPENO,\n" +
                "regexp_replace(substr(c.pt_reg_date,1,10),'-','') as pt_reg_date ,\n" +
                "c.PT_REG_NO from enterprisebaseinfocollect a \n" +
                "join s_sipo_patent_copyright b on a.pripid = b.pt_reg_group_pripid \n" +
                "join s_sipo_patent_info c on b.pt_reg_no = c.pt_reg_no and b.pt_typeno = c.pt_typeno\n" +
                "where regexp_replace(substr(c.pt_reg_date,1,10),'-','') >  regexp_replace('" + TimeUtil.getYearAgo(1) + "','-','') ";

        return DataFrameUtil.getDataFrame(spark, hql, "tmpall1");

    }

    private static Dataset getDateFit1(SparkSession spark) {

        String hql = "select pripid,\n" +
                "pt_typeno,\n" +
                "pt_reg_date,\n" +
                "pt_reg_no,\n" +
                "case when pt_typeno = '11' and (pt_reg_date + 200000) > regexp_replace('" + TimeUtil.getNowYMDStr() + "','-','') then '1' \n" +
                "when pt_typeno = '12' and (pt_reg_date + 200000) >regexp_replace('" + TimeUtil.getNowYMDStr() + "','-','') then '1'\n" +
                "when pt_typeno = '20' and (pt_reg_date + 100000) >regexp_replace('" + TimeUtil.getNowYMDStr() + "','-','') then '1'\n" +
                "when pt_reg_date = '30' and (pt_reg_date + 100000) >regexp_replace('" + TimeUtil.getNowYMDStr() + "','-','') then '1'\n" +
                "else '0' end as tag from  tmpall1";

        return DataFrameUtil.getDataFrame(spark, hql, "tmpdatefit1");

    }


    private static Dataset getShouQuan1(SparkSession spark) {

        String hql = "select a.pripid,\n" +
                "a.pt_typeno,\n" +
                "a.pt_reg_date,\n" +
                "a.pt_reg_no,\n" +
                "b.lawstate,\n" +
                "b.lawstate_date \n" +
                "from tmpdatefit1 a \n" +
                "join s_sipo_patent_lawstate b \n" +
                "on a.pt_reg_no = b.pt_reg_no \n" +
                "where b.lawstate = '授权' and a.tag = '1'";

        return DataFrameUtil.getDataFrame(spark, hql, "tmpshouquan1");

    }

    private static Dataset getRanked1(SparkSession spark) {

        String hql = "select *,\n" +
                " row_number() over(partition by pt_reg_no,pt_typeno order by lawstate_date) as rank \n" +
                "from tmpshouquan1";

        return DataFrameUtil.getDataFrame(spark, hql, "tmpranked1");

    }

    private static Dataset getNew1(SparkSession spark) {

        String hql = "select * from tmpranked1 where rank = 1";

        return DataFrameUtil.getDataFrame(spark, hql, "tmpnew1");
    }

    private static Dataset getOk1(SparkSession spark) {

        String hql = "select * from tmpnew1 \n" +
                "where lawstate <>'避免重复授权放弃专利权' and \n" +
                "lawstate <>'避免重复授予专利权' and \n" +
                "lawstate <>'其他有关事项(避免重复授权放弃专利权)'and\n" +
                "lawstate <>'其他有关事项(专利权全部撤销)'and\n" +
                "lawstate <>'其他有关事项被视为放弃专利权的权利'and\n" +
                "lawstate <>'其他有关事项避免重复授权放弃专利权'and\n" +
                "lawstate <>'其他有关事项视为放弃专利权及自行放弃专利权'and\n" +
                "lawstate <>'其他有关事项视为放弃专利权及自行放弃专利权公布内容'and\n" +
                "lawstate <>'其他有关事项专利权全部撤销'and\n" +
                "lawstate <>'专利权的撤销(专利权的全部撤销)'and\n" +
                "lawstate <>'专利权的撤销(专利权全部撤销)'and\n" +
                "lawstate <>'专利权的撤销专利权全部撤销'and\n" +
                "lawstate <>'专利权的视为放弃'and\n" +
                "lawstate <>'专利权的无效宣告(①专利权全部无效)'and\n" +
                "lawstate <>'专利权的终止'and\n" +
                "lawstate <>'专利权的终止  未缴年费专利权终止'and\n" +
                "lawstate <>'专利权的终止((1)未缴年费专利权终止)'and\n" +
                "lawstate <>'专利权的终止((2)专利权有效期届满)'and\n" +
                "lawstate <>'专利权的终止(①未缴年费专利权终止)'and\n" +
                "lawstate <>'专利权的终止(①专利权的自动放弃)'and\n" +
                "lawstate <>'专利权的终止(①专利权有效期届满)'and\n" +
                "lawstate <>'专利权的终止(②未缴年费专利权终止)'and\n" +
                "lawstate <>'专利权的终止(②专利权有效期届满)'and\n" +
                "lawstate <>'专利权的终止(③专利权有效期届满)'and\n" +
                "lawstate <>'专利权的终止(未缴纳年费专利权终止)'and\n" +
                "lawstate <>'专利权的终止(未缴年费专利权终止)'and\n" +
                "lawstate <>'专利权的终止(专利权的主动放弃)'and\n" +
                "lawstate <>'专利权的终止(专利权有效期届满)'and\n" +
                "lawstate <>'专利权的终止未缴纳年费专利权终止'and\n" +
                "lawstate <>'专利权的终止未缴年费专利权终止'and\n" +
                "lawstate <>'专利权的终止主动放弃专利权的专利'and\n" +
                "lawstate <>'专利权的终止专利权的主动放弃'and\n" +
                "lawstate <>'专利权的终止专利权的自动放弃'and\n" +
                "lawstate <>'专利权的终止专利权人放弃专利权'and\n" +
                "lawstate <>'专利权的终止专利权有效期届满'and\n" +
                "lawstate <>'专利权的主动放弃'";

        return DataFrameUtil.getDataFrame(spark, hql, "tmpok1");
    }

    private static Dataset getHistoryApplicant1(SparkSession spark) {

        String hql = "select pripid, count(1) as eb0059 from tmpok1\n" +
                "group by pripid having count(1) > 0";

        return DataFrameUtil.getDataFrame(spark, hql, "tmpcount1");

    }

    //三年内的专利

    private static Dataset getAll3(SparkSession spark) {

        String hql = "select a.pripid,\n" +
                "c.PT_TYPENO,\n" +
                "regexp_replace(substr(c.pt_reg_date,1,10),'-','') as pt_reg_date ,\n" +
                "c.PT_REG_NO from enterprisebaseinfocollect a \n" +
                "join  s_sipo_patent_copyright b on a.pripid = b.pt_reg_group_pripid \n" +
                "join s_sipo_patent_info c on b.pt_reg_no = c.pt_reg_no and b.pt_typeno = c.pt_typeno\n" +
                "where regexp_replace(substr(c.pt_reg_date,1,10),'-','') >  regexp_replace('" + TimeUtil.getYearAgo(3) + "','-','') ";

        return DataFrameUtil.getDataFrame(spark, hql, "tmpall3");

    }

    private static Dataset getDateFit3(SparkSession spark) {

        String hql = "select pripid,\n" +
                "pt_typeno,\n" +
                "pt_reg_date,\n" +
                "pt_reg_no,\n" +
                "case when pt_typeno = '11' and (pt_reg_date + 200000) > regexp_replace('" + TimeUtil.getNowYMDStr() + "','-','') then '1' \n" +
                "when pt_typeno = '12' and (pt_reg_date + 200000) > regexp_replace('" + TimeUtil.getNowYMDStr() + "','-','') then '1'\n" +
                "when pt_typeno = '20' and (pt_reg_date + 100000) > regexp_replace('" + TimeUtil.getNowYMDStr() + "','-','') then '1'\n" +
                "when pt_reg_date = '30' and (pt_reg_date + 100000) > regexp_replace('" + TimeUtil.getNowYMDStr() + "','-','') then '1'\n" +
                "else '0' end as tag from  tmpall3";

        return DataFrameUtil.getDataFrame(spark, hql, "tmpdatefit3");

    }


    private static Dataset getShouQuan3(SparkSession spark) {

        String hql = "select a.pripid,\n" +
                "a.pt_typeno,\n" +
                "a.pt_reg_date,\n" +
                "a.pt_reg_no,\n" +
                "b.lawstate,\n" +
                "b.lawstate_date \n" +
                "from tmpdatefit3 a \n" +
                "join s_sipo_patent_lawstate b \n" +
                "on a.pt_reg_no = b.pt_reg_no \n" +
                "where b.lawstate = '授权' and a.tag = '1'";

        return DataFrameUtil.getDataFrame(spark, hql, "tmpshouquan3");

    }

    private static Dataset getRanked3(SparkSession spark) {

        String hql = "select *,\n" +
                " row_number() over(partition by pt_reg_no,pt_typeno order by lawstate_date) as rank \n" +
                "from tmpshouquan3";

        return DataFrameUtil.getDataFrame(spark, hql, "tmpranked3");

    }

    private static Dataset getNew3(SparkSession spark) {

        String hql = "select * from tmpranked3 where rank = 1";

        return DataFrameUtil.getDataFrame(spark, hql, "tmpnew3");
    }

    private static Dataset getOk3(SparkSession spark) {

        String hql = "select * from tmpnew3 \n" +
                "where lawstate <>'避免重复授权放弃专利权' and \n" +
                "lawstate <>'避免重复授予专利权' and \n" +
                "lawstate <>'其他有关事项(避免重复授权放弃专利权)'and\n" +
                "lawstate <>'其他有关事项(专利权全部撤销)'and\n" +
                "lawstate <>'其他有关事项被视为放弃专利权的权利'and\n" +
                "lawstate <>'其他有关事项避免重复授权放弃专利权'and\n" +
                "lawstate <>'其他有关事项视为放弃专利权及自行放弃专利权'and\n" +
                "lawstate <>'其他有关事项视为放弃专利权及自行放弃专利权公布内容'and\n" +
                "lawstate <>'其他有关事项专利权全部撤销'and\n" +
                "lawstate <>'专利权的撤销(专利权的全部撤销)'and\n" +
                "lawstate <>'专利权的撤销(专利权全部撤销)'and\n" +
                "lawstate <>'专利权的撤销专利权全部撤销'and\n" +
                "lawstate <>'专利权的视为放弃'and\n" +
                "lawstate <>'专利权的无效宣告(①专利权全部无效)'and\n" +
                "lawstate <>'专利权的终止'and\n" +
                "lawstate <>'专利权的终止  未缴年费专利权终止'and\n" +
                "lawstate <>'专利权的终止((1)未缴年费专利权终止)'and\n" +
                "lawstate <>'专利权的终止((2)专利权有效期届满)'and\n" +
                "lawstate <>'专利权的终止(①未缴年费专利权终止)'and\n" +
                "lawstate <>'专利权的终止(①专利权的自动放弃)'and\n" +
                "lawstate <>'专利权的终止(①专利权有效期届满)'and\n" +
                "lawstate <>'专利权的终止(②未缴年费专利权终止)'and\n" +
                "lawstate <>'专利权的终止(②专利权有效期届满)'and\n" +
                "lawstate <>'专利权的终止(③专利权有效期届满)'and\n" +
                "lawstate <>'专利权的终止(未缴纳年费专利权终止)'and\n" +
                "lawstate <>'专利权的终止(未缴年费专利权终止)'and\n" +
                "lawstate <>'专利权的终止(专利权的主动放弃)'and\n" +
                "lawstate <>'专利权的终止(专利权有效期届满)'and\n" +
                "lawstate <>'专利权的终止未缴纳年费专利权终止'and\n" +
                "lawstate <>'专利权的终止未缴年费专利权终止'and\n" +
                "lawstate <>'专利权的终止主动放弃专利权的专利'and\n" +
                "lawstate <>'专利权的终止专利权的主动放弃'and\n" +
                "lawstate <>'专利权的终止专利权的自动放弃'and\n" +
                "lawstate <>'专利权的终止专利权人放弃专利权'and\n" +
                "lawstate <>'专利权的终止专利权有效期届满'and\n" +
                "lawstate <>'专利权的主动放弃'";

        return DataFrameUtil.getDataFrame(spark, hql, "tmpok3");
    }

    private static Dataset getHistoryApplicant3(SparkSession spark) {

        String hql = "select pripid, count(1) as eb0060 from tmpok3\n" +
                "group by pripid having count(1) > 0";

        return DataFrameUtil.getDataFrame(spark, hql, "tmpcount3");

    }

    private static Dataset getPatent(SparkSession spark) {

        String hql = "select a.pripid,\n" +
                "a.eb0061,\n" +
                "b.eb0062,\n" +
                "c.eb0060,\n" +
                "d.eb0059,\n" +
                "case when b.eb0062 is null then '0' \n" +
                "when b.eb0062 is not null then c.eb0060/b.eb0062 end as eb0112 from \n" +
                "tmphistorypatent a \n" +
                "left join tmpcount b on a.pripid = b.pripid\n" +
                "left join tmpcount3 c on a.pripid = c.pripid\n" +
                "left join tmpcount1 d on a.pripid = d.pripid";

        return DataFrameUtil.getDataFrame(spark, hql, "tmppatentkpi");

    }

    //得到专利标签
    public static Dataset getPatentKpi(SparkSession spark) {
        //历史申请专利 含无效
        HistoryApplicant(spark);
        //历史申请专利 含无效
        getAll(spark);
        getDateFit(spark);
        getShouQuan(spark);
        getRanked(spark);
        getNew(spark);
        getOk(spark);
        getHistoryApplicant(spark);
        //历史申请专利 一年内
        getAll1(spark);
        getDateFit1(spark);
        getShouQuan1(spark);
        getRanked1(spark);
        getNew1(spark);
        getOk1(spark);
        getHistoryApplicant1(spark);
        //历史申请专利 三年内
        getAll3(spark);
        getDateFit3(spark);
        getShouQuan3(spark);
        getRanked3(spark);
        getNew3(spark);
        getOk3(spark);
        getHistoryApplicant3(spark);

        //merge专利标签数据
        return getPatent(spark);


    }

}
