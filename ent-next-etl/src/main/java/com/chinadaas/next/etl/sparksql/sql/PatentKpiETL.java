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


    //历史申请专利(含无效) 经过去重后 统计专利,去重不考虑 type
    private static Dataset HistoryApplicant(SparkSession spark) {

        String hql =  "select pripid,count(1) as eb0061 from (select distinct a.pripid ,\n" +
                "c.pt_reg_no \n" +
                "from enterprisebaseinfocollect a \n" +
                "join s_sipo_patent_copyright b \n" +
                "on a.pripid = b.pt_reg_group_pripid \n" +
                "join s_sipo_patent_info c \n" +
                "on b.pt_reg_no = c.pt_reg_no and b.pt_typeno = c.pt_typeno) group by pripid";

        return DataFrameUtil.getDataFrame(spark, hql, "tmphistorypatent");

    }
    //历史累计专利申请数（有效数量）经过多重逻辑筛选


    //拿到所有的法律状态
    private static Dataset getAll(SparkSession spark) {

        String hql = "select a.pripid,\n" +
                "c.PT_TYPENO,\n" +
                "d.lawstate, \n" +
                "regexp_replace(substr(trim(c.pt_reg_date),1,10),'-','') as pt_reg_date ,\n" +
                "c.PT_REG_NO from enterprisebaseinfocollect a \n" +
                "join s_sipo_patent_copyright b on a.pripid = b.pt_reg_group_pripid \n" +
                "join s_sipo_patent_info c on b.pt_reg_no = c.pt_reg_no and b.pt_typeno = c.pt_typeno \n" +
                "join s_sipo_patent_lawstate d on b.pt_reg_no = d.pt_reg_no";

        return DataFrameUtil.getDataFrame(spark, hql, "tmpall");

    }

    //拿到所有符合时间的法律状态
    private static Dataset getDateFit(SparkSession spark) {

        String hql = "select pripid,\n" +
                "pt_typeno,\n" +
                "pt_reg_date,\n" +
                "pt_reg_no,\n" +
                "lawstate, \n" +
                "case when pt_typeno = '11' and (pt_reg_date + 200000) >= regexp_replace("+TimeUtil.getNowYMDStr()+",'-','') then '1' \n" +
                "when pt_typeno = '12' and (pt_reg_date + 200000) >= regexp_replace("+TimeUtil.getNowYMDStr()+",'-','') then '1'\n" +
                "when pt_typeno = '20' and (pt_reg_date + 100000) >= regexp_replace("+TimeUtil.getNowYMDStr()+",'-','') then '1'\n" +
                "when pt_typeno = '30' and (pt_reg_date + 100000) >= regexp_replace("+TimeUtil.getNowYMDStr()+",'-','') then '1'\n" +
                "else '0' end as tag from tmpall";

        return DataFrameUtil.getDataFrame(spark, hql, "tmpdatefit");

    }



    private static Dataset lawstate(SparkSession spark) {

        String hql = "select a.pripid,\n" +
                "a.pt_typeno,\n" +
                "a.pt_reg_date,\n" +
                "a.pt_reg_no,\n" +
                "b.lawstate,\n" +
                "b.lawstate_date \n" +
                "from tmpdatefit a \n" +
                "join s_sipo_patent_lawstate b \n" +
                "on a.pt_reg_no = b.pt_reg_no where tag = 1";


        return DataFrameUtil.getDataFrame(spark, hql, "tmplawstate");

    }


    //拿到出现授权的数据的专利号
    private static Dataset getShouquan(SparkSession spark) {
        String hql = "select pripid,pt_typeno, pt_reg_no,pt_reg_date,lawstate,lawstate_date from tmplawstate where lawstate = '授权'";


        return DataFrameUtil.getDataFrame(spark, hql, "tmpshouquan");

    }


     //拿到最新时间的法律状态
    private static Dataset getRanked(SparkSession spark) {

        String hql = "select * from (select pripid,\n" +
                "pt_typeno,\n" +
                "pt_reg_date,\n" +
                "pt_reg_no,\n" +
                "lawstate,\n" +
                "lawstate_date, \n" +
                "max(trim(lawstate_date)) over (partition by pt_reg_no) as maxlaw_date \n" +
                "from tmplawstate) \n" +
                "where maxlaw_date  = lawstate_date";


        return DataFrameUtil.getDataFrame(spark, hql, "tmpranked");

    }

   //关联判断 有效性不存在 专利失效状态
    private static Dataset getOk(SparkSession spark) {

        String hql = "select distinct pripid,pt_reg_no from (select a.pripid,\n" +
                "a.pt_typeno,\n" +
                "a.pt_reg_date,\n" +
                "a.pt_reg_no,\n" +
                "case when \n" +
                "trim(b.lawstate) ='避免重复授权放弃专利权' or \n" +
                "trim(b.lawstate) ='避免重复授予专利权' or \n" +
                "trim(b.lawstate) ='其他有关事项(避免重复授权放弃专利权)'or\n" +
                "trim(b.lawstate) ='其他有关事项(专利权全部撤销)'or\n" +
                "trim(b.lawstate) ='其他有关事项被视为放弃专利权的权利'or\n" +
                "trim(b.lawstate) ='其他有关事项避免重复授权放弃专利权'or\n" +
                "trim(b.lawstate) ='其他有关事项视为放弃专利权及自行放弃专利权'or\n" +
                "trim(b.lawstate) ='其他有关事项视为放弃专利权及自行放弃专利权公布内容'or\n" +
                "trim(b.lawstate) ='其他有关事项专利权全部撤销'or\n" +
                "trim(b.lawstate) ='专利权的撤销(专利权的全部撤销)'or\n" +
                "trim(b.lawstate) ='专利权的撤销(专利权全部撤销)'or\n" +
                "trim(b.lawstate) ='专利权的撤销专利权全部撤销'or\n" +
                "trim(b.lawstate) ='专利权的视为放弃'or\n" +
                "trim(b.lawstate) ='专利权的无效宣告(①专利权全部无效)'or\n" +
                "trim(b.lawstate) ='专利权的终止'or\n" +
                "trim(b.lawstate) ='专利权的终止  未缴年费专利权终止'or\n" +
                "trim(b.lawstate) ='专利权的终止((1)未缴年费专利权终止)'or\n" +
                "trim(b.lawstate) ='专利权的终止((2)专利权有效期届满)'or\n" +
                "trim(b.lawstate) ='专利权的终止(①未缴年费专利权终止)'or\n" +
                "trim(b.lawstate) ='专利权的终止(①专利权的自动放弃)'or\n" +
                "trim(b.lawstate) ='专利权的终止(①专利权有效期届满)'or\n" +
                "trim(b.lawstate) ='专利权的终止(②未缴年费专利权终止)'or\n" +
                "trim(b.lawstate) ='专利权的终止(②专利权有效期届满)'or\n" +
                "trim(b.lawstate) ='专利权的终止(③专利权有效期届满)'or\n" +
                "trim(b.lawstate) ='专利权的终止(未缴纳年费专利权终止)'or\n" +
                "trim(b.lawstate) ='专利权的终止(未缴年费专利权终止)'or\n" +
                "trim(b.lawstate) ='专利权的终止(专利权的主动放弃)'or\n" +
                "trim(b.lawstate) ='专利权的终止(专利权有效期届满)'or\n" +
                "trim(b.lawstate) ='专利权的终止未缴纳年费专利权终止'or\n" +
                "trim(b.lawstate) ='专利权的终止未缴年费专利权终止'or\n" +
                "trim(b.lawstate) ='专利权的终止主动放弃专利权的专利'or\n" +
                "trim(b.lawstate) ='专利权的终止专利权的主动放弃'or\n" +
                "trim(b.lawstate) ='专利权的终止专利权的自动放弃'or\n" +
                "trim(b.lawstate) ='专利权的终止专利权人放弃专利权'or\n" +
                "trim(b.lawstate) ='专利权的终止专利权有效期届满'or\n" +
                "trim(b.lawstate) ='专利权的主动放弃' then '-9' else\n" +
                "a.lawstate end as lawstate,\n" +
                "b.lawstate_date from tmpshouquan a join tmpranked b on a.pt_reg_no = b.pt_reg_no) where lawstate <> '-9'";


        return DataFrameUtil.getDataFrame(spark, hql, "tmpok");
    }


    //统计每个pripid有多少了有效专利
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


    //筛选出申请日期在一年内的数据
    private static Dataset getDateFit1(SparkSession spark) {

        String hql = "select pripid,\n" +
                "pt_typeno,\n" +
                "pt_reg_date,\n" +
                "pt_reg_no,\n" +
                "case when pt_typeno = '11' and (pt_reg_date + 200000) > regexp_replace('" + TimeUtil.getNowYMDStr() + "','-','') then '1' \n" +
                "when pt_typeno = '12' and (pt_reg_date + 200000) >regexp_replace('" + TimeUtil.getNowYMDStr() + "','-','') then '1'\n" +
                "when pt_typeno = '20' and (pt_reg_date + 100000) >regexp_replace('" + TimeUtil.getNowYMDStr() + "','-','') then '1'\n" +
                "when pt_typeno = '30' and (pt_reg_date + 100000) >regexp_replace('" + TimeUtil.getNowYMDStr() + "','-','') then '1'\n" +
                "else '0' end as tag from  tmpall1";

        return DataFrameUtil.getDataFrame(spark, hql, "tmpdatefit1");

    }


    private static Dataset lawstate1 (SparkSession spark) {

        String hql = "select a.pripid,\n" +
                "a.pt_typeno,\n" +
                "a.pt_reg_date,\n" +
                "a.pt_reg_no,\n" +
                "b.lawstate,\n" +
                "b.lawstate_date \n" +
                "from tmpdatefit1 a \n" +
                "join s_sipo_patent_lawstate b \n" +
                "on a.pt_reg_no = b.pt_reg_no \n" +
                "where a.tag = '1'";

        return DataFrameUtil.getDataFrame(spark, hql, "tmplawstate1");

    }


    //拿到一年内的授权数据
    private static Dataset getShouquan1(SparkSession spark){
        String hql = "select pripid,pt_typeno, pt_reg_no,pt_reg_date,lawstate,lawstate_date from tmplawstate1 where lawstate = '授权'";


        return DataFrameUtil.getDataFrame(spark,hql,"tmpshouquan1");
    }

    //拿到历史在一年内有效时间范围内的 最新状态
    private static Dataset getRanked1(SparkSession spark) {

        String hql = "select * from (select pripid,\n" +
                "pt_typeno,\n" +
                "pt_reg_date,\n" +
                "pt_reg_no,\n" +
                "lawstate,\n" +
                "lawstate_date, \n" +
                "max(trim(lawstate_date)) over (partition by pt_reg_no) as maxlaw_date \n" +
                "from tmplawstate1) \n" +
                "where maxlaw_date  = lawstate_date";

        return DataFrameUtil.getDataFrame(spark, hql, "tmpranked1");

    }


    //拿到一年内
    private static Dataset getOk1(SparkSession spark) {

        String hql =  "select distinct pripid,pt_reg_no from (select a.pripid,\n" +
                "a.pt_typeno,\n" +
                "a.pt_reg_date,\n" +
                "a.pt_reg_no,\n" +
                "case when \n" +
                "trim(b.lawstate) ='避免重复授权放弃专利权' or \n" +
                "trim(b.lawstate) ='避免重复授予专利权' or \n" +
                "trim(b.lawstate) ='其他有关事项(避免重复授权放弃专利权)'or\n" +
                "trim(b.lawstate) ='其他有关事项(专利权全部撤销)'or\n" +
                "trim(b.lawstate) ='其他有关事项被视为放弃专利权的权利'or\n" +
                "trim(b.lawstate) ='其他有关事项避免重复授权放弃专利权'or\n" +
                "trim(b.lawstate) ='其他有关事项视为放弃专利权及自行放弃专利权'or\n" +
                "trim(b.lawstate) ='其他有关事项视为放弃专利权及自行放弃专利权公布内容'or\n" +
                "trim(b.lawstate) ='其他有关事项专利权全部撤销'or\n" +
                "trim(b.lawstate) ='专利权的撤销(专利权的全部撤销)'or\n" +
                "trim(b.lawstate) ='专利权的撤销(专利权全部撤销)'or\n" +
                "trim(b.lawstate) ='专利权的撤销专利权全部撤销'or\n" +
                "trim(b.lawstate) ='专利权的视为放弃'or\n" +
                "trim(b.lawstate) ='专利权的无效宣告(①专利权全部无效)'or\n" +
                "trim(b.lawstate) ='专利权的终止'or\n" +
                "trim(b.lawstate) ='专利权的终止  未缴年费专利权终止'or\n" +
                "trim(b.lawstate) ='专利权的终止((1)未缴年费专利权终止)'or\n" +
                "trim(b.lawstate) ='专利权的终止((2)专利权有效期届满)'or\n" +
                "trim(b.lawstate) ='专利权的终止(①未缴年费专利权终止)'or\n" +
                "trim(b.lawstate) ='专利权的终止(①专利权的自动放弃)'or\n" +
                "trim(b.lawstate) ='专利权的终止(①专利权有效期届满)'or\n" +
                "trim(b.lawstate) ='专利权的终止(②未缴年费专利权终止)'or\n" +
                "trim(b.lawstate) ='专利权的终止(②专利权有效期届满)'or\n" +
                "trim(b.lawstate) ='专利权的终止(③专利权有效期届满)'or\n" +
                "trim(b.lawstate) ='专利权的终止(未缴纳年费专利权终止)'or\n" +
                "trim(b.lawstate) ='专利权的终止(未缴年费专利权终止)'or\n" +
                "trim(b.lawstate) ='专利权的终止(专利权的主动放弃)'or\n" +
                "trim(b.lawstate) ='专利权的终止(专利权有效期届满)'or\n" +
                "trim(b.lawstate) ='专利权的终止未缴纳年费专利权终止'or\n" +
                "trim(b.lawstate) ='专利权的终止未缴年费专利权终止'or\n" +
                "trim(b.lawstate) ='专利权的终止主动放弃专利权的专利'or\n" +
                "trim(b.lawstate) ='专利权的终止专利权的主动放弃'or\n" +
                "trim(b.lawstate) ='专利权的终止专利权的自动放弃'or\n" +
                "trim(b.lawstate) ='专利权的终止专利权人放弃专利权'or\n" +
                "trim(b.lawstate) ='专利权的终止专利权有效期届满'or\n" +
                "trim(b.lawstate) ='专利权的主动放弃' then '-9' else\n" +
                "a.lawstate end as lawstate,\n" +
                "b.lawstate_date from tmpshouquan1 a join tmpranked1 b on a.pt_reg_no = b.pt_reg_no) where lawstate <> '-9'";

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
                "when pt_typeno = '30' and (pt_reg_date + 100000) > regexp_replace('" + TimeUtil.getNowYMDStr() + "','-','') then '1'\n" +
                "else '0' end as tag from  tmpall3";

        return DataFrameUtil.getDataFrame(spark, hql, "tmpdatefit3");

    }


    private static Dataset lawstate3(SparkSession spark) {

        String hql = "select a.pripid,\n" +
                "a.pt_typeno,\n" +
                "a.pt_reg_date,\n" +
                "a.pt_reg_no,\n" +
                "b.lawstate,\n" +
                "b.lawstate_date \n" +
                "from tmpdatefit3 a \n" +
                "join s_sipo_patent_lawstate b \n" +
                "on a.pt_reg_no = b.pt_reg_no \n" +
                "where a.tag = '1'";

        return DataFrameUtil.getDataFrame(spark, hql, "tmplawstate2");

    }
    private static Dataset getShouquan3(SparkSession spark){
        String hql = "select pripid,pt_typeno, pt_reg_no,pt_reg_date,lawstate,lawstate_date from tmplawstate2 where lawstate = '授权'";


        return DataFrameUtil.getDataFrame(spark,hql,"tmpshouquan2");
    }

    //拿到历史在一年内有效时间范围内的 最新状态
    private static Dataset getRanked3(SparkSession spark) {

        String hql = "select * from (select pripid,\n" +
                "pt_typeno,\n" +
                "pt_reg_date,\n" +
                "pt_reg_no,\n" +
                "lawstate,\n" +
                "lawstate_date, \n" +
                "max(trim(lawstate_date)) over (partition by pt_reg_no) as maxlaw_date \n" +
                "from tmplawstate2) \n" +
                "where maxlaw_date  = lawstate_date";

        return DataFrameUtil.getDataFrame(spark, hql, "tmpranked2");

    }

    private static Dataset getOk3(SparkSession spark) {

        String hql =  "select distinct pripid,pt_reg_no from (select a.pripid,\n" +
                "a.pt_typeno,\n" +
                "a.pt_reg_date,\n" +
                "a.pt_reg_no,\n" +
                "case when \n" +
                "trim(b.lawstate) ='避免重复授权放弃专利权' or \n" +
                "trim(b.lawstate) ='避免重复授予专利权' or \n" +
                "trim(b.lawstate) ='其他有关事项(避免重复授权放弃专利权)'or\n" +
                "trim(b.lawstate) ='其他有关事项(专利权全部撤销)'or\n" +
                "trim(b.lawstate) ='其他有关事项被视为放弃专利权的权利'or\n" +
                "trim(b.lawstate) ='其他有关事项避免重复授权放弃专利权'or\n" +
                "trim(b.lawstate) ='其他有关事项视为放弃专利权及自行放弃专利权'or\n" +
                "trim(b.lawstate) ='其他有关事项视为放弃专利权及自行放弃专利权公布内容'or\n" +
                "trim(b.lawstate) ='其他有关事项专利权全部撤销'or\n" +
                "trim(b.lawstate) ='专利权的撤销(专利权的全部撤销)'or\n" +
                "trim(b.lawstate) ='专利权的撤销(专利权全部撤销)'or\n" +
                "trim(b.lawstate) ='专利权的撤销专利权全部撤销'or\n" +
                "trim(b.lawstate) ='专利权的视为放弃'or\n" +
                "trim(b.lawstate) ='专利权的无效宣告(①专利权全部无效)'or\n" +
                "trim(b.lawstate) ='专利权的终止'or\n" +
                "trim(b.lawstate) ='专利权的终止  未缴年费专利权终止'or\n" +
                "trim(b.lawstate) ='专利权的终止((1)未缴年费专利权终止)'or\n" +
                "trim(b.lawstate) ='专利权的终止((2)专利权有效期届满)'or\n" +
                "trim(b.lawstate) ='专利权的终止(①未缴年费专利权终止)'or\n" +
                "trim(b.lawstate) ='专利权的终止(①专利权的自动放弃)'or\n" +
                "trim(b.lawstate) ='专利权的终止(①专利权有效期届满)'or\n" +
                "trim(b.lawstate) ='专利权的终止(②未缴年费专利权终止)'or\n" +
                "trim(b.lawstate) ='专利权的终止(②专利权有效期届满)'or\n" +
                "trim(b.lawstate) ='专利权的终止(③专利权有效期届满)'or\n" +
                "trim(b.lawstate) ='专利权的终止(未缴纳年费专利权终止)'or\n" +
                "trim(b.lawstate) ='专利权的终止(未缴年费专利权终止)'or\n" +
                "trim(b.lawstate) ='专利权的终止(专利权的主动放弃)'or\n" +
                "trim(b.lawstate) ='专利权的终止(专利权有效期届满)'or\n" +
                "trim(b.lawstate) ='专利权的终止未缴纳年费专利权终止'or\n" +
                "trim(b.lawstate) ='专利权的终止未缴年费专利权终止'or\n" +
                "trim(b.lawstate) ='专利权的终止主动放弃专利权的专利'or\n" +
                "trim(b.lawstate) ='专利权的终止专利权的主动放弃'or\n" +
                "trim(b.lawstate) ='专利权的终止专利权的自动放弃'or\n" +
                "trim(b.lawstate) ='专利权的终止专利权人放弃专利权'or\n" +
                "trim(b.lawstate) ='专利权的终止专利权有效期届满'or\n" +
                "trim(b.lawstate) ='专利权的主动放弃' then '-9' else\n" +
                "a.lawstate end as lawstate,\n" +
                "b.lawstate_date from tmpshouquan2 a join tmpranked2 b on a.pt_reg_no = b.pt_reg_no) where lawstate <> '-9'";

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
        //历史申请专利不含无效专利
        getAll(spark);
        getDateFit(spark);
        lawstate(spark);
        getRanked(spark);
        getShouquan(spark);
        getOk(spark);
        getHistoryApplicant(spark);
        //历史申请专利 一年内
        getAll1(spark);
        getDateFit1(spark);
        lawstate1(spark);
        getShouquan1(spark);
        getRanked1(spark);
        getOk1(spark);
        getHistoryApplicant1(spark);
        //历史申请专利 三年内
        getAll3(spark);
        getDateFit3(spark);
        lawstate3(spark);
        getShouquan3(spark);
        getRanked3(spark);
        getOk3(spark);
        getHistoryApplicant3(spark);

        //merge专利标签数据
        return getPatent(spark);


    }



}
