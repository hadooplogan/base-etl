package com.chinadaas.next.etl.sparksql.sql;

import com.chinadaas.common.util.DataFrameUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;


/**
 * 2016年财报信息，2016相对于2015增长比率
 *
 */
public class FinancialSituation {

    private static Dataset get2016(SparkSession spark){
        String hql = "select \n" +
                "a.pripid,\n" +
                "case when b.empnum > 0 and b.empnum <= 10 then '01' \n" +
                "when b.empnum > 10 and b.empnum <= 30 then '02'\n" +
                "when b.empnum > 30 and b.empnum <= 50 then '03'\n" +
                "when b.empnum > 50 and b.empnum <= 100 then '04'\n" +
                "when b.empnum > 100 and b.empnum <= 300 then '05'\n" +
                "when b.empnum > 300 and b.empnum <= 500 then '06'\n" +
                "when b.empnum > 500 and b.empnum <= 1000 then '07'\n" +
                "when b.empnum > 1000 and b.empnum <= 2000 then '08'\n" +
                "when b.empnum > 2000 and b.empnum <= 3000 then '09'\n" +
                "when b.empnum > 3000 and b.empnum <= 5000 then '10'\n" +
                "when b.empnum > 5000 and b.empnum <= 10000 then '11'\n" +
                "when b.empnum > 10000 then '12'\n" +
                "else '未知' end as ee0066,\n" +
                "case when c.ratgro < 0 then '01'\n" +
                "when c.ratgro > 0 and c.ratgro <= 10 then '02'\n" +
                "when c.ratgro > 10 and c.ratgro <=20 then '03'\n" +
                "when c.ratgro > 50 and c.ratgro <= 100 then '04' \n" +
                "when c.ratgro > 100 and c.ratgro <= 300 then '05'\n" +
                "when c.ratgro > 300 and c.ratgro <= 500 then '06'\n" +
                "when c.ratgro > 500 and c.ratgro <= 1000 then '07'\n" +
                "when c.ratgro > 1000 and c.ratgro <= 2000 then '08'\n" +
                "when c.ratgro > 2000 and c.ratgro <= 3000 then '09'\n" +
                "when c.ratgro > 3000 and c.ratgro <= 5000 then '10'\n" +
                "when c.ratgro > 5000 and c.ratgro <= 8000 then '11'\n" +
                "when c.ratgro > 8000 and c.ratgro <= 10000 then '12'\n" +
                "when c.ratgro > 10000 and c.ratgro <= 30000 then '13'\n" +
                "when c.ratgro > 30000 and c.ratgro <= 50000 then '14'\n" +
                "when c.ratgro > 50000 and c.ratgro <= 100000 then '15' \n" +
                "when c.ratgro > 100000 then '16'\n" +
                "else '未知' end as ee0069,\n" +
                "case when c.assgro < 0 then '01'\n" +
                "when c.assgro > 0 and c.assgro <= 10 then '02'\n" +
                "when c.assgro > 10 and c.assgro <=50 then '03'\n" +
                "when c.assgro > 50 and c.assgro <= 100 then '04'\n" +
                "when c.assgro >100 and c.assgro <= 300 then '05'\n" +
                "when c.assgro > 300 and c.assgro <= 500 then '06'\n" +
                "when c.assgro > 500 and c.assgro <= 1000 then '07'\n" +
                "when c.assgro > 1000 and c.assgro <= 2000 then '08'\n" +
                "when c.assgro > 2000 and c.assgro <= 3000 then '09'\n" +
                "when c.assgro > 5000 and c.assgro <= 8000 then '10'\n" +
                "when c.assgro > 8000 and c.assgro <= 10000 then '11'\n" +
                "when c.assgro > 10000 and c.assgro <= 30000 then '12'\n" +
                "when c.assgro > 30000 and c.assgro <= 50000 then '13'\n" +
                "when c.assgro > 50000 and c.assgro <= 10 then '14'\n" +
                "when c.assgro > 100000 then '15'  \n" +
                "else '未知' end as ee0067,\n" +
                "case when c.vendinc < 0 then '15'\n" +
                "when c.vendinc > 0 and c.vendinc <= 10 then '01'\n" +
                "when c.vendinc > 10 and c.vendinc <=5 then '02'\n" +
                "when c.vendinc > 50 and c.vendinc <= 100 then '03'\n" +
                "when c.vendinc >100 and c.vendinc <= 300 then '04'\n" +
                "when c.vendinc > 300 and c.vendinc <= 500 then '05'\n" +
                "when c.vendinc > 500 and c.vendinc <= 1000 then '06'\n" +
                "when c.vendinc > 1000 and c.vendinc <= 2000 then '07'\n" +
                "when c.vendinc > 2000 and c.vendinc <= 3000 then '08'\n" +
                "when c.vendinc > 5000 and c.vendinc <= 8000 then '09'\n" +
                "when c.vendinc > 8000 and c.vendinc <= 10000 then '10'\n" +
                "when c.vendinc > 10000 and c.vendinc <= 30000 then '11'\n" +
                "when c.vendinc > 30000 and c.vendinc <= 50000 then '12'\n" +
                "when c.vendinc > 50000 and c.vendinc <= 100000 then '13'\n" +
                "when c.vendinc > 100000 then '14' \n" +
                "else '未知' end as ee0068,\n" +
                "case when c.progro < 0 then '15'\n" +
                "when c.progro > 0 and c.progro <= 100000 then '01'\n" +
                "when c.progro > 100000 and c.progro <=50000 then '02'\n" +
                "when c.progro > 500000 and c.progro <= 1000000 then '03'\n" +
                "when c.progro >1000000 and c.progro <= 3000000 then '04'\n" +
                "when c.progro > 3000000 and c.progro <= 5000000 then '05'\n" +
                "when c.progro > 5000000 and c.progro <= 10000000 then '06'\n" +
                "when c.progro > 10000000 and c.progro <= 20000000 then '07'\n" +
                "when c.progro > 20000000 and c.progro <= 30000000 then '08'\n" +
                "when c.progro > 50000000 and c.progro <= 80000000 then '09'\n" +
                "when c.progro > 80000000 and c.progro <= 100000000 then '10'\n" +
                "when c.progro > 100000000 and c.progro <= 300000000 then '11'\n" +
                "when c.progro > 300000000 and c.progro <= 500000000 then '12'\n" +
                "when c.progro > 500000000 and c.progro <= 1000000000 then '13'\n" +
                "when c.progro > 1000000000 then '14' \n" +
                "else '未知' end as ee0070,\n" +
                "case when (c.progro/c.vendinc)*100 <= -10 then '01' \n" +
                "when (c.progro/c.vendinc)*100 > -10 and (c.progro/c.vendinc)*100 < 0 then '02'\n" +
                "when (c.progro/c.vendinc)*100 > 0 and (c.progro/c.vendinc)*100 <= 10 then '03'\n" +
                "when (c.progro/c.vendinc)*100 > 10 and (c.progro/c.vendinc)*100 <= 20 then '04'\n" +
                "when (c.progro/c.vendinc)*100 > 20 and (c.progro/c.vendinc)*100 <= 30 then '05'\n" +
                "when (c.progro/c.vendinc)*100 > 30 and (c.progro/c.vendinc)*100 <= 40 then '06'\n" +
                "when (c.progro/c.vendinc)*100 > 40 and (c.progro/c.vendinc)*100 <= 50 then '07'\n" +
                "when (c.progro/c.vendinc)*100 > 50 then '08'\n" +
                "else '未知' \n" +
                "end as ee0071,\n" +
                "case when (c.netinc/c.vendinc)*100 <= -10 then '01' \n" +
                "when (c.netinc/c.vendinc)*100 > -10 and (c.netinc/c.vendinc)*100 < 0 then '02'\n" +
                "when (c.netinc/c.vendinc)*100 > 0 and (c.netinc/c.vendinc)*100 <= 10 then '03'\n" +
                "when (c.netinc/c.vendinc)*100 >10 and (c.netinc/c.vendinc)*100 <= 20 then '04'\n" +
                "when (c.netinc/c.vendinc)*100 > 20 and (c.netinc/c.vendinc)*100 <= 30 then '05'\n" +
                "when (c.netinc/c.vendinc)*100 > 30 and (c.netinc/c.vendinc)*100 <= 40 then '06'\n" +
                "when (c.netinc/c.vendinc)*100 > 40 and (c.netinc/c.vendinc)*100 <= 50 then '07'\n" +
                "when (c.netinc/c.vendinc)*100 > 50 then '08'\n" +
                "else '未知' \n" +
                "end as ee0072,\n" +
                "case when (c.netinc/c.totequ)*100 <= -20 then '01' \n" +
                "when (c.netinc/c.totequ)*100 > -20 and (c.netinc/c.totequ)*100 < 10 then '02' \n" +
                "when (c.netinc/c.totequ)*100 > -10 and (c.netinc/c.totequ)*100 < 0 then '03'\n" +
                "when (c.netinc/c.totequ)*100 > 0 and (c.netinc/c.totequ)*100 <= 5 then '04'\n" +
                "when (c.netinc/c.totequ)*100 >5 and (c.netinc/c.totequ)*100 <= 10 then '05'\n" +
                "when (c.netinc/c.totequ)*100 > 10 and (c.netinc/c.totequ)*100 <= 15 then '06'\n" +
                "when (c.netinc/c.totequ)*100 > 15 and (c.netinc/c.totequ)*100 <= 20 then '07'\n" +
                "when (c.netinc/c.totequ)*100 > 20 and (c.netinc/c.totequ)*100 <= 30 then '08'\n" +
                "when (c.netinc/c.totequ)*100 > 30 and (c.netinc/c.totequ)*100 <= 50 then '09'\n" +
                "when (c.netinc/c.totequ)*100 > 50 then '10'\n" +
                "else '未知' \n" +
                "end as ee0073,\n" +
                "case when ( c.liagro/c.totequ)*100 < 0 then '01'\n" +
                "when ( c.liagro/c.totequ)*100 > 0 and ( c.liagro/c.totequ)*100 <= 50 then '02'\n" +
                "when ( c.liagro/c.totequ)*100 > 50 and ( c.liagro/c.totequ)*100 < 100 then '03'\n" +
                "when ( c.liagro/c.totequ)*100 > 100 and ( c.liagro/c.totequ)*100 <= 200 then '04'\n" +
                "when ( c.liagro/c.totequ)*100 > 200 and ( c.liagro/c.totequ)*100 <= 300 then '05'\n" +
                "when ( c.liagro/c.totequ)*100 > 300 and ( c.liagro/c.totequ)*100 <= 500 then '06'\n" +
                "when ( c.liagro/c.totequ)*100 > 500 and ( c.liagro/c.totequ)*100 <= 800 then '07'\n" +
                "when ( c.liagro/c.totequ)*100 > 800 then '08'\n" +
                "else '未知' end as ee0074,\n" +
                "case when (c.progro/b.empnum) >0 and (c.progro/b.empnum) <=10 then '01'\n" +
                "when (c.progro/b.empnum)  > 10 and (c.progro/b.empnum)  <= 50 then '02'\n" +
                "when (c.progro/b.empnum)  > 50 and (c.progro/b.empnum)  <= 100 then '03'\n" +
                "when (c.progro/b.empnum)  > 100 and (c.progro/b.empnum)  <= 300 then '04'\n" +
                "when (c.progro/b.empnum)  > 300 and (c.progro/b.empnum)  <= 500 then '05'\n" +
                "when (c.progro/b.empnum)  > 500 and (c.progro/b.empnum)  <= 1000 then '06'\n" +
                "when (c.progro/b.empnum)  > 1000 and (c.progro/b.empnum)  <= 2000 then '07'\n" +
                "when (c.progro/b.empnum)  > 2000 and (c.progro/b.empnum)  <= 3000 then '08'\n" +
                "when (c.progro/b.empnum)  > 3000 and (c.progro/b.empnum)  <= 5000 then '09'\n" +
                "when (c.progro/b.empnum)  > 5000 and (c.progro/b.empnum) <= 10000 then '10'\n" +
                "when (c.progro/b.empnum) > 10000 then '11'\n" +
                "when (c.progro/b.empnum) < 0 then'12'\n" +
                "else '未知' end as ee0075,\n" +
                "case when (c.vendinc/b.empnum) > 0 and (c.vendinc/b.empnum) <=10 then '01'\n" +
                "when (c.vendinc/b.empnum)  > 10 and (c.vendinc/b.empnum)  <= 50 then '02'\n" +
                "when (c.vendinc/b.empnum)  > 50 and (c.vendinc/b.empnum)  <= 100 then '03'\n" +
                "when (c.vendinc/b.empnum)  > 100 and (c.vendinc/b.empnum)  <= 300 then '04'\n" +
                "when (c.vendinc/b.empnum)  > 300 and (c.vendinc/b.empnum)  <= 500 then '05'\n" +
                "when (c.vendinc/b.empnum)  > 500 and (c.vendinc/b.empnum)  <= 1000 then '06'\n" +
                "when (c.vendinc/b.empnum)  > 1000 and (c.vendinc/b.empnum)  <= 2000 then '07'\n" +
                "when (c.vendinc/b.empnum)  > 2000 and (c.vendinc/b.empnum)  <= 3000 then '08'\n" +
                "when (c.vendinc/b.empnum)  > 3000 and (c.vendinc/b.empnum)  <= 5000 then '09'\n" +
                "when (c.vendinc/b.empnum)  > 5000 and (c.vendinc/b.empnum) <= 10000 then '10'\n" +
                "when (c.vendinc/b.empnum) > 10000 then '11'\n" +
                "when (c.vendinc/b.empnum) < 0 then'12'\n" +
                "else '未知' end as ee0076\n" +
                "from enterprisebaseinfocollect a \n" +
                "left join s_en_nb_baseinfo b \n" +
                "on a.pripid = b.pripid \n" +
                "left join s_en_nb_capitalinfo c \n" +
                "on b.task_id = c.task_id";

        return DataFrameUtil.getDataFrame(spark,hql,"tmpbasic2016");

    }



    private static Dataset getSpecific2016(SparkSession spark){

        String hql = "select\n" +
                "a.pripid,\n" +
                "c.assgro,\n" +
                "c.vendinc,\n" +
                "case when c.liagro is null or c.liagro = '' or c.totequ is null or c.totequ = ''\n" +
                "then '未知' else cast ((c.liagro/c.totequ) as decimal(10,4)) end as leverageratio,\n" +
                "case when c.ratgro is null and c.ratgro = '' then '未知' else c.ratgro end as ratgro,\n" +
                "case when b.empnum is null and b.empnum = '' then '未知'  else cast ((c.progro/b.empnum) as decimal(10,4)) end as pcpm,\n" +
                "case when b.empnum is null and b.empnum = '' then '未知' else b.empnum end as empnum\n" +
                "from enterprisebaseinfocollect a \n" +
                "left join s_en_nb_baseinfo b\n" +
                "on a.pripid = b.pripid\n" +
                "left join s_en_nb_capitalinfo c \n" +
                "on b.task_id = c.task_id";
     return    DataFrameUtil.getDataFrame(spark,hql,"tmp2016",0);

    }


    private static Dataset getSpecific2015(SparkSession spark){

        String hql = "select a.pripid ,\n" +
                "case when b.assgro = '01' then '负数'\n" +
                "when b.assgro = '02' then 25.0\n" +
                "when b.assgro = '03' then 50.0\n" +
                "when b.assgro = '04' then 150.0\n" +
                "when b.assgro = '05' then 400.0\n" +
                "when b.assgro = '06' then 750.0\n" +
                "when b.assgro = '07' then 1500.0\n" +
                "when b.assgro = '08' then 2500.0\n" +
                "when b.assgro = '09' then 4000.0\n" +
                "when b.assgro = '10' then 6500.0\n" +
                "when b.assgro = '11' then 9000.0\n" +
                "when b.assgro = '12' then 20000.0\n" +
                "when b.assgro = '13' then 40000.0\n" +
                "when b.assgro = '14' then 75000.0\n" +
                "when b.assgro = '15' then '未知'\n" +
                "when b.assgro = '99' and b.assgro = '0' then '未知'\n" +
                "else '未知' end as assgro,\n" +
                "case when b.vendinc = '01' then 5\n" +
                "when b.vendinc = '02' then 30.0\n" +
                "when b.vendinc = '03' then 75.0\n" +
                "when b.vendinc = '04' then 200.0\n" +
                "when b.vendinc = '05' then 400.0\n" +
                "when b.vendinc = '06' then 750.0\n" +
                "when b.vendinc = '07' then 1500.0\n" +
                "when b.vendinc = '08' then 2500.0\n" +
                "when b.vendinc = '09' then 4000.0\n" +
                "when b.vendinc = '10' then 6500.0\n" +
                "when b.vendinc = '11' then 9000.0\n" +
                "when b.vendinc = '12' then 20000.0\n" +
                "when b.vendinc = '13' then 40000.0\n" +
                "when b.vendinc = '14' then 75000.0\n" +
                "when b.vendinc = '15' then '未知'\n" +
                "when b.vendinc = '16' then '亏损'\n" +
                "when b.vendinc = '99' and b.vendinc = '0' then '未知'\n" +
                "else '未知' end as vendinc,\n" +
                "case when b.leverageratio = '01' then '负数'\n" +
                "when b.leverageratio = '02' then 0.2500\n" +
                "when b.leverageratio = '03' then 0.7500\n" +
                "when b.leverageratio = '04' then 1.5000\n" +
                "when b.leverageratio = '05' then 2.5000\n" +
                "when b.leverageratio = '06' then 4.0000\n" +
                "when b.leverageratio = '07' then 6.5000\n" +
                "when b.leverageratio = '08' then '未知'\n" +
                "when b.leverageratio = '99' then '未知'\n" +
                "else '未知' end as leverageratio,\n" +
                "case when b.ratgro = '01' then '负数'\n" +
                "when b.ratgro = '02' then 5\n" +
                "when b.ratgro = '03' then 30\n" +
                "when b.ratgro = '04' then 75\n" +
                "when b.ratgro = '05' then 200\n" +
                "when b.ratgro = '06' then 400\n" +
                "when b.ratgro = '07' then 750\n" +
                "when b.ratgro = '08' then 1500\n" +
                "when b.ratgro = '09' then 2500\n" +
                "when b.ratgro = '10' then 4000\n" +
                "when b.ratgro = '11' then 6500\n" +
                "when b.ratgro = '12' then 9000\n" +
                "when b.ratgro = '13' then 20000\n" +
                "when b.ratgro = '14' then 40000\n" +
                "when b.ratgro = '15' then '未知'\n" +
                "when b.ratgro = '99' then '未知'\n" +
                "else '未知' end as ratgro,\n" +
                "case when b.pcpm = '01' then 5\n" +
                "when b.pcpm = '02' then 30\n" +
                "when b.pcpm = '03' then 75\n" +
                "when b.pcpm = '04' then 200\n" +
                "when b.pcpm = '05' then 400\n" +
                "when b.pcpm = '06' then 750\n" +
                "when b.pcpm = '07' then 1500\n" +
                "when b.pcpm = '08' then 2500\n" +
                "when b.pcpm = '09' then 4000\n" +
                "when b.pcpm = '10' then 7500\n" +
                "when b.pcpm = '11' then '未知'\n" +
                "when b.pcpm = '12' then '负数'\n" +
                "when b.pcpm = '99' then '未知'\n" +
                "else '未知' end as pcpm,\n" +
                "case when b.empnum = '01' then 5\n" +
                "when b.empnum = '02' then 20\n" +
                "when b.empnum = '03' then 40\n" +
                "when b.empnum = '04' then 75\n" +
                "when b.empnum = '05' then 200\n" +
                "when b.empnum = '06' then 400\n" +
                "when b.empnum = '07' then 750\n" +
                "when b.empnum = '08' then 1500\n" +
                "when b.empnum = '09' then 2500\n" +
                "when b.empnum = '10' then 4000\n" +
                "when b.empnum = '11' then 7500\n" +
                "when b.empnum = '12' then '未知'\n" +
                "when b.empnum = '99' then '未知'\n" +
                "else '未知' end as empnum\n" +
                "from enterprisebaseinfocollect a \n" +
                "left join\n" +
                "an_table b on a.pripid = b.pripid";

        return DataFrameUtil.getDataFrame(spark,hql,"tmp2015");

    }



    private static Dataset getRate2016(SparkSession spark){

        String hql = "select a.pripid,\n" +
                "case when a.assgro is null or a.assgro = '' then '未知'\n" +
                "when b.assgro = '负数' and a.assgro > 0 then '扭亏为盈'\n" +
                "when b.assgro = '负数' and a.assgro <= 0 then '亏损'\n" +
                "when b.assgro = '未知' then '未知'\n" +
                "else cast (((a.assgro - b.assgro)/b.assgro) as decimal(10,4)) end as ee0077,\n" +
                "case when a.vendinc is null then '未知'\n" +
                "when b.vendinc = '亏损' and a.vendinc > 0 then '扭亏为盈'\n" +
                "when b.vendinc = '亏损' and a.vendinc <= 0 then '亏损'\n" +
                "when b.vendinc = '未知' then '未知'\n" +
                "else cast (((a.vendinc - b.vendinc)/b.vendinc) as decimal(10,4)) end as ee0078,\n" +
                "case when a.leverageratio is null or a.leverageratio = '' then '未知'\n" +
                "when b.leverageratio = '负数' and a.leverageratio > 0 then '扭亏为盈'\n" +
                "when b.leverageratio = '负数' and a.leverageratio <= 0 then '亏损'\n" +
                "when b.leverageratio = '未知' then '未知'\n" +
                "else cast (((a.leverageratio - b.leverageratio)/b.leverageratio) as decimal(10,4)) end as ee0080,\n" +
                "case when a.ratgro is null or a.ratgro = '' then '未知'\n" +
                "when b.ratgro = '负数' and a.ratgro > 0 then '扭亏为盈'\n" +
                "when b.ratgro = '负数' and a.ratgro <= 0 then '亏损'\n" +
                "when b.ratgro = '未知' then '未知'\n" +
                "else cast (((a.ratgro - b.ratgro)/b.ratgro) as decimal(10,4)) end as ee0081,\n" +
                "case when a.pcpm is null then '未知'\n" +
                "when b.pcpm = '未知' then '未知'\n" +
                "when b.pcpm = '负数' and a.pcpm > 0 then '扭亏为盈'\n" +
                "when b.pcpm = '负数' and a.pcpm <= 0 then '亏损'\n" +
                "else cast (((a.pcpm - b.pcpm)/b.pcpm) as decimal(10,4)) end as ee0082,\n" +
                "case when a.empnum = '未知' then '未知'\n" +
                "when b.empnum = '未知' then '未知'\n" +
                "else cast (((a.empnum - b.empnum)/b.empnum) as decimal(10,4)) end as ee0083\n" +
                "from tmp2016 a left join tmp2015 b \n" +
                "on a.pripid = b.pripid ";

        return DataFrameUtil.getDataFrame(spark,hql,"tmprate",0);

    }


    private static Dataset getFinancialMerge(SparkSession spark){

        String hql ="select \n" +
                "a.pripid,\n" +
                "a.ee0066,\n" +
                "a.ee0067,\n" +
                "a.ee0068,\n" +
                "a.ee0069,\n" +
                "a.ee0070,\n" +
                "a.ee0071,\n" +
                "a.ee0072,\n" +
                "a.ee0073,\n" +
                "a.ee0074,\n" +
                "a.ee0075,\n" +
                "a.ee0076,\n" +
                "b.ee0077,\n" +
                "b.ee0078,\n" +
                "b.ee0080,\n" +
                "b.ee0081,\n" +
                "b.ee0082,\n" +
                "b.ee0083\n" +
                "from \n" +
                "tmpbasic2016 a \n" +
                "left join tmprate b on a.pripid = b.pripid";


return DataFrameUtil.getDataFrame(spark,hql,"tmpfinancial");


    }



  public static Dataset getFinancial(SparkSession spark){
      get2016(spark);
      getSpecific2016(spark);
      getSpecific2015(spark);
      getRate2016(spark);


      return getFinancialMerge(spark);




  }
}
