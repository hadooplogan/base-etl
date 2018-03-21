package com.chinadaas.next.etl.sparksql.sql;

import com.chinadaas.common.util.DataFrameUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;


/**
 * 2016年财报信息，2016相对于2015增长比率
 * 未知不做转码处理，因为取值中 可能有 '-9'
 * 码值很多，主要需要做的是核对码值，不和主表关联，先只拿出能关联的数据。
 */
public class FinancialSituation {

    private static Dataset get2016(SparkSession spark){
        String hql = "select \n" +
                "a.pripid,\n" +
                "case when trim(a.empnum) > 0.0 and trim(a.empnum) <= 10.0 then '01' \n" +
                "when trim(a.empnum) > 10.0 and trim(a.empnum) <= 30.0 then '02'\n" +
                "when trim(a.empnum) > 30.0 and trim(a.empnum) <= 50.0 then '03'\n" +
                "when trim(a.empnum) > 50.0 and trim(a.empnum) <= 100.0 then '04'\n" +
                "when trim(a.empnum) > 100.0 and trim(a.empnum) <= 300.0 then '05'\n" +
                "when trim(a.empnum) > 300.0 and trim(a.empnum) <= 500.0 then '06'\n" +
                "when trim(a.empnum) > 500.0 and trim(a.empnum) <= 1000.0 then '07'\n" +
                "when trim(a.empnum) > 1000.0 and trim(a.empnum) <= 2000.0 then '08'\n" +
                "when trim(a.empnum) > 2000.0 and trim(a.empnum) <= 3000.0 then '09'\n" +
                "when trim(a.empnum) > 3000.0 and trim(a.empnum) <= 5000.0 then '10'\n" +
                "when trim(a.empnum) > 5000.0 and trim(a.empnum) <= 10000.0 then '11'\n" +
                "when trim(a.empnum) > 10000.0 then '12'\n" +
                "else '未知' end as ee0066,\n" +
                "case when b.ratgro < 0.0 then '01'\n" +
                "when b.ratgro > 0.0 and b.ratgro <= 10.0 then '02'\n" +
                "when b.ratgro > 10.0 and b.ratgro <=50.0 then '03'\n" +
                "when b.ratgro > 50.0 and b.ratgro <= 100.0 then '04' \n" +
                "when b.ratgro > 100.0 and b.ratgro <= 300.0 then '05'\n" +
                "when b.ratgro > 300.0 and b.ratgro <= 500.0 then '06'\n" +
                "when b.ratgro > 500.0 and b.ratgro <= 1000.0 then '07'\n" +
                "when b.ratgro > 1000.0 and b.ratgro <= 2000.0 then '08'\n" +
                "when b.ratgro > 2000.0 and b.ratgro <= 3000.0 then '09'\n" +
                "when b.ratgro > 3000.0 and b.ratgro <= 5000.0 then '10'\n" +
                "when b.ratgro > 5000.0 and b.ratgro <= 8000.0 then '11'\n" +
                "when b.ratgro > 8000.0 and b.ratgro <= 10000.0 then '12'\n" +
                "when b.ratgro > 10000.0 and b.ratgro <= 30000.0 then '13'\n" +
                "when b.ratgro > 30000.0 and b.ratgro <= 50000.0 then '14'\n" +
                "when b.ratgro > 50000.0 and b.ratgro <= 100000.0 then '15' \n" +
                "when b.ratgro > 100000.0 then '16'\n" +
                "when b.ratgro = 0.0 then '未知'\n"+
                "else '未知' end as ee0069,\n" +
                "case when b.assgro < 0.0 then '01'\n" +
                "when b.assgro > 0.0 and b.assgro <= 50.0 then '02'\n" +
                "when b.assgro > 50.0 and b.assgro <= 100.0 then '03'\n" +
                "when b.assgro > 100.0 and b.assgro <= 300.0 then '04'\n" +
                "when b.assgro > 300.0 and b.assgro <= 500.0 then '05'\n" +
                "when b.assgro > 500.0 and b.assgro <= 1000.0 then '06'\n" +
                "when b.assgro > 1000.0 and b.assgro <= 2000.0 then '07'\n" +
                "when b.assgro > 2000.0 and b.assgro <= 3000.0 then '08'\n" +
                "when b.assgro > 3000.0 and b.assgro <= 5000.0 then '09'\n" +
                "when b.assgro > 5000.0 and b.assgro <= 8000.0 then '10'\n" +
                "when b.assgro > 8000.0 and b.assgro <= 10000.0 then '11'\n" +
                "when b.assgro > 10000.0 and b.assgro <= 30000.0 then '12'\n" +
                "when b.assgro > 30000.0 and b.assgro <= 50000.0 then '13'\n" +
                "when b.assgro > 50000.0 and b.assgro <= 100000.0 then '14'\n" +
                "when b.assgro > 100000.0 then '15'  \n" +
                "when b.assgro = 0.0 then '未知'\n"+
                "else '未知' end as ee0067,\n" +
                "case when b.vendinc < 0 then '16'\n" +
                "when b.vendinc > 0 and b.vendinc <= 10 then '01'\n" +
                "when b.vendinc > 10 and b.vendinc <= 50 then '02'\n" +
                "when b.vendinc > 50 and b.vendinc <= 100 then '03'\n" +
                "when b.vendinc > 100 and b.vendinc <= 300 then '04'\n" +
                "when b.vendinc > 300 and b.vendinc <= 500 then '05'\n" +
                "when b.vendinc > 500 and b.vendinc <= 1000 then '06'\n" +
                "when b.vendinc > 1000 and b.vendinc <= 2000 then '07'\n" +
                "when b.vendinc > 2000 and b.vendinc <= 3000 then '08'\n" +
                "when b.vendinc > 3000 and b.vendinc <= 5000 then '09'\n" +
                "when b.vendinc > 5000 and b.vendinc <= 8000 then '10'\n" +
                "when b.vendinc > 8000 and b.vendinc <= 10000 then '11'\n" +
                "when b.vendinc > 10000 and b.vendinc <= 30000 then '12'\n" +
                "when b.vendinc > 30000 and b.vendinc <= 50000 then '13'\n" +
                "when b.vendinc > 50000 and b.vendinc <= 100000 then '14'\n" +
                "when b.vendinc > 100000 then '15' \n" +
                "when b.vendinc = 0.0 then '未知'\n"+
                "else '未知' end as ee0068, \n"+
                "case when b.progro < 0 then '16'\n" +
                "when b.progro > 0 and b.progro <= 10 then '01'\n" +
                "when b.progro > 10 and b.progro <=50 then '02'\n" +
                "when b.progro > 50 and b.progro <= 100 then '03'\n" +
                "when b.progro >100 and b.progro <= 300 then '04'\n" +
                "when b.progro > 300 and b.progro <= 500 then '05'\n" +
                "when b.progro > 500 and b.progro <= 1000 then '06'\n" +
                "when b.progro > 1000 and b.progro <= 2000 then '07'\n" +
                "when b.progro > 2000 and b.progro <= 3000 then '08'\n" +
                "when b.progro > 3000 and b.progro <= 5000 then '09'\n" +
                "when b.progro > 5000 and b.progro <= 8000 then '10'\n" +
                "when b.progro > 8000 and b.progro <= 10000 then '11'\n" +
                "when b.progro > 10000 and b.progro <= 30000 then '12'\n" +
                "when b.progro > 30000 and b.progro <= 50000 then '13'\n" +
                "when b.progro > 50000 and b.progro <= 100000 then '14'\n" +
                "when b.progro > 100000 then '15' \n" +
                "when b.progro = 0.0 then '未知'\n"+
                "else '未知' end as ee0070, \n"+
                "case when (b.progro/b.vendinc)*100 <= -10 then '01' \n" +
                "when (b.progro/b.vendinc)*100 > -10 and (b.progro/b.vendinc)*100 < 0 then '02'\n" +
                "when (b.progro/b.vendinc)*100 > 0 and (b.progro/b.vendinc)*100 <= 10 then '03'\n" +
                "when (b.progro/b.vendinc)*100 > 10 and (b.progro/b.vendinc)*100 <= 20 then '04'\n" +
                "when (b.progro/b.vendinc)*100 > 20 and (b.progro/b.vendinc)*100 <= 30 then '05'\n" +
                "when (b.progro/b.vendinc)*100 > 30 and (b.progro/b.vendinc)*100 <= 40 then '06'\n" +
                "when (b.progro/b.vendinc)*100 > 40 and (b.progro/b.vendinc)*100 <= 50 then '07'\n" +
                "when (b.progro/b.vendinc)*100 > 50 then '08'\n" +
                "else '未知' \n" +
                "end as ee0071,\n" +
                "case when (b.netinc/b.vendinc)*100 <= -10 then '01' \n" +
                "when (b.netinc/b.vendinc)*100 > -10 and (b.netinc/b.vendinc)*100 < 0 then '02'\n" +
                "when (b.netinc/b.vendinc)*100 > 0 and (b.netinc/b.vendinc)*100 <= 10 then '03'\n" +
                "when (b.netinc/b.vendinc)*100 >10 and (b.netinc/b.vendinc)*100 <= 20 then '04'\n" +
                "when (b.netinc/b.vendinc)*100 > 20 and (b.netinc/b.vendinc)*100 <= 30 then '05'\n" +
                "when (b.netinc/b.vendinc)*100 > 30 and (b.netinc/b.vendinc)*100 <= 40 then '06'\n" +
                "when (b.netinc/b.vendinc)*100 > 40 and (b.netinc/b.vendinc)*100 <= 50 then '07'\n" +
                "when (b.netinc/b.vendinc)*100 > 50 then '08'\n" +
                "else '未知' \n" +
                "end as ee0072,\n" +
                "case when (b.netinc/b.totequ)*100 <= -20 then '01' \n" +
                "when (b.netinc/b.totequ)*100 > -20 and (b.netinc/b.totequ)*100 <= 10 then '02' \n" +
                "when (b.netinc/b.totequ)*100 > -10 and (b.netinc/b.totequ)*100 < 0 then '03'\n" +
                "when (b.netinc/b.totequ)*100 > 0 and (b.netinc/b.totequ)*100 <= 5 then '04'\n" +
                "when (b.netinc/b.totequ)*100 > 5 and (b.netinc/b.totequ)*100 <= 10 then '05'\n" +
                "when (b.netinc/b.totequ)*100 > 10 and (b.netinc/b.totequ)*100 <= 15 then '06'\n" +
                "when (b.netinc/b.totequ)*100 > 15 and (b.netinc/b.totequ)*100 <= 20 then '07'\n" +
                "when (b.netinc/b.totequ)*100 > 20 and (b.netinc/b.totequ)*100 <= 30 then '08'\n" +
                "when (b.netinc/b.totequ)*100 > 30 and (b.netinc/b.totequ)*100 <= 50 then '09'\n" +
                "when (b.netinc/b.totequ)*100 > 50 then '10'\n" +
                "else '未知' \n" +
                "end as ee0073,\n" +
                "case when ( b.liagro/b.totequ)*100 < 0 then '01'\n" +
                "when (b.liagro/b.totequ)*100 > 0 and (b.liagro/b.totequ)*100 <= 50 then '02'\n" +
                "when (b.liagro/b.totequ)*100 > 50 and (b.liagro/b.totequ)*100 < 100 then '03'\n" +
                "when (b.liagro/b.totequ)*100 > 100 and (b.liagro/b.totequ)*100 <= 200 then '04'\n" +
                "when (b.liagro/b.totequ)*100 > 200 and (b.liagro/b.totequ)*100 <= 300 then '05'\n" +
                "when (b.liagro/b.totequ)*100 > 300 and (b.liagro/b.totequ)*100 <= 500 then '06'\n" +
                "when (b.liagro/b.totequ)*100 > 500 and (b.liagro/b.totequ)*100 <= 800 then '07'\n" +
                "when (b.liagro/b.totequ)*100 > 800 then '08'\n" +
                "else '未知' end as ee0074,\n" +
                "case when (b.progro/a.empnum) > 0 and (b.progro/a.empnum) <= 10 then '01'\n" +
                "when (b.progro/a.empnum)  > 10 and (b.progro/a.empnum)  <= 50 then '02'\n" +
                "when (b.progro/a.empnum)  > 50 and (b.progro/a.empnum)  <= 100 then '03'\n" +
                "when (b.progro/a.empnum)  > 100 and (b.progro/a.empnum)  <= 300 then '04'\n" +
                "when (b.progro/a.empnum)  > 300 and (b.progro/a.empnum)  <= 500 then '05'\n" +
                "when (b.progro/a.empnum)  > 500 and (b.progro/a.empnum)  <= 1000 then '06'\n" +
                "when (b.progro/a.empnum)  > 1000 and (b.progro/a.empnum)  <= 2000 then '07'\n" +
                "when (b.progro/a.empnum)  > 2000 and (b.progro/a.empnum)  <= 3000 then '08'\n" +
                "when (b.progro/a.empnum)  > 3000 and (b.progro/a.empnum)  <= 5000 then '09'\n" +
                "when (b.progro/a.empnum)  > 5000 and (b.progro/a.empnum) <= 10000 then '10'\n" +
                "when (b.progro/a.empnum) > 10000 then '11'\n" +
                "when (b.progro/a.empnum) < 0 then'12'\n" +
                "else '未知' end as ee0075,\n" +
                "case when (b.vendinc/a.empnum) > 0 and (b.vendinc/a.empnum) <= 10 then '01'\n" +
                "when (b.vendinc/a.empnum)  > 10 and (b.vendinc/a.empnum)  <= 50 then '02'\n" +
                "when (b.vendinc/a.empnum)  > 50 and (b.vendinc/a.empnum)  <= 100 then '03'\n" +
                "when (b.vendinc/a.empnum)  > 100 and (b.vendinc/a.empnum)  <= 300 then '04'\n" +
                "when (b.vendinc/a.empnum)  > 300 and (b.vendinc/a.empnum)  <= 500 then '05'\n" +
                "when (b.vendinc/a.empnum)  > 500 and (b.vendinc/a.empnum)  <= 1000 then '06'\n" +
                "when (b.vendinc/a.empnum)  > 1000 and (b.vendinc/a.empnum)  <= 2000 then '07'\n" +
                "when (b.vendinc/a.empnum)  > 2000 and (b.vendinc/a.empnum)  <= 3000 then '08'\n" +
                "when (b.vendinc/a.empnum)  > 3000 and (b.vendinc/a.empnum)  <= 5000 then '09'\n" +
                "when (b.vendinc/a.empnum)  > 5000 and (b.vendinc/a.empnum) <= 10000 then '10'\n" +
                "when (b.vendinc/a.empnum) > 10000 then '11'\n" +
                "when (b.vendinc/a.empnum) < 0 then'12'\n" +
                "else '未知' end as ee0076\n" +
                "from s_en_nb_baseinfo a \n" +
                "join s_en_nb_capitalinfo b \n" +
                "on a.task_id = b.task_id";

        return DataFrameUtil.getDataFrame(spark,hql,"tmpbasic2016");

    }


    //同样对未知数据处理为'未知',0.0为未知。
    private static Dataset getSpecific2016(SparkSession spark){

        String hql = "select\n" +
                "a.pripid,\n" +
                "b.assgro,\n" +
                "b.vendinc,\n" +
                "case when b.liagro is null or b.liagro = '' or b.totequ is null or b.totequ = ''\n" +
                "then '未知' else cast ((b.liagro/b.totequ) as decimal(10,4)) end as leverageratio,\n" +
                "case when b.ratgro is null or b.ratgro = ''  or b.ratgro = 0.0 then '未知' else b.ratgro end as ratgro,\n" +
                "case when a.empnum is null or a.empnum = '' or a.empnum = 0.0 then '未知' else cast ((b.progro/a.empnum) as decimal(10,4)) end as pcpm,\n" +
                "case when a.empnum is null or a.empnum = '' or a.empnum = 0.0 then '未知' else a.empnum end as empnum\n" +
                "from s_en_nb_baseinfo a\n" +
                "join s_en_nb_capitalinfo b \n" +
                "on a.task_id = b.task_id";
     return    DataFrameUtil.getDataFrame(spark,hql,"tmp2016",0);

    }

    //对于负数 进行处理为 'n' ,对于上限福利为'm',未知仍为 '未知'
    private static Dataset getSpecific2015(SparkSession spark){

        String hql = "select pripid ,\n" +
                "case when trim(assgro) = '1.0' then 'n'\n" +
                "when trim(assgro) = '2.0' then 25.0\n" +
                "when trim(assgro) = '3.0' then 75.0\n" +
                "when trim(assgro) = '4.0' then 200.0\n" +
                "when trim(assgro) = '5.0' then 400.0\n" +
                "when trim(assgro) = '6.0' then 750.0\n" +
                "when trim(assgro) = '7.0' then 1500.0\n" +
                "when trim(assgro) = '8.0' then 2500.0\n" +
                "when trim(assgro) = '9.0' then 4000.0\n" +
                "when trim(assgro) = '10.0' then 6500.0\n" +
                "when trim(assgro) = '11.0' then 9000.0\n" +
                "when trim(assgro) = '12.0' then 20000.0\n" +
                "when trim(assgro) = '13.0' then 40000.0\n" +
                "when trim(assgro) = '14.0' then 75000.0\n" +
                "when trim(assgro) = '15.0' then 'm'\n" +
                "when trim(assgro) = '99.0' and trim(assgro) = '0' then '未知'\n" +
                "else '未知' end as assgro,\n" +
                "case when trim(vendinc) = '1' then 5.0\n" +
                "when trim(vendinc) = '2.0' then 30.0\n" +
                "when trim(vendinc) = '3.0' then 75.0\n" +
                "when trim(vendinc) = '4.0' then 200.0\n" +
                "when trim(vendinc) = '5.0' then 400.0\n" +
                "when trim(vendinc) = '6.0' then 750.0\n" +
                "when trim(vendinc) = '7.0' then 1500.0\n" +
                "when trim(vendinc) = '8.0' then 2500.0\n" +
                "when trim(vendinc) = '9.0' then 4000.0\n" +
                "when trim(vendinc) = '10.0' then 6500.0\n" +
                "when trim(vendinc) = '11.0' then 9000.0\n" +
                "when trim(vendinc) = '12.0' then 20000.0\n" +
                "when trim(vendinc) = '13.0' then 40000.0\n" +
                "when trim(vendinc) = '14.0' then 75000.0\n" +
                "when trim(vendinc) = '15.0' then 'm'\n" +
                "when trim(vendinc) = '16.0' then 'n'\n" +
                "when trim(vendinc) = '99.0' and trim(vendinc) = '0' then '未知'\n" +
                "else '未知' end as vendinc,\n" +
                "case when trim(leverageratio) = '1.0' then 'n'\n" +
                "when trim(leverageratio) = '2.0' then 0.2500 \n" +
                "when trim(leverageratio) = '3.0' then 0.7500 \n" +
                "when trim(leverageratio) = '4.0' then 1.5000\n" +
                "when trim(leverageratio) = '5.0' then 2.5000\n" +
                "when trim(leverageratio) = '6.0' then 4.0000\n" +
                "when trim(leverageratio) = '7.0' then 6.5000\n" +
                "when trim(leverageratio) = '8.0' then 'm'\n" +
                "when trim(leverageratio) = '99.0' or trim(leverageratio) = '0' then '未知'\n" +
                "else '未知' end as leverageratio,\n" +
                "case when trim(ratgro) = '1.0' then 'n'\n" +
                "when trim(ratgro) = '2.0' then 5.0\n" +
                "when trim(ratgro) = '3.0' then 30.0\n" +
                "when trim(ratgro) = '4.0' then 75.0\n" +
                "when trim(ratgro) = '5.0' then 200.0\n" +
                "when trim(ratgro) = '6.0' then 400.0\n" +
                "when trim(ratgro) = '7.0' then 750.0\n" +
                "when trim(ratgro) = '8.0' then 1500.0\n" +
                "when trim(ratgro) = '9.0' then 2500.0\n" +
                "when trim(ratgro) = '10.0' then 4000.0\n" +
                "when trim(ratgro) = '11.0' then 6500.0\n" +
                "when trim(ratgro) = '12.0' then 9000.0\n" +
                "when trim(ratgro) = '13.0' then 20000.0\n" +
                "when trim(ratgro) = '14.0' then 40000.0\n" +
                "when trim(ratgro) = '15.0' then 75000.0\n" +
                "when trim(ratgro) = '16.0' then 'm'\n" +
                "when trim(ratgro) = '99.0' then '未知'\n" +
                "else '未知' end as ratgro,\n" +
                "case when trim(pcpm) = '1.0' then 5.0\n" +
                "when trim(pcpm) = '2.0' then 30.0\n" +
                "when trim(pcpm) = '3.0' then 75.0\n" +
                "when trim(pcpm) = '4.0' then 200.0\n" +
                "when trim(pcpm) = '5.0' then 400.0\n" +
                "when trim(pcpm) = '6.0' then 750.0\n" +
                "when trim(pcpm) = '7.0' then 1500.0\n" +
                "when trim(pcpm) = '8.0' then 2500.0\n" +
                "when trim(pcpm) = '9.0' then 4000.0\n" +
                "when trim(pcpm) = '10.0' then 7500.0\n" +
                "when trim(pcpm) = '11.0' then 'm'\n" +
                "when trim(pcpm) = '12.0' then 'n'\n" +
                "when trim(pcpm) = '99.0' then '未知'\n" +
                "else '未知' end as pcpm,\n" +
                "case when trim(empnum) = '1.0' then 5.0\n" +
                "when trim(empnum) = '2.0' then 20.0\n" +
                "when trim(empnum) = '3.0' then 40.0\n" +
                "when trim(empnum) = '4.0' then 75.0\n" +
                "when trim(empnum) = '5.0' then 200.0\n" +
                "when trim(empnum) = '6.0' then 400.0\n" +
                "when trim(empnum) = '7.0' then 750.0\n" +
                "when trim(empnum) = '8.0' then 1500.0\n" +
                "when trim(empnum) = '9.0' then 2500.0\n" +
                "when trim(empnum) = '10.0' then 4000.0\n" +
                "when trim(empnum) = '11.0' then 7500.0\n" +
                "when trim(empnum) = '12.0' then 'm'\n" +
                "when trim(empnum) = '99.0' then '未知'\n" +
                "else '未知' end as empnum\n" +
                "from an_report";

        return DataFrameUtil.getDataFrame(spark,hql,"tmp2015");

    }



    private static Dataset getRate2016(SparkSession spark){

        String hql = "select a.pripid,\n" +
                "case when a.assgro is null or a.assgro = '' or a.assgro = '未知' or a.assgro = 'm'then '未知'\n" +
                "when b.assgro = 'n' and a.assgro > 0 then '扭亏为盈'\n" +
                "when b.assgro = 'n' and a.assgro <= 0 then '亏损'\n" +
                "when b.assgro = '未知' then '未知'\n" +
                "else cast (((a.assgro - b.assgro)/b.assgro) as decimal(10,4)) end as ee0077,\n" +
                "case when a.vendinc is null or a.vendinc = '' or a.vendinc = '未知' or a.vendinc = 'm' then '未知'\n" +
                "when b.vendinc = 'n' and a.vendinc > 0 then '扭亏为盈'\n" +
                "when b.vendinc = 'n' and a.vendinc <= 0 then '亏损'\n" +
                "when b.vendinc = '未知' then '未知'\n" +
                "else cast (((a.vendinc - b.vendinc)/b.vendinc) as decimal(10,4)) end as ee0078,\n" +
                "case when a.leverageratio is null or a.leverageratio = '' or a.leverageratio = '未知' or a.leverageratio = 'm' then '未知'\n" +
                "when b.leverageratio = 'n' and a.leverageratio > 0 then '扭亏为盈'\n" +
                "when b.leverageratio = 'n' and a.leverageratio <= 0 then '亏损'\n" +
                "when b.leverageratio = '未知' then '未知'\n" +
                "else cast (((a.leverageratio - b.leverageratio)/b.leverageratio) as decimal(10,4)) end as ee0080,\n" +
                "case when (a.ratgro is null or a.ratgro = '' or a.ratgro = '未知' or a.ratgro = 'm') then '未知'\n" +
                "when b.ratgro = 'n' and a.ratgro > 0 then '扭亏为盈'\n" +
                "when b.ratgro = 'n' and a.ratgro <= 0 then '亏损'\n" +
                "when b.ratgro = '未知' then '未知'\n" +
                "else cast (((a.ratgro - b.ratgro)/b.ratgro) as decimal(10,4)) end as ee0081,\n" +
                "case when a.pcpm is null or a.pcpm = '' or a.pcpm = '未知' or a.pcpm = 'm' then '未知'\n" +
                "when b.pcpm = '未知' then '未知'\n" +
                "when b.pcpm = 'n' and a.pcpm > 0 then '扭亏为盈'\n" +
                "when b.pcpm = 'n' and a.pcpm <= 0 then '亏损'\n" +
                "else cast (((a.pcpm - b.pcpm)/b.pcpm) as decimal(10,4)) end as ee0082,\n" +
                "case when a.empnum = '未知' or a.empnum = 'm' then '未知'\n" +
                "when b.empnum = '未知' then '未知'\n" +
                "else cast (((a.empnum - b.empnum)/b.empnum) as decimal(10,4)) end as ee0083\n" +
                "from tmp2016 a join tmp2015 b \n" +
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
