package com.chinadaas.next.etl.sparksql.sql;

import com.chinadaas.common.util.DataFrameUtil;
import com.chinadaas.common.util.TimeUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * Created by gongxs01 on 2017/8/30.
 */
public class BaseBiKpiETL {

    /**
     * 1	在营（开业）
     * 2	吊销
     * 21	吊销，未注销
     * 22	吊销，已注销
     * 3	注销
     * 4	迁出
     * 5	撤销
     * 6	临时(个体工商户使用)
     * 8	停业
     * 9	其他
     */

    public static Dataset getBaseBiKpi(SparkSession spark, String datadate) {

        String hql = " select a.pripid ,\n" +
                " a.credit_code ,\n" +
                " a.entname ,\n" +
                " a.s_ext_nodenum as ee0001,\n" +
                " h.city_adcode as ee0002,\n" +
                " case when a.entstatus ='2' or a.entstatus ='21' or a.entstatus ='22' or a.entstatus ='3' then '1'  else '0' end  as ee0003 ,\n" +
                " case when esdate <= '1900-01-01' then '-9' " +
                " when entstatus <> '1' and revdate = 'null' and candate <> 'null' then  round ((year(candate)*12+month(candate) - month(esdate)-year(esdate)*12)/12,1) \n" +
                " when entstatus <> '1' and revdate <> 'null' and candate = 'null' then round ((year(revdate)*12+month(revdate) - month(esdate)-year(esdate)*12)/12,1)\n" +
                " when entstatus <> '1' and revdate = 'null' and candate = 'null' then round ((year(APPRDATE)*12+month(APPRDATE) - month(esdate)-year(esdate)*12)/12,1)\n" +
                " when entstatus <> '1' and revdate <> 'null' and candate <> 'null' then  round ((year(candate)*12+month(candate) - month(esdate)-year(esdate)*12)/12,1)\n" +//2018-1-4增加吊销和注销都不为空的日期情况。
                " when entstatus = '1' then round (((" + TimeUtil.getYear() + "-year(esdate))*12+" + TimeUtil.getMonth() + "-month(esdate))/12,1) \n" +
                " else '-9'\n" +
                " end as eb0004,\n" +
                " a.regcap as ee0004,\n" +
                " a.regcapcur as ee0005,\n" +
                " case when (REGCAPCUR = '156' or REGCAPCUR = '') and 0< cast(regcap as double) and cast(regcap as double) <= 50.0 then '01' \n" +
                " when (REGCAPCUR = '156' or REGCAPCUR = '') and 50.0 < cast(regcap as double) and cast(regcap as double) <= 100.0 then '02'\n" +
                " when (REGCAPCUR = '156' or REGCAPCUR = '') and 100.0 < cast(regcap as double) and cast(regcap as double) <= 200.0 then '03'\n" +
                " when (REGCAPCUR = '156' or REGCAPCUR = '') and 200.0 < cast(regcap as double) and cast(regcap as double) <= 300.0 then '04'\n" +
                " when (REGCAPCUR = '156' or REGCAPCUR = '')" +
                " and 300.0 < cast(regcap as double) and cast(regcap as double) <= 400.0 then '05'\n" +
                " when (REGCAPCUR = '156' or REGCAPCUR = '') and 400.0 < cast(regcap as double) and cast(regcap as double) <= 500.0 then '06'\n" +
                " when (REGCAPCUR = '156' or REGCAPCUR = '') and 500.0 < cast(regcap as double) and cast(regcap as double) <= 600.0 then '07'\n" +
                " when (REGCAPCUR = '156' or REGCAPCUR = '') and 600.0 < cast(regcap as double) and cast(regcap as double) <= 800.0 then '08'\n" +
                " when (REGCAPCUR = '156' or REGCAPCUR = '') and 800.0 < cast(regcap as double) and cast(regcap as double) <= 1000.0 then '09'\n" +
                " when (REGCAPCUR = '156' or REGCAPCUR = '') and 1000.0 < cast(regcap as double) and cast(regcap as double) <= 1500.0 then '10'\n" +
                " when (REGCAPCUR = '156' or REGCAPCUR = '') and 1500.0 < cast(regcap as double) and cast(regcap as double) <= 2000.0 then '11'\n" +
                " when (REGCAPCUR = '156' or REGCAPCUR = '') and 2000.0 < cast(regcap as double) and cast(regcap as double) <= 3000.0 then '12'\n" +
                " when (REGCAPCUR = '156' or REGCAPCUR = '') and 3000.0 < cast(regcap as double) and cast(regcap as double) <= 5000.0 then '13'\n" +
                " when (REGCAPCUR = '156' or REGCAPCUR = '') and 5000.0 < cast(regcap as double) and cast(regcap as double) <= 10000.0 then '14'\n" +
                " when (REGCAPCUR = '156' or REGCAPCUR = '') and 10000.0 < cast(regcap as double) and cast(regcap as double) <= 20000.0 then '15'\n" +
                " when (REGCAPCUR = '156' or REGCAPCUR = '') and 20000.0 < cast(regcap as double) and cast(regcap as double) <= 30000.0 then '16'\n" +
                " when (REGCAPCUR = '156' or REGCAPCUR = '') and 30000.0 < cast(regcap as double) and cast(regcap as double) <= 50000.0 then '17'\n" +
                " when (REGCAPCUR = '156' or REGCAPCUR = '') and 50000.0 < cast(regcap as double) and cast(regcap as double) <= 100000.0 then '18'\n" +
                " when (REGCAPCUR = '156' or REGCAPCUR = '') and 100000.0 < cast(regcap as double) and cast(regcap as double) <= 800000.0 then '19'\n" +
                " when (REGCAPCUR = '156' or REGCAPCUR = '') and 800000.0 < cast(regcap as double) and cast(regcap as double) <= 1000000.0 then '20'\n" +
                " when (REGCAPCUR = '156' or REGCAPCUR = '') and 1000000.0 < cast(regcap as double) then '21'\n" +
                " else '-9' end as eb0006,\n" +
                " case when REGCAPCUR = '840' and 0< cast(regcap as double) and cast(regcap as double) <= 50.0 then '01' \n" +
                " when REGCAPCUR = '840' and 50.0 < cast(regcap as double) and cast(regcap as double) <= 100.0 then '02'\n" +
                " when REGCAPCUR = '840' and 100.0 < cast(regcap as double) and cast(regcap as double) <= 200.0 then '03'\n" +
                " when REGCAPCUR = '840' and 200.0 < cast(regcap as double) and cast(regcap as double) <= 300.0 then '04'\n" +
                " when REGCAPCUR = '840' and 300.0 < cast(regcap as double) and cast(regcap as double) <= 400.0 then '05'\n" +
                " when REGCAPCUR = '840' and 400.0 < cast(regcap as double) and cast(regcap as double) <= 500.0 then '06'\n" +
                " when REGCAPCUR = '840' and 500.0 < cast(regcap as double) and cast(regcap as double) <= 600.0 then '07'\n" +
                " when REGCAPCUR = '840' and 600.0 < cast(regcap as double) and cast(regcap as double) <= 800.0 then '08'\n" +
                " when REGCAPCUR = '840' and 800.0 < cast(regcap as double) and cast(regcap as double) <= 1000.0 then '09'\n" +
                " when REGCAPCUR = '840' and 1000.0 < cast(regcap as double) and cast(regcap as double) <= 1500.0 then '10'\n" +
                " when REGCAPCUR = '840' and 1500.0 < cast(regcap as double) and cast(regcap as double) <= 2000.0 then '11'\n" +
                " when REGCAPCUR = '840' and 2000.0 < cast(regcap as double) and cast(regcap as double) <= 3000.0 then '12'\n" +
                " when REGCAPCUR = '840' and 3000.0 < cast(regcap as double) and cast(regcap as double) <= 5000.0 then '13'\n" +
                " when REGCAPCUR = '840' and 5000.0 < cast(regcap as double) and cast(regcap as double) <= 10000.0 then '14'\n" +
                " when REGCAPCUR = '840' and 10000.0 < cast(regcap as double) and cast(regcap as double) <= 20000.0 then '15'\n" +
                " when REGCAPCUR = '840' and 20000.0 < cast(regcap as double) and cast(regcap as double) <= 30000.0 then '16'\n" +
                " when REGCAPCUR = '840' and 30000.0 < cast(regcap as double) and cast(regcap as double) <= 50000.0 then '17'\n" +
                " when REGCAPCUR = '840' and 50000.0 < cast(regcap as double) and cast(regcap as double) <= 100000.0 then '18'\n" +
                " when REGCAPCUR = '840' and 100000.0 < cast(regcap as double) and cast(regcap as double) <= 800000.0 then '19'\n" +
                " when REGCAPCUR = '840' and 800000.0 < cast(regcap as double) and cast(regcap as double) <= 1000000.0 then '20'\n" +
                " when REGCAPCUR = '840' and 1000000.0 < cast(regcap as double) then '21'\n" +
                " else '-9' end as eb0007,\n" +
                "case when enttype in('3100','4100','4110','4200','4210','4300','4310','4400','4410')then '01' \n" +
                "when enttype in('3200','4120','4220','4320','4420') then '02'\n" +
                "when enttype = '4500' then '03'\n" +
                "when enttype in('9999','9500') then '04'\n" +
                "when enttype in('3500','4600') then '05'\n" +
                "when enttype in('1200','1210','1211','1212','1213','1219','1220','1221','1222','1223','1229','2200','2210','2211','2212','2213','2219','2220','2221','2222','2223','2229','3300','3400','4330','4340','4700') then '06'\n" +
                "when enttype in('4540','4560') then '07'\n" +
                "when enttype in('5000','5100','5110','5120','5130','5140','5150','5160','5190','5200','5210','5220','5230','5240','5290','5300','5310','5320','5390','5400','5410','5420','5430','5490','5800','5810','5820','5830','5840','5890','7000','7100','7110','7120','7130','7190','7200','7300','7310','7390') then '08'\n" +
                "when enttype in('6000','6100','6110','6120','6130','6140','6150','6160','6170','6190','6200','6210','6220','6230','6240','6250','6260','6290','6300','6310','6320','6390','6400','6410','6420','6430','6490','6800','6810','6820','6830','6840','6890')then '09'\n" +
                "when enttype in('1100','1110','1120','1121','1122','1123','1130','1140','1150','1151','1152','1153','1190','2100','2110','2120','2121','2122','2123','2130','2140','2150','2151','2152','2153','2190') then '10'\n" +
                "when enttype in('4530','4531','4532','4533','4550','4551','4552','4553') then '11'\n" +
                "when enttype in('1000','2000','3000','4000','8000','9000','9100','9200','9900') then '12'\n" +
                "when enttype is null or enttype = '!' or enttype = 'null' then '未知' else '未知' end as ee0043,\n"+
                " a.enttype as ee0007,\n" +
                " case  when a.enttype like '1%' or (a.enttype = '9900' and (a.entname not like '%分公司' or a.entname not like '%分店')) then '01' \n" +
                " when a.enttype like '2%' or (a.enttype = '9900' and (a.entname like '%分公司' or a.entname like '%分店')) then '02'  \n" +
                " when a.enttype like '3%' then '03'\n" +
                " when a.enttype like '4%' then '04'\n" +
                " when a.enttype like '5%,6%,7%' then '05'\n" +
                " when a.enttype in ('9100', '9200') then '06'\n" +
                " when a.enttype like '8%' then '08'\n" +
                " when a.enttype = '9500' then '09'  else '-9'  end as ee0008,\n" +
                " case  when a.enttype in ('2000', '2100', '2110', '2120', '2121', '2122', '2123', '2130', '2140', '2150', '2151', '2152', '2153', '2190', '2200', '2210', '2211', '2212', '2213', '2219', '2220', '2221', '2222', '2223', '2229', '4300', '4310', '4320', '4330', '4340', '4550', '4551', '4552', '4553', '4560') then '06' " +
                " when a.enttype is null or a.enttype in ('9500')   then '04'  " +
                " when a.enttype in ('1110', '1140', '1223', '3000', '3100', '3200', '3400', '3500', '4000', '4100', '4110', '4120', '4220', '4400', '4410', '4420') then '01' " +
                " when a.enttype in ('8000')   then '07'  " +
                " when a.enttype in ('9100', '9200')   then '05'  when a.enttype in    ('!', '1000', '1100', '1130', '1150', '1151', '1152', '1153', '1190', '1200', '1210', '1212', '1220', '1222', '1229', '4500', '4530', '4531', '4532', '4533', '4540', '4600', '4700', '9900') then '02' " +
                " when a.enttype in ('1120', '1121', '1122', '1123', '1221', '5000', '5100', '5110', '5120', '5130', '5140', '5150', '5160', '5190', '5210', '5220', '5230', '5310', '5320', '5400', '5410', '5430', '5490', '5800', '5810', '5820', '5840', '5890', '6100', '6110', '6120', '6130', '6140', '6150', '6160', '6170', '6190', '6210', '6220', '6230', '6320', '6390', '6400', '6410', '6430', '6800', '6810', '6820', '6830', '6840', '6890', '7110', '7120', '7130', '7200', '7300', '7310', '7390') then '03' " +
                " else '-9' end as ee0009,\n" +
                " case when c.code_type_name = '开发区代码' then '1' when c.code_type_name != '开发区代码' then '0' else '-9' end as ee0010,\n" +
                " a.industryphy as ee0011,\n" +
                " substr(industryco,1,2) as ee0012,\n" +
                " a.industryco as ee0013,\n" +
                " case when a.credlevel is null then '-9'  when a.credlevel = '!' then '-9' when a.credlevel = '99' then '-9' when a.credlevel = '' then '-9'  else a.credlevel end as ee0014,\n" +
                " case when a.parform is null then '-9' when a.parform = '!'then '-9' when a.parform = '' then '-9' when a.parform = '99' then '-9' else a.parform end as ee0015,\n" +
                " case when a.hypotaxis is null then '-9' when a.hypotaxis = '' then '-9' when a.hypotaxis = '99' then '-9' else a.hypotaxis end as ee0016,\n" +
                " case when a.sconform  is null then '-9' when a.sconform = '' then '-9' when a.sconform = '!' then '-9' when a.sconform = '99' then '-9' else a.sconform end as ee0017,\n" +
                " case when a.insform = '!' then '-9' when a.insform = '' then '-9' when a.insform is null then '-9' when a.insform = '99' then '-9' else a.insform end as ee0018,\n" +
                " case when a.tel regexp'^(13|15|18|17)[0-9]{9}$'\n" +
                " or a.tel regexp'^[(|（]?(13|15|18|17)[0-9]{9}[-|(|)|）|\\\\|*| |,|，|/]{1,6}+(13|15|18|17)[0-9]{9}'\n" +
                " or a.tel regexp'^[(|（]?(13|15|18|17)[0-9]{9}[-|(|)}）|\\\\|*| |,|，|/]{1,6}+(0[0-9]{2,3}[\\-|\\(\\\\|\\*|\\-|-|\\ ]?)?([2-9][0-9]{6,7})(\\-[0-9]{1,4})?'\n" +
                " or a.tel regexp'^[(|（]?(0[0-9]{2,3}[-|)|)|）|\\\\|*|\\-|-|\\ }/]?)?([2-9]{1}[0-9]{1}{5,7})(\\-[0-9]{1,4})?'\n" +
                " or a.tel regexp'^[(|（]?(0[0-9]{2,3}[-|)|)|）|\\\\|*|\\-|-|\\ ]?)?([2-9]{1}[0-9]{1}{5,7})(\\-[0-9]{1,4})?+(13|15|18|17)[0-9]{9}'\n" +
                " or a.tel regexp'^[(|（]?(0[0-9]{2,3}[-|)|)|）|\\\\|*|\\-|-|\\ ]?)?([2-9]{1}[0-9]{1}{5,7})(\\-[0-9]{1,4})?[-|(|)}）|\\\\|*| |,|，|/]+[(|（]?(0[0-9]{2,3}[-|)|)|）|\\\\|*|\\-|-|\\ ]?)?([2-9]{1}[0-9]{1}{5,7})(\\-[0-9]{1,4})?'\n" +
                " then '是' else '否' end as ee0019 ,\n" +
                " case when substr(a.regorg,1,4) = substr(a.oplocdistrict,1,4) and a.regorg <> '' and a.oplocdistrict <> '' then 1 when a.regorg = '' or a.regorg is null or a.oplocdistrict = '' or a.oplocdistrict is null then '-9' else 0 end as ee0006,\n" +
                " case when trim(b.empnum) is null then '-9' when trim(b.empnum) = '99' then '-9' when trim(b.empnum) = '' then '-9' else trim(b.empnum) end as ee0039,\n" +
                " case when trim(b.assgro) is null then '-9' when trim(b.assgro) = '99' then '-9' when trim(b.assgro) = '' then '-9' else  trim(b.assgro) end as ee0040,\n" +
                " case when trim(b.vendinc) is null then '-9' when trim(b.vendinc) = '99' then '-9' when trim(b.vendinc) = '' then '-9' else trim(b.vendinc) end as ee0041,\n" +
                " case when trim(b.ratgro) is null then '-9' when trim(b.ratgro) = '99' then '-9' when trim(b.ratgro) = '' then '-9' else trim(b.ratgro) end as ee0042,\n" +
                " case when trim(b.ros) is null then '-9' when trim(b.ros) = '99' then '-9' when trim(b.ros) = '' then '-9' else trim(b.ros) end as ee0044,\n" +
                " case when trim(b.nros) is null then '-9' when trim(b.nros) = '99' then '-9' when trim(b.nros) = '' then '-9' else trim(b.nros) end as ee0045,\n" +
                " case when trim(b.roe) is null then '-9' when trim(b.roe) = '99' then '-9' when trim(b.roe) = '' then '-9' else  trim(b.roe) end as ee0046,\n" +
                " case when trim(b.leverageratio) is null then '-9' when trim(b.leverageratio) = '99' then '-9' when trim(b.leverageratio) = '' then '-9' else trim(b.leverageratio) end as ee0047,\n" +
                " case when trim(b.pcpm) is null then '-9' when trim(b.pcpm) = '99' then '-9' when trim(b.pcpm) = '' then '-9' else trim(b.pcpm) end as ee0048,\n" +
                " case when trim(b.pcsi) is null then '-9' when trim(b.pcsi) = '99' then '-9'  when trim(b.pcsi) = '' then '-9' else trim(b.pcsi) end as ee0049,\n" +
                " case when b.yoyassetgrowth_t1 is null then '-9' when b.yoyassetgrowth_t1 = '99' then '-9' when b.yoyassetgrowth_t1 = '' then '-9' else round(b.yoyassetgrowth_t1,4) end as ee0050,\n" +
                " case when b.yoyrevenuegrowth_t1 is null then '-9' when b.yoyrevenuegrowth_t1 = '99' then '-9' when b.yoyrevenuegrowth_t1 = '' then '-9' else round(b.yoyrevenuegrowth_t1,4) end as ee0051,\n" +
                " case when b.yoynetincgrowth_t1 is null then '-9' when b.yoynetincgrowth_t1 = '99' then '-9' when b.yoynetincgrowth_t1 = '' then '-9' else round(b.yoynetincgrowth_t1,4) end as ee0052,\n" +
                " case when b.yoyleverageratiochange_t1 is null then '-9' when  b.yoyleverageratiochange_t1 = '99' then '-9' when b.yoyleverageratiochange_t1 = '' then '-9' else round(b.yoyleverageratiochange_t1,4) end as ee0053,\n" +
                " case when b.yoytaxgrowth_t1 is null then '-9' when b.yoytaxgrowth_t1 = '99' then '-9' when b.yoytaxgrowth_t1 = '' then '-9' else  round(b.yoytaxgrowth_t1,4) end as ee0054,\n" +
                " case when d.ent_scale is null then '-9' else d.ent_scale end as eb0005,\n" +
                " case when f.area_feature is null then '-9' when  f.area_feature = '99' then '-9' else f.area_feature end as eb0001,\n" +
                " case when f.area_level is null then '-9' when f.area_level = '99' then '-9' else f.area_level end as eb0002,\n" +
                " case when email is null then 0 else 1 end as ee0020,\n" +
                " case when g.industry_feature is null then '-9' else g.industry_feature end as eb0003," +
                " percent_rank() over(partition by a.s_ext_nodenum,substr(a.industryco,1,2) order by a.regcap) as eb0114\n" +//注册资本百分位 2018-01-8
                " from enterprisebaseinfocollect a\n" +
                " left join t_dex_app_codelist c on a.ecotecdevzone = c.codevalue\n" +
                " left join industry_ent_fluctuation g on concat(a.industryphy,substr(a.industryco,1,2)) = g.id\n" +
                " left join an_table b on a.pripid = b.pripid \n" +
                " left join an_ent_scale_2015 d on a.pripid = d.pripid\n" +
                " left join codelist_area e on a.regorg = e.codevalue\n" +
                " left join newent_area_distribution f on e.city = f.city\n" +
                " left join codelist_regorg h on a.regorg = h.regorg\n" +
                " where a.pripid <> ''";

        return DataFrameUtil.getDataFrame(spark, hql.replaceAll("datadate", datadate), "baseBiKpiTmp");
    }

}
