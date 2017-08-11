package com.chinadaas.association.etl.sparksql;
import com.chinadaas.common.util.DataFormatConvertUtil;
import com.chinadaas.common.util.DataFrameUtil;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

import java.io.Serializable;


public class EntRelationETL implements Serializable {
    private static final long serialVersionUID = -2950730482818667986L;
    //public static final String HIVE_SCHEMA = DataFormatConvertUtil.getSchema();
    public String date;
    public void setDate(String date) {
        this.date = date;
    }


    /**
     * 个人节点数据
     * ***********************************************************************************************
     *  规则：
     *  1、个人节点包含自然人股东（invtype为判别'20','21','22','30','35','36','77' 为个人）、高管、组织结构人员
     *  2、当zspid为空时，使用该人员的所在的公司pripir-name作为人员标志
     *  3、组织人员表当name长度小于4时标记为个人节点
     *  4、关联人员风险提示信息判别人员风险
     *  5、关联s_cif_indmap_hdfs_ext_表识别encode_v1
     *  6、将人员数据进行去重
     *  ***********************************************************************************************
     * @param sqlContext
     * @return
     */
    public DataFrame getPersonDataFrame(HiveContext sqlContext) {
        getPersonInfo01(sqlContext);
        getPersonInfo02(sqlContext);
        getPersonInfo03(sqlContext);
        getPersonInfo04(sqlContext);
        return getPersonInfo05(sqlContext);
    }


    private DataFrame getPersonInfo01(HiveContext sqlContext) {
        StringBuffer sql = new StringBuffer();
        sql.append(" SELECT case when (zspid='null' and pripid<>'null'  and inv<>'') then concat_ws('-',pripid,inv) else zspid end key, inv AS name                  ");
        sql.append("   FROM e_inv_investment_parquet ");
        sql.append(" WHERE  invtype in('20','21','22','30','35','36','77')  ");
        sql.append("   AND inv <> ''                                   ");
        sql.append(" UNION                                             ");
        sql.append(" SELECT case when (zspid='null' and  pripid<>'null' and name<>'') then concat_ws('-',pripid,name) else zspid end  key, name                         ");
        sql.append(" FROM entPersonTmp      ");
        sql.append(" WHERE name <> ''                                  ");
        sql.append(" UNION ");
        sql.append(" select distinct ");
        sql.append(" concat_ws('-', pri.pripid, pri.inv) ");
        sql.append("  as key,pri.inv ");
        sql.append(" from entOrgRelatgionTmp01 pri where length(inv) < 4");
        return DataFrameUtil.getDataFrame(sqlContext, sql.toString(), "personInfoTmp01");
    }


    //人员风险信息提示
    private DataFrame getPersonInfo02(HiveContext sqlContext) {
        String hql = " select a.*,\n" +
                "       case\n" +
                "         when b.fsx_name is not null then\n" +
                "          concat_ws('-', '失信被执行人', b.fsx_fbdate)\n" +
                "         else\n" +
                "          null\n" +
                "       end riskinfo1," +
                "      case\n" +
                "         when c.fss_name is not null then\n" +
                "          concat_ws('-', '被执行人', c.fss_time)\n" +
                "         else\n" +
                "          null\n" +
                "       end riskinfo2\n" +
                "  from personInfoTmp01 a\n" +
                "  left join (select max(fsx_fbdate) as fsx_fbdate, zspid,fsx_name\n" +
                "               from dis_sxbzxr_new_hdfs_ext_%s\n" +
                "              group by zspid,fsx_name) b\n" +
                "    on a.key = b.zspid" +
                "    and a.name=b.fsx_name " +
                "left join (select max(fss_time) as fss_time, zspid,fss_name\n" +
                "               from dis_bzxr_new_hdfs_ext_%s\n" +
                "              group by zspid,fss_name) c\n" +
                "    on a.key = c.zspid" +
                "    and a.name=c.fss_name ";
        return DataFrameUtil.getDataFrame(sqlContext, String.format(hql,date,date), "personInfoTmp02");
    }

    private DataFrame getPersonInfo03(HiveContext sqlContext) {
       String hql = " select a.key,a.name, case when a.riskinfo1 is not null or a.riskinfo2 is not null  " +
               "                            then concat_ws(' ',a.riskinfo1,a.riskinfo2)  else '' " +
               "                            end  riskinfo,b.encode_v1 from personInfoTmp02 a left join " +
               "                            s_cif_indmap_hdfs_ext_%s b" +
               "                            on a.key=b.zspid";
        return DataFrameUtil.getDataFrame(sqlContext, String.format(hql,date), "personInfoTmp03");
    }

    private DataFrame getPersonInfo04(HiveContext sqlContext) {
        StringBuffer sql = new StringBuffer();
        sql.append(" SELECT *, row_number() over(partition by key) rk  ");
        sql.append("   FROM personInfoTmp03                           ");
        return DataFrameUtil.getDataFrame(sqlContext, sql.toString(), "personInfoTmp04");
    }

    private DataFrame getPersonInfo05(HiveContext sqlContext) {
        StringBuffer sql = new StringBuffer();
        sql.append(" SELECT key,name,riskinfo,encode_v1 FROM  personInfoTmp04 b WHERE b.rk=1 ");
        return DataFrameUtil.getDataFrame(sqlContext, sql.toString(), "personInfoTmp05");
    }
    /**
     * 企业节点数据
     * @param sqlContext
     * @return
     */
    public DataFrame getEntDataFrame(HiveContext sqlContext) {
        getEntInfo04(sqlContext);
        return getEntInfo05(sqlContext);
    }

    public DataFrame getEntInfo01(HiveContext sqlContext) {
        String hql =" select s_ext_nodenum,\n" +
                "       pripid,\n" +
                "       entname,\n" +
                "       regno,\n" +
                "       enttype,\n" +
                "       industryphy,\n" +
                "       industryco,\n" +
                "       abuitem,\n" +
                "       opfrom,\n" +
                "       opto,\n" +
                "       postalcode,\n" +
                "       tel,\n" +
                "       email,\n" +
                "       esdate,\n" +
                "       apprdate,\n" +
                "       regorg,\n" +
                "       entstatus,\n" +
                "       regcap,\n" +
                "       opscope,\n" +
                "       opform,\n" +
                "       dom,\n" +
                "       reccap,\n" +
                "       regcapcur,\n" +
                "       forentname,\n" +
                "       country,\n" +
                "       entname_old,\n" +
                "       name,\n" +
                "       ancheyear,\n" +
                "       candate,\n" +
                "       revdate,\n" +
                "       case when (licid='' or licid='null' or licid is null) and length(credit_code)>17 then substr(credit_code,9,9) else licid end licid ,\n" +
                "       credit_code,\n" +
                "       case when (tax_code='' or tax_code='null' or tax_code is null) and length(credit_code)>17 then substr(credit_code,3,13) else tax_code end tax_code,\n" +
                "       zspid,\n" +
                "       empnum,\n" +
                "       cerno," +
                "       oriregno " +
                "  from enterprisebaseinfocollect_hdfs_ext_%s WHERE pripid <> ''";

        return DataFrameUtil.getDataFrame(sqlContext,String.format(hql,date) , "entInfoTmp03");
    }

    //企业风险提示
    private DataFrame getEntInfo04(HiveContext sqlContext) {
        String hql = "SELECT a.pripid,\n" +
                "       a.entname,\n" +
                "       a.regno,\n" +
                "       a.credit_code,\n" +
                "       a.esdate,\n" +
                "       a.industryphy,\n" +
                "       a.regcap,\n" +
                "       a.entstatus,\n" +
                "       a.regcapcur," +
                "       case\n" +
                "         when b.entname is not null then\n" +
                "          concat_ws('-', '企业经营异常名录', b.indate)\n" +
                "         else\n" +
                "          null\n" +
                "       end riskinfo1," +
                "         case\n" +
                "         when c.fsx_name is not null then\n" +
                "          concat_ws('-', '失信被执行人', c.fsx_fbdate)\n" +
                "         else\n" +
                "          null\n" +
                "       end riskinfo2," +
                "       case\n" +
                "         when d.fss_name is not null then\n" +
                "          concat_ws('-', '被执行人', d.fss_time)\n" +
                "         else\n" +
                "          null\n" +
                "       end riskinfo3\n" +
                "  from entInfoTmp03 a\n" +
                "  left join (select max(indate) as indate, entname\n" +
                "               from s_en_abnormity_hdfs_ext_%s\n" +
                "              group by entname) b\n" +
                "    on a.entname = b.entname " +
                "  left join (select max(fsx_fbdate) as fsx_fbdate, fsx_name\n" +
                "               from dis_sxbzxr_new_hdfs_ext_%s\n" +
                "              group by fsx_name) c\n" +
                "    on a.entname = c.fsx_name " +
                "  left join (select max(fss_time) as fss_time, fss_name\n" +
                "               from dis_bzxr_new_hdfs_ext_%s\n" +
                "              group by fss_name) d\n" +
                "    on a.entname = d.fss_name";
        return DataFrameUtil.getDataFrame(sqlContext, String.format(hql,date,date,date), "entInfoTmp04");
    }

    private DataFrame getEntInfo05(HiveContext sqlContext) {
        String hql = "SELECT pripid,\n" +
                "       entname,\n" +
                "       regno,\n" +
                "       credit_code,\n" +
                "       esdate,\n" +
                "       industryphy,\n" +
                "       regcap,\n" +
                "       entstatus,\n" +
                "       regcapcur," +
                "       case when riskinfo1 is not null or " +
                "                 riskinfo2 is not null or " +
                "                 riskinfo3 is not null " +
                "            then concat_ws(' ',riskinfo1,riskinfo2,riskinfo3) " +
                "            else '' end riskinfo \n" +
                "  FROM entInfoTmp04 b\n";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "entInfoTmp05");
    }



    /**
     * 人员地址节点
     * @param sqlContext
     * @return
     */
    public DataFrame getPersonAddrDataFrame(HiveContext sqlContext) {
        getPseronAddrInfo01(sqlContext);
        getPseronAddrInfo02(sqlContext);
        return getPseronAddrInfo03(sqlContext);
    }

    private DataFrame getPseronAddrInfo01(HiveContext sqlContext) {
        String hql = "select *\n" +
                "  from entPersonTmp a\n" +
               " where (length(a.DOM) > 4 AND\n" +
                "       (a.dom LIKE '%号%' OR a.dom LIKE '%村%组%' OR a.dom LIKE '%楼%') and\n" +
                "       a.dom not like '%集体%')";
        return DataFrameUtil.getDataFrame(sqlContext,hql , "personAddrInfoTmp01");
    }

    private DataFrame getPseronAddrInfo02(HiveContext sqlContext) {
        String hql = "select pre.dom,case\n" +
                "                         when (pre.zspid = 'null' and pre.pripid <> 'null' and\n" +
                "                              pre.name <> '') then\n" +
                "                          concat_ws('-', pre.pripid, pre.name)\n" +
                "                         else\n" +
                "                          pre.zspid\n" +
                "                       end zspid\n" +
                "  from personAddrInfoTmp01 pre\n" +
                " inner join entInfoTmp03 ent\n" +
                "    on pre.pripid = ent.pripid\n" ;
        return DataFrameUtil.getDataFrame(sqlContext, hql, "personAddrInfoTmp02",DataFrameUtil.CACHETABLE_EAGER);
    }

    private DataFrame getPseronAddrInfo03(HiveContext sqlContext) {
        String hql = "select dom\n" +
                "  from personAddrInfoTmp02 pri\n" +
                " where pri.dom is not null\n" +
                "   and pri.dom <> ''\n" +
                " group by dom\n" +
                "having count(distinct zspid) > 1 and count(distinct zspid) <= 5";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "personAddrInfoTmp03");
    }


    /**
     * 人员地址节点关系
     * @param sqlContext
     * @return
     */
    public DataFrame getPersonAddrRelaDF(HiveContext sqlContext) {
        return getPseronAddrRela(sqlContext);
    }

    private DataFrame getPseronAddrRela(HiveContext sqlContext) {
        String hql = "select pr.zspid, ai.dom\n" +
                "  from personAddrInfoTmp03 ai\n" +
                " inner join (select distinct dom,zspid from personAddrInfoTmp02 where zspid <>'null') pr\n" +
                "    on pr.dom = ai.dom " ;
        return DataFrameUtil.getDataFrame(sqlContext, hql, "personAddrRelaTmp01");
    }


    /**
     * 企业tel节点
     * @param sqlContext
     * @return
     */
    public DataFrame getTelInfoDF(HiveContext sqlContext) {
        getTelInfoDF01(sqlContext);
        return getTelInfoDF02(sqlContext);
    }

    private DataFrame getTelInfoDF01(HiveContext sqlContext) {
        String hql = " select tel, pripid, entstatus,enttype,industryphy\n" +
                "   from entInfoTmp03 \n" +
                "  where tel regexp '^(13|15|18|17)[0-9]{9}$'\n" +
                "     or tel regexp '^[(|（]?(13|15|18|17)[0-9]{9}[-|(|)|）|\\\\|*| |,|，|/]{1,6}+(13|15|18|17)[0-9]{9}'\n" +
                "     or tel regexp '^[(|（]?(13|15|18|17)[0-9]{9}[-|(|)}）|\\\\|*| |,|，|/]{1,6}+(0[0-9]{2,3}[\\-|\\(\\\\|\\*|\\-|-|\\ ]?)?([2-9][0-9]{6,7})(\\-[0-9]{1,4})?'\n" +
                "     or tel regexp '^[(|（]?(0[0-9]{2,3}[-|)|)|）|\\\\|*|\\-|-|\\ }/]?)?([2-9]{1}[0-9]{1}{5,7})(\\-[0-9]{1,4})?'\n" +
                "     or tel regexp '^[(|（]?(0[0-9]{2,3}[-|)|)|）|\\\\|*|\\-|-|\\ ]?)?([2-9]{1}[0-9]{1}{5,7})(\\-[0-9]{1,4})?+(13|15|18|17)[0-9]{9}'\n" +
                "     or tel regexp\n" +
                "  '^[(|（]?(0[0-9]{2,3}[-|)|)|）|\\\\|*|\\-|-|\\ ]?)?([2-9]{1}[0-9]{1}{5,7})(\\-[0-9]{1,4})?[-|(|)}）|\\\\|*| |,|，|/]+[(|（]?(0[0-9]{2,3}[-|)|)|）|\\\\|*|\\-|-|\\ ]?)?([2-9]{1}[0-9]{1}{5,7})(\\-[0-9]{1,4})?' \n";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "telInfoTmp01",DataFrameUtil.CACHETABLE_PARQUET);
    }

    private DataFrame getTelInfoDF02(HiveContext sqlContext) {
        String hql = "select a.tel\n" +
                "  from (select tel, pripid\n" +
                "          from telInfoTmp01\n" +
                "         where tel is not null\n" +
                "           and tel <> ''\n" +
                "           and length(tel) > 5) a \n" +
                " group by a.tel \n" +
                " having count(a.pripid) > 1 and count(a.pripid) <= 5";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "telInfoTmp021");
    }

    /**
     * 企业tel节点关系
     * @param sqlContext
     * @return
     */
    public DataFrame getTelRelaInfoDF(HiveContext sqlContext) {
        getTelRelaInfoDF02(sqlContext);
        DataFrame df = distinctEntRelationDF("telRelaInfoTmp02",sqlContext);
        df.cache().registerTempTable("telRelaResultInfoTmp02");
        return df;
    }

    private DataFrame getTelRelaInfoDF02(HiveContext sqlContext) {
        String hql = " select me.pripid, ai.tel as dtcpara\n" +
                "  from telInfoTmp02 ai\n" +
                " inner join (select distinct tel, pripid from telInfoTmp01) me\n" +
                "    on ai.tel = me.tel\n" +
                " where ai.tel is not null\n" +
                "   and ai.tel <> ''" +
                "   and ai.tel <> 'null' ";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "telRelaInfoTmp02");
    }

    public DataFrame getEntAndEntTelInfoDF(HiveContext sqlContext) {
        String hql = "select a.pripid,\n" +
                "         a.tel,\n" +
                "         b.pripid as entpripid,\n" +
                "         case\n" +
                "           when b.entstatus = '2' or b.entstatus = '3' or b.enttype = '4533' then\n" +
                "            0\n" +
                "           when a.industryphy = b.industryphy then\n" +
                "            0.5\n" +
                "           else\n" +
                "            0.3\n" +
                "         end riskscore\n" +
                "    from (select ent.entstatus,\n" +
                "                 ent.enttype,\n" +
                "                 ent.industryphy,\n" +
                "                 ent.pripid,\n" +
                "                 ent.tel \n" +
                "            from telInfoTmp01 ent\n" +
                "            join telRelaResultInfoTmp02 dom\n" +
                "              on dom.pripid = ent.pripid) a\n" +
                "    join (select ent.entstatus,\n" +
                "                 ent.enttype,\n" +
                "                 ent.industryphy,\n" +
                "                 ent.pripid,\n" +
                "                 ent.tel\n" +
                "            from telInfoTmp01 ent\n" +
                "            join telRelaResultInfoTmp02 dom\n" +
                "              on dom.pripid = ent.pripid) b\n" +
                "      on a.tel = b.tel\n" +
                "     and a.pripid <> b.pripid ";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "telRelaInfoTmp1111");
    }


    /**
     * 企业dom节点
     * @param sqlContext
     * @return
     */
    public DataFrame getEntDomInfoDF(HiveContext sqlContext) {
        getEntDomInfoDF01(sqlContext);
        return getEntDomInfoDF02(sqlContext);
    }

    private DataFrame getEntDomInfoDF01(HiveContext sqlContext) {
        String hql = "select dom,pripid,entstatus,enttype,industryphy \n" +
                "  from entInfoTmp03 a\n";
        String hql2 = hql +
                " where (length(a.DOM) > 4 AND\n" +
                "       (a.dom LIKE '%号%' OR a.dom LIKE '%村%组%' OR a.dom LIKE '%楼%'))\n" +
                "   and ((length(a.entname) > 3) OR\n" +
                "       a.entname LIKE '%公司%' OR a.entname LIKE '%室%' OR\n" +
                "       a.entname LIKE '%企业%')";
        return DataFrameUtil.getDataFrame(sqlContext, hql2, "entDomTmp01",DataFrameUtil.CACHETABLE_PARQUET);
    }

    private DataFrame getEntDomInfoDF02(HiveContext sqlContext) {
        String hql = "select dom\n" +
                "  from entDomTmp01\n" +
                "   where dom is not null\n" +
                "   and dom <> ''" +
                "   and dom <> 'null'\n" +
                " group by dom\n" +
                "having count(distinct pripid) > 1 and count(distinct pripid) <= 5 ";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "entDomTmp02");
    }

    /**
     * 企业dom节点关系
     * @param sqlContext
     * @return
     */
    public DataFrame getEntDomInfoRelaDF(HiveContext sqlContext) {
        getEntDomInfoRelaDF02(sqlContext);
        DataFrame df = distinctEntRelationDF("entDomRelaTmp02",sqlContext);
        df.cache().registerTempTable("entDomRelaResultTmp02");
        return df;
    }

    private DataFrame getEntDomInfoRelaDF02(HiveContext sqlContext) {
        String hql = "select me.pripid, ai.dom as dtcpara\n" +
                "  from entDomTmp02 ai\n" +
                " inner join (select distinct  dom, pripid from entDomTmp01 where pripid is not null " +
                " and pripid <> '' and pripid <> 'null' ) me\n" +
                "    on ai.dom = me.dom\n" ;
        return DataFrameUtil.getDataFrame(sqlContext, hql, "entDomRelaTmp02");
    }

    public DataFrame getEntAndEntDomInfoRelaDF(HiveContext sqlContext){
        String hql="select a.pripid,\n" +
                "         a.dom,\n" +
                "         b.pripid as entpripid,\n" +
                "         case\n" +
                "           when b.entstatus = '2' or b.entstatus = '3' or b.enttype = '4533' then\n" +
                "            0\n" +
                "           when a.industryphy = b.industryphy then\n" +
                "            0.5\n" +
                "           else\n" +
                "            0.3\n" +
                "         end riskscore\n" +
                "    from (select ent.entstatus,\n" +
                "                 ent.enttype,\n" +
                "                 ent.industryphy,\n" +
                "                 ent.pripid,\n" +
                "                 dom.dtcpara as dom\n" +
                "            from entDomTmp01 ent\n" +
                "            join entDomRelaResultTmp02 dom\n" +
                "              on dom.pripid = ent.pripid) a\n" +
                "    join (select ent.entstatus,\n" +
                "                 ent.enttype,\n" +
                "                 ent.industryphy,\n" +
                "                 ent.pripid,\n" +
                "                 dom.dtcpara as dom\n" +
                "            from entDomTmp01 ent\n" +
                "            join entDomRelaResultTmp02 dom\n" +
                "              on dom.pripid = ent.pripid) b\n" +
                "      on a.dom = b.dom\n" +
                "     and a.pripid <> b.pripid ";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "entDomRelaTmp11");
    }

    /**
     * 法人关系
     * @param sqlContext
     * @return
     */
    public DataFrame getLegalRelaDF(HiveContext sqlContext) {
        getLegalRelaDF01(sqlContext);
        distinctEntRelationDF("legalRelaTmp01",sqlContext);
        return getLegalRelaDF02(sqlContext);
    }

    private DataFrame getLegalRelaDF01(HiveContext sqlContext) {
        String hql = "select distinct case\n" +
                "                  when (pri.zspid = 'null' and pri.pripid <> 'null' and\n" +
                "                       pri.name <> '') then\n" +
                "                   concat_ws('-', pri.pripid, pri.name)\n" +
                "                  else\n" +
                "                   pri.zspid\n" +
                "                end dtcpara,\n" +
                "                pri.pripid\n" +
                "  from entPersonTmp pri\n" +
                " where pri.name <> ''" +
                "   and pri.name <> 'null'\n" +
                "   and pri.pripid <> 'null'\n" +
                "   and pri.pripid <> ''\n" +
                "   and pri.lerepsign = '1' " ;
        return DataFrameUtil.getDataFrame(sqlContext, hql, "legalRelaTmp01");
    }

    private DataFrame getLegalRelaDF02(HiveContext sqlContext) {
        String hql = "select \n" +
                "       pri.dtcpara as zspid,\n" +
                "       pri.pripid, " +
                "        case\n" +
                "         when ent.entstatus = '2' or ent.entstatus = '3' or\n" +
                "              ent.enttype = '4533' then\n" +
                "          0.1\n" +
                "         else\n" +
                "          1.0\n" +
                "       end riskscore\n" +
                "  from legalRelaTmp01tmp08 pri\n" +
                " inner join entInfoTmp03 ent\n" +
                "    on pri.pripid = ent.pripid" ;
        return DataFrameUtil.getDataFrame(sqlContext, hql, "legalRelaTmp02");
    }

    /**
     * 任职关系
     * @param sqlContext
     * @return
     */
    public DataFrame getStaffRelaDF(HiveContext sqlContext) {
        getStaffRelaDF011(sqlContext);
        getStaffRelaDF012(sqlContext);
        distinctEntRelationDF("staffRelaTmp012",sqlContext);

        return getStaffRelaDF02(sqlContext);
    }


    private DataFrame getStaffRelaDF011(HiveContext sqlContext) {
        String hql = "select distinct case\n" +
                "                  when (pri.zspid = 'null' and pri.pripid <> 'null' and\n" +
                "                       pri.name <> '') then\n" +
                "                   concat_ws('-', pri.pripid, pri.name)\n" +
                "                  else\n" +
                "                   pri.zspid\n" +
                "                end startkey,\n" +
                "                pri.position,\n" +
                "                pri.pripid \n" +
                "  from entPersonTmp pri\n" +
                " where pri.name <> ''\n" +
                "   and pri.name <> 'null' \n" +
                "   and pri.pripid <> 'null'\n" +
                "   and pri.pripid <> '' ";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "staffRelaTmp011",DataFrameUtil.CACHETABLE_PARQUET);
    }

    private DataFrame getStaffRelaDF012(HiveContext sqlContext) {
        String hql = "select concat_ws('-',startkey,position) as dtcpara,pripid from staffRelaTmp011";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "staffRelaTmp012");
    }

    private DataFrame getStaffRelaDF02(HiveContext sqlContext) {
        String hql = "select a.startkey,\n" +
                "       a.position,\n" +
                "       a.pripid as endkey,\n" +
                "       riskscore(2,\n" +
                "                 concat_ws('|', ent.entstatus, ent.enttype, a.position)) as riskscore\n" +
                "  from staffRelaTmp011 a\n" +
                "  join staffRelaTmp012tmp08 b\n" +
                "    on a.pripid = b.pripid" +
                "    and concat_ws('-',a.startkey,a.position)=b.dtcpara \n" +
                "  join entInfoTmp03 ent\n" +
                "    on a.pripid = ent.pripid";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "staffRelaTmp01");
    }

    /**
     * 企业投资关系
     * @param sqlContext
     * @return
     */
    public DataFrame getInvRelaDF(HiveContext sqlContext) {
//        getInvRelaDF02(sqlContext);
        getInvRelaDF03(sqlContext);
        getInvRelaDF04(sqlContext);
        distinctEntRelationDF("invRelaTmp031",sqlContext);
        getInvRelaDF041(sqlContext);
        distinctEntRelationDF("invRelaTmp041",sqlContext);
        return getInvRelaDF05(sqlContext);
    }


/*    private DataFrame getInvRelaDF02(HiveContext sqlContext) {
        String hql = "select distinct inv,\n" +
                "              condate,\n" +
                "              subconam,\n" +
                "              currency,\n" +
                "              conprop,\n" +
                "              blicno,\n" +
                "              pripid\n" +
                "from e_inv_investment_parquet \n";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "invRelaTmp02");
    }*/

    private DataFrame getInvRelaDF03(HiveContext sqlContext) {
        String hql = "select en.pripid   as startKey,\n" +
                "       hd.condate,\n" +
                "       hd.subconam,\n" +
                "       hd.currency,\n" +
                "       hd.conprop,\n" +
                "       hd.pripid   as endKey\n" +
                "  from (select distinct pripid, credit_code\n" +
                "                  from entInfoTmp03 \n" +
                "                 where credit_code <> '' and credit_code <> 'null') en," +
                " (select inv,condate,currency," +
                "         subconam,conprop,blicno,pripid " +
                "       from e_inv_investment_parquet where blicno<>'' and length(blicno)>17) hd\n" +
                " where hd.blicno = en.credit_code\n" +
                "   and en.pripid <> hd.pripid \n" +
                "union all\n" +
                "select en.pripid   as startKey,\n" +
                "       hd.condate,\n" +
                "       hd.subconam,\n" +
                "       hd.currency,\n" +
                "       hd.conprop,\n" +
                "       hd.pripid   as endKey\n" +
                "  from (select distinct pripid, entname\n" +
                "                          from entInfoTmp03 where entname<> '' and entname<>'null') en, " +
                "(select inv,condate,currency," +
                "        subconam,conprop,blicno,pripid " +
                " from e_inv_investment_parquet where inv<>'') hd\n" +
                " where hd.inv = en.entname " +
                " union all " +
                " select en.pripid   as startKey,\n" +
                "       hd.condate,\n" +
                "       hd.subconam,\n" +
                "       hd.currency,\n" +
                "       hd.conprop,\n" +
                "       hd.pripid   as endKey\n" +
                "  from (select distinct pripid, entname_old\n" +
                "                          from entInfoTmp03 where entname_old<> '' and entname_old<>'null') en," +
                " (select inv,condate,currency," +
                "          subconam,conprop,blicno,pripid " +
                "  from e_inv_investment_parquet where inv<>'') hd\n" +
                " where hd.inv = en.entname_old ";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "invRelaTmp03",DataFrameUtil.CACHETABLE_PARQUET);
    }

    private DataFrame getInvRelaDF04(HiveContext sqlContext) {
        String hql = "select concat_ws('-',startKey,condate,subconam,currency,conprop) as dtcpara, endKey as pripid from invRelaTmp03 ";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "invRelaTmp031");
    }


    private DataFrame getInvRelaDF041(HiveContext sqlContext){
        String hql =
                "select concat_ws('-',a.endKey,a.condate,a.subconam,a.currency,a.conprop) as dtcpara, a.startKey as pripid  \n" +
                        "from invRelaTmp03 a\n" +
                        "join invRelaTmp031tmp08 b\n" +
                        "  on a.endKey = b.pripid\n" +
                        " and concat_ws('-',\n" +
                        "               a.startKey,\n" +
                        "               a.condate,\n" +
                        "               a.subconam,\n" +
                        "               a.currency,\n" +
                        "               a.conprop) = b.dtcpara\n";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "invRelaTmp041");
    }


   //TODO 改成配置表的方式挡板功能
    private DataFrame getInvRelaDF05(HiveContext sqlContext) {
        String hql = "select distinct a.startKey, a.condate, a.subconam, a.currency, a.conprop, a.endKey,riskscore(1,concat_ws('|',b.entstatus,b.enttype,a.conprop)) as riskscore\n" +
                "  from invRelaTmp03 a " +
                "  join entInfoTmp03 b " +
                "  on a.endKey=b.pripid " +
                "  join invRelaTmp041tmp08 c " +
                "  on a.startKey=c.pripid " +
                "  and concat_ws('-',\n" +
                "               a.endKey,\n" +
                "               a.condate,\n" +
                "               a.subconam,\n" +
                "               a.currency,\n" +
                "               a.conprop) = c.dtcpara" +
                " where a.startKey <> 'A-1f6bdca0-35f3-4ce7-b4f0-1ccf1e4b2539'\n";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "invRelaTmp04");
    }



   /* *//**
     * 企业控股股东
     * @param sqlContext
     * @return
     *//*
    public DataFrame getInvHoldRelaDF(HiveContext sqlContext) {
        getInvHoldRelaDF01(sqlContext);
     return   getInvHoldRelaDF02(sqlContext);
    }

    private DataFrame getInvHoldRelaDF01(HiveContext sqlContext) {
        String hql = "select max(conprop) as conprop, endkey\n" +
                "          from invRelaTmp04\n" +
                "         group by endkey";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "invHoldRelaTmp01");
    }

    private DataFrame getInvHoldRelaDF02(HiveContext sqlContext) {
        String hql = "select distinct b.startKey, b.condate, b.subconam, b.currency, b.conprop, b.endKey,b.riskscore\n" +
                "  from invHoldRelaTmp01 a" +
                " inner join invRelaTmp04 b\n" +
                "    on a.conprop = b.conprop\n" +
                "   and a.endkey = b.endkey";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "invHoldRelaTmp02");
    }
*/
    /**
     * 企业参股股东
     * @param sqlContext
     * @return
     */
    public DataFrame getInvJoinRelaDF(HiveContext sqlContext) {
        getPersonJoinRelaDF01(sqlContext);
        getInvJoinRelaDF01(sqlContext);
        getInvJoinRelaDF02(sqlContext);
        getInvJoinRelaDF03(sqlContext);
        getInvJoinRelaDF04(sqlContext);
     return   getInvJoinRelaDF05(sqlContext);
    }

    public DataFrame getPersonJoinRelaDF01(HiveContext sqlContext) {
        String hql = "select startKey, position, endkey\n" +
                "          from staffRelaTmp01\n" +
                "         where position in ('410A',\n" +
                "                           '410B',\n" +
                "                           '410C',\n" +
                "                           '410D',\n" +
                "                           '410E',\n" +
                "                           '410F',\n" +
                "                           '410G',\n" +
                "                           '410Z',\n" +
                "                           '430A',\n" +
                "                           '431A',\n" +
                "                           '432K',\n" +
                "                           '433A',\n" +
                "                           '433B',\n" +
                "                           '434Q',\n" +
                "                           '436A',\n" +
                "                           '441A',\n" +
                "                           '441B',\n" +
                "                           '441C',\n" +
                "                           '441D',\n" +
                "                           '441E',\n" +
                "                           '441F',\n" +
                "                           '441G',\n" +
                "                           '442G',\n" +
                "                           '451D',\n" +
                "                           '490A',\n" +
                "                           '491A')";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "personJoinRelaTmp01");
    }

    private DataFrame getInvJoinRelaDF01(HiveContext sqlContext) {
        String hql = "select startkey,endkey\n" +
                "  from invRelaTmp04 \n" +
                " where conprop > 0.2\n" +
                "   and conprop < 0.5";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "invJoinRelaTmp01");
    }

    private DataFrame getInvJoinRelaDF02(HiveContext sqlContext) {
        String hql = "select a.startkey, a.endkey, b.startkey as person_id\n" +
                "  from invJoinRelaTmp01 a\n" +
                "  join personJoinRelaTmp01  b\n" +
                "    on a.startkey = b.endkey";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "invJoinRelaTmp02",DataFrameUtil.CACHETABLE_LAZY);
    }

    private DataFrame getInvJoinRelaDF03(HiveContext sqlContext) {
        String hql = "select a.endkey, b.startkey as person_id\n" +
                "   from invJoinRelaTmp02 a\n" +
                "   join personJoinRelaTmp01 b\n" +
                "     on a.endkey = b.endkey";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "invJoinRelaTmp03");
    }
    private DataFrame getInvJoinRelaDF04(HiveContext sqlContext) {
        String hql = " select distinct a.startkey, a.endkey\n" +
                "   from invJoinRelaTmp02 a\n" +
                "   join invJoinRelaTmp03 b\n" +
                "     on a.endkey = b.endkey\n" +
                "    and a.person_id = b.person_id";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "invJoinRelaTmp04");
    }
     private DataFrame getInvJoinRelaDF05(HiveContext sqlContext) {
        String hql = "select distinct a.* from invRelaTmp04 a join invJoinRelaTmp04 b on  a.startkey=b.startkey and a.endkey=b.endkey";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "invJoinRelaTmp05");
    }


    /**
     * 人员投资
     * @param sqlContext
     * @return
     */
    public DataFrame getPersonInv(HiveContext sqlContext){
        getPersonInv01(sqlContext);
        getPersonInv02(sqlContext);
        distinctEntRelationDF("personInvTmp012",sqlContext);

        return getPersonInv03(sqlContext);
    }

    private DataFrame getPersonInv01(HiveContext sqlContext){
        String hql =
                " select distinct case\n" +
                        "                  when (pri.zspid = 'null' and pri.pripid <> 'null' and\n" +
                        "                       pri.inv <> '') then\n" +
                        "                   concat_ws('-', pri.pripid, pri.inv)\n" +
                        "                  else\n" +
                        "                   pri.zspid\n" +
                        "                end startKey,\n" +
                        "                pri.condate,\n" +
                        "                pri.subconam,\n" +
                        "                pri.currency,\n" +
                        "                pri.conprop,\n" +
                        "                pri.pripid as endKey\n" +
                        "  from e_inv_investment_parquet pri\n" +
                        " where pri.inv <> '' " +
                        "   and pri.inv <> 'null'\n" +
                        "   and pri.invtype in ('20', '21', '22', '30', '35', '36','77')\n" +
                        "   and pri.pripid <> 'null'\n" +
                        "   and pri.pripid <> ''";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "personInvTmp011",DataFrameUtil.CACHETABLE_PARQUET);
    }


    private DataFrame getPersonInv02(HiveContext sqlContext){
        String hql =
                " select concat_ws('-',startKey,condate,subconam,currency,conprop) as dtcpara, endKey as pripid from personInvTmp011";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "personInvTmp012");
    }

    private DataFrame getPersonInv03(HiveContext sqlContext){
        String hql =
                "select a.*,\n" +
                        "     riskscore(1,\n" +
                        "               concat_ws('|', ent.entstatus, ent.enttype, a.conprop)) as riskscore\n" +
                        "from personInvTmp011 a\n" +
                        "join personInvTmp012tmp08 b\n" +
                        "  on a.endKey = b.pripid\n" +
                        " and concat_ws('-',\n" +
                        "               a.startKey,\n" +
                        "               a.condate,\n" +
                        "               a.subconam,\n" +
                        "               a.currency,\n" +
                        "               a.conprop) = b.dtcpara\n" +
                        "join entInfoTmp03 ent\n" +
                        "  on a.endKey = ent.pripid ";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "personInvTmp111");
    }



    /**
     * 自然人参股股东
     * @param sqlContext
     */
    public DataFrame getPersonJoinRelaDF(HiveContext sqlContext) {
        getPersonJoinRelaDF02(sqlContext);
        return   getPersonJoinRelaDF03(sqlContext);
    }



    private DataFrame getPersonJoinRelaDF02(HiveContext sqlContext) {
        String hql = "select startkey,endkey\n" +
                "  from personInvTmp01\n" +
                " where conprop > 0.2\n" +
                "   and conprop < 0.5 ";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "personJoinRelaTmp02");
    }

    private DataFrame getPersonJoinRelaDF03(HiveContext sqlContext) {
        String hql = "select c.* from \n" +
                " personJoinRelaTmp02 a\n" +
                "  join personJoinRelaTmp01 b\n" +
                "    on a.endkey = b.endkey\n" +
                "    and a.startkey=b.startkey" +
                "  join personInvTmp01 c " +
                "    on a.startkey=c.startkey" +
                "    and a.endkey=c.endkey";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "personJoinRelaTmp03");
    }


    /**
     * 人员相关关系合并为一条关系（inv，staff，legal）
     * @param sqlContext
     * @return
     */
    public DataFrame getPersonMergeRelaDF(HiveContext sqlContext) {
        getPersonMergeRelaDF01(sqlContext);
        return   getPersonMergeRelaDF02(sqlContext);
    }

    private DataFrame getPersonMergeRelaDF01(HiveContext sqlContext){
        String hql = " select startKey, riskscore, endKey\n" +
                "  from personInvTmp01\n" +
                "union all\n" +
                "select zspid as startKey, riskscore, pripid as endKey\n" +
                "  from legalRelaTmp02\n" +
                "union all\n" +
                "select startKey, riskscore, endKey\n" +
                "  from staffRelaTmp01 ";
        return  DataFrameUtil.getDataFrame(sqlContext, hql, "personMergeRelationTm01");
    }


    private DataFrame getPersonMergeRelaDF02(HiveContext sqlContext){
        String hql = " select a.startKey,\n" +
                "       case\n" +
                "         when a.riskscore > 1 then\n" +
                "          1\n" +
                "         else\n" +
                "          a.riskscore\n" +
                "       end riskscore,\n" +
                "       a.endKey\n" +
                "  from (select startKey, sum(riskscore) as riskscore, endKey\n" +
                "          from personMergeRelationTm01\n" +
                "         group by startKey, endKey) a";
        return  DataFrameUtil.getDataFrame(sqlContext, hql, "personMergeRelationTm02");
    }


    /**
     * 企业相关关系合并为一条关系（inv，疑似关系）
     * @param sqlContext
     * @return
     */
    public DataFrame getInvMergeRelationDF(HiveContext sqlContext) {
        getInvMergeRelationDF01(sqlContext);
        return   getInvMergeRelationDF02(sqlContext);
    }

    private DataFrame getInvMergeRelationDF01(HiveContext sqlContext){
        String hql = "select startKey, riskscore, endKey\n" +
                "  from invRelaTmp04\n" +
                "union all\n" +
                "select pripid as startKey, riskscore, entpripid as endKey\n" +
                "  from entDomRelaTmp11\n" +
                "union all\n" +
                "select pripid as startKey, riskscore, entpripid as endKey\n" +
                "  from telRelaInfoTmp111";
        return  DataFrameUtil.getDataFrame(sqlContext, hql, "invMergeRelationTm01");
    }

    private DataFrame getInvMergeRelationDF02(HiveContext sqlContext){

        String hql = " select a.startKey,\n" +
                "       case\n" +
                "         when a.riskscore > 1 then\n" +
                "          1\n" +
                "         else\n" +
                "          a.riskscore\n" +
                "       end riskscore,\n" +
                "       a.endKey\n" +
                "  from (select startKey, sum(riskscore) as riskscore, endKey\n" +
                "          from invMergeRelationTm01\n" +
                "         group by startKey, endKey) a";

        return  DataFrameUtil.getDataFrame(sqlContext, hql, "invMergeRelationTm");
    }


    /**
     * 重复数据的处理
     * @param tableName
     * @param sqlContext
     * @return
     */
    public DataFrame distinctEntRelationDF(String tableName,HiveContext sqlContext){
        distinctEntRelationDF01(tableName,sqlContext);
        distinctEntRelationDF02(tableName,sqlContext);
        distinctEntRelationDF03(tableName,sqlContext);
        distinctEntRelationDF04(tableName,sqlContext);
        distinctEntRelationDF05(tableName,sqlContext);
        distinctEntRelationDF06(tableName,sqlContext);
        distinctEntRelationDF071(tableName,sqlContext);
        DataFrame df= distinctEntRelationDF08(tableName,sqlContext);
        return df;
    }


        private DataFrame distinctEntRelationDF01(String tableName,HiveContext sqlContext){
        String hql = "select a.pripid, a.entstatus, a.entname, a.credit_code, a.regno, b.dtcpara\n" +
                "  from entInfoTmp03 a\n" +
                "  join "+tableName+" b\n" +
                "    on a.pripid = b.pripid";
        return DataFrameUtil.getDataFrame(sqlContext,hql,tableName+"tmp01",DataFrameUtil.CACHETABLE_PARQUET);
        }

    private DataFrame distinctEntRelationDF02(String tableName,HiveContext sqlContext){
        String hql = "select count(*), entname, credit_code, regno, dtcpara\n" +
                "  from "+tableName+"tmp01\n" +
                " group by entname, credit_code, regno, dtcpara\n" +
                " having count(*) > 1";
        return DataFrameUtil.getDataFrame(sqlContext,hql,tableName+"tmp02",DataFrameUtil.CACHETABLE_PARQUET);
    }

    private DataFrame distinctEntRelationDF03(String tableName,HiveContext sqlContext){
        String hql = "select a.*\n" +
                "  from "+tableName+"tmp01 a\n" +
                "  join "+tableName+"tmp02 b\n" +
                "    on a.credit_code = b.credit_code\n" +
                "   and a.regno = b.regno\n" +
                "   and a.entname = b.entname\n" +
                "   and a.dtcpara = b.dtcpara\n" +
                " where a.entstatus = '1'";
        return DataFrameUtil.getDataFrame(sqlContext,hql,tableName+"tmp03",DataFrameUtil.CACHETABLE_PARQUET);
    }

    private DataFrame distinctEntRelationDF04(String tableName,HiveContext sqlContext){
        String hql = "select count(*), entname, credit_code, regno, dtcpara\n" +
                "  from "+tableName+"tmp03 \n" +
                " group by entname, credit_code, regno, dtcpara\n" +
                "having count(*) > 1";

        return DataFrameUtil.getDataFrame(sqlContext,hql,tableName+"tmp04");
    }

    private DataFrame distinctEntRelationDF05(String tableName,HiveContext sqlContext){
        String hql = "select a.*\n" +
                "  from "+tableName+"tmp01 a\n" +
                "  join "+tableName+"tmp04 b\n" +
                "    on a.credit_code = b.credit_code\n" +
                "   and a.regno = b.regno\n" +
                "   and a.entname = b.entname\n" +
                "   and a.dtcpara = b.dtcpara";

        return DataFrameUtil.getDataFrame(sqlContext,hql,tableName+"tmp05");
    }

    private DataFrame distinctEntRelationDF06(String tableName,HiveContext sqlContext){
        String hql =
                "select b.pripid, b.entstatus, b.entname, b.credit_code, b.regno, b.dtcpara\n" +
                "  from (select *, row_number() over(partition by key order by a.pripid) rk\n" +
                "          from (select pripid,\n" +
                "                       entstatus,\n" +
                "                       entname,\n" +
                "                       credit_code,\n" +
                "                       regno,\n" +
                "                       dtcpara,\n" +
                "                       concat_ws('-', entname, credit_code, regno, dtcpara) as key\n" +
                "                  from "+tableName+"tmp05) a) b\n" +
                " where b.rk = 1";

        return DataFrameUtil.getDataFrame(sqlContext,hql,tableName+"tmp06");
    }

    private DataFrame distinctEntRelationDF071(String tableName,HiveContext sqlContext){
        String hql =
                "select b.*\n" +
                        "  from (select count(*), entname, credit_code, regno, dtcpara\n" +
                        "          from "+tableName+"tmp03\n" +
                        "         group by entname, credit_code, regno, dtcpara\n" +
                        "        having count(*) = 1) a\n" +
                        "  join "+tableName+"tmp01 b\n" +
                        "    on a.entname = b.entname\n" +
                        "   and a.credit_code = b.credit_code\n" +
                        "   and a.regno = b.regno\n" +
                        "   and a.dtcpara = b.dtcpara\n" +
                        " where entstatus = '1' ";
        System.out.println(hql);

        return DataFrameUtil.getDataFrame(sqlContext,hql,tableName+"tmp071");
    }

    private DataFrame distinctEntRelationDF08(String tableName,HiveContext sqlContext){
        String hql = "select a.pripid, a.dtcpara\n" +
                "  from (select a.*, b.entname as nameb\n" +
                "          from "+tableName+"tmp01 a\n" +
                "          left join "+tableName+"tmp02 b\n" +
                "            on a.credit_code = b.credit_code\n" +
                "           and a.regno = b.regno\n" +
                "           and a.entname = b.entname\n" +
                "           and a.dtcpara = b.dtcpara) a\n" +
                " where a.nameb is null\n" +
                "union all\n" +
                "select b.pripid, b.dtcpara\n" +
                "  from "+tableName+"tmp06 b" +
                " union all " +
                "select c.pripid, c.dtcpara " +
                "  from "+tableName+"tmp071 c";

        return DataFrameUtil.getDataFrame(sqlContext,hql,tableName+"tmp08");
    }


    /**
     * 分支机构关系
     * @param sqlContext
     * @return
     */
    public DataFrame getBranchRelation(HiveContext sqlContext){
        String hql = "select pripid,brn_rpripid from  s_en_brn_org_hdfs_ext_%s where brn_rpripid <> 'null' " +
                "     union " +
                "     select a.pripid,b.pripid as brn_rpripid from s_en_brn_org_hdfs_ext_%s a join entInfoTmp03 b" +
                "     on a.brn_entname=b.entname and a.brn_regno=b.regno" +
                "     where a.brn_rpripid<>'null'" +
                "     and  a.brn_entname <>'null'";
        return DataFrameUtil.getDataFrame(sqlContext,String.format(hql,date,date),"branchRelationTmp01");
    }


    public DataFrame getEntOrgRelatgion(HiveContext sqlContext){
        getEntOrgRelatgion01(sqlContext);
        return getEntOrgRelatgion02(sqlContext);
    }


    private DataFrame getEntOrgRelatgion01(HiveContext sqlContext){
        String hql =
                "select c.condate, c.inv, c.subconam, c.currency, c.conprop, c.pripid,c.zspid \n" +
                "  from (select a.condate, a.inv, a.subconam, a.currency, a.conprop, a.pripid,b.entname,a.zspid\n" +
                "          from e_inv_investment_parquet a\n" +
                "          left join (select a.startKey, a.endKey, b.entname,b.entname_old\n" +
                "                      from invRelaTmp04 a\n" +
                "                      join entInfoTmp03 b\n" +
                "                        on a.startKey = b.pripid) b\n" +
                "           on a.pripid = b.endKey " +
                "           and (a.inv = b.entname or a.inv = b.entname_old)\n" +
                "         where a.invtype in ('10',\n" +
                "                             '11',\n" +
                "                             '12',\n" +
                "                             '13',\n" +
                "                             '14',\n" +
                "                             '15',\n" +
                "                             '31',\n" +
                "                             '32',\n" +
                "                             '33',\n" +
                "                             '34',\n" +
                "                             '40',\n" +
                "                             '50',\n" +
                "                             '90','88')) c\n" +
                " where c.entname is null ";

        return DataFrameUtil.getDataFrame(sqlContext,hql,"entOrgRelatgionTmp01",DataFrameUtil.CACHETABLE_EAGER);
    }

    private DataFrame getEntOrgRelatgion02(HiveContext sqlContext){
        String hql = " select distinct concat_ws('-', org.pripid, org.inv) as startKey,\n" +
                "       org.condate,\n" +
                "       org.subconam,\n" +
                "       org.currency,\n" +
                "       org.conprop,\n" +
                "       org.pripid as endKey," +
                "        riskscore(1,concat_ws('|', ent.entstatus, ent.enttype, org.conprop)) as riskscore\n" +
                "  from entOrgRelatgionTmp01 org " +
                "  join  entInfoTmp03 ent " +
                "  on  org.pripid = ent.pripid \n" +
                " where length(org.inv) >= 4  ";
        return DataFrameUtil.getDataFrame(sqlContext,hql,"entOrgRelatgionTmp02");
    }

    public DataFrame getpersonOrgRelation(HiveContext sqlContext){
        String hql = " select distinct \n" +
                "                   concat_ws('-', pri.pripid, pri.inv) \n" +
                "                as startKey,\n" +
                "                pri.condate,\n" +
                "                pri.subconam,\n" +
                "                pri.currency,\n" +
                "                pri.conprop,\n" +
                "                pri.pripid as endKey," +
                "                riskscore(1,concat_ws('|', ent.entstatus, ent.enttype, pri.conprop)) as riskscore\n" +
                "  from entOrgRelatgionTmp01 pri " +
                "  join entInfoTmp03 ent " +
                "  on  pri.pripid = ent.pripid\n" +
                " where length(inv) < 4 " +
                " union" +
                " select * from personInvTmp01 ";
        return DataFrameUtil.getDataFrame(sqlContext,hql,"entOrgRelatgionTmp03");
    }

    public DataFrame getOrgNode(HiveContext sqlContext){
        String hql = "select distinct concat_ws('-', pripid, inv) as key, inv as name,'org' as type\n" +
                "   from entOrgRelatgionTmp01\n" +
                "  where length(inv) >= 4";
        return DataFrameUtil.getDataFrame(sqlContext,hql,"orgNodeTmpe01");
    }

    private DataFrame getHoldRelation01(HiveContext sqlContext){
        String hql = "select a.*,'person' as flag from entOrgRelatgionTmp03 a" +
                "  union all " +
                "    select b.* ,'ent' as flag from invRelaTmp04 b    " +
                "   union all " +
                "    select c.*,'entorg' as flag from  entOrgRelatgionTmp02 c" ;
        return DataFrameUtil.getDataFrame(sqlContext, hql, "holdRelationTmp01",DataFrameUtil.CACHETABLE_LAZY);
    }

    private DataFrame getHoldRelation02(HiveContext sqlContext){
        String hql = "select max(conprop) as conprop, endkey\n" +
                "          from holdRelationTmp01\n" +
                "         group by endkey";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "holdRelationTmp02");
    }

    private DataFrame getHoldRelation03(HiveContext sqlContext){
        String hql = "select b.startKey, b.condate, b.subconam, b.currency, b.conprop, b.endKey,b.flag \n" +
                "  from holdRelationTmp02 a" +
                " inner join holdRelationTmp01 b\n" +
                "    on a.conprop = b.conprop\n" +
                "   and a.endkey = b.endkey";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "holdRelationTmp011");
    }


    /**
     * 自然人控股股东
     * @param sqlContext
     * @return
     */
    public DataFrame getPersonHoldRelaDF(HiveContext sqlContext) {
        getHoldRelation(sqlContext);
        return   getPersonHoldRelaDF01(sqlContext);
    }

    private DataFrame getPersonHoldRelaDF01(HiveContext sqlContext) {
        String hql = " select distinct b.startKey, b.condate, b.subconam, b.currency, b.conprop, b.endKey from holdRelationTmp011 b where b.flag='person' and  b.conprop<>0.0";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "personHoldRelaTmp01");
    }

    public DataFrame getHoldRelation(HiveContext sqlContext){
        getHoldRelation01(sqlContext);
        getHoldRelation02(sqlContext);
        return   getHoldRelation03(sqlContext);

    }


    /**
     * 企业控股股东
     * @param sqlContext
     * @return
     */
    public DataFrame getInvHoldRelaDF(HiveContext sqlContext) {
        return   getInvHoldRelaDF01(sqlContext);
    }

    private DataFrame getInvHoldRelaDF01(HiveContext sqlContext) {
        String hql = " select distinct b.startKey, b.condate, b.subconam, b.currency, b.conprop, b.endKey from holdRelationTmp011 b where b.flag='ent' and b.conprop<>0.0";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "invHoldRelaTmp01");
    }


    /**
     * 组织机构控股股东
     * @param sqlContext
     * @return
     */
    public DataFrame getOrgHoldRelaDF(HiveContext sqlContext) {
        String hql =" select distinct b.startKey, b.condate, b.subconam, b.currency, b.conprop, b.endKey from holdRelationTmp011 b where b.flag='entorg' and  b.conprop<>0.0";
        return   DataFrameUtil.getDataFrame(sqlContext, hql, "orgHoldRelaTmp01");
    }

    /**
     * 获取主要管理职位
     */
    public DataFrame mainStaff(HiveContext sqlContext){
        String hql="select *\n" +
                "  from staffRelationTmp\n" +
                " where position in ('410A',\n" +
                "                    '410B',\n" +
                "                    '410C',\n" +
                "                    '410D',\n" +
                "                    '410E',\n" +
                "                    '410F',\n" +
                "                    '410G',\n" +
                "                    '410Z',\n" +
                "                    '430A',\n" +
                "                    '431A',\n" +
                "                    '431B',\n" +
                "                    '432A',\n" +
                "                    '432K',\n" +
                "                    '433A',\n" +
                "                    '433B',\n" +
                "                    '434Q',\n" +
                "                    '441A',\n" +
                "                    '441B',\n" +
                "                    '441C',\n" +
                "                    '441D',\n" +
                "                    '441E',\n" +
                "                    '441F',\n" +
                "                    '441G',\n" +
                "                    '442G',\n" +
                "                    '451C',\n" +
                "                    '451D',\n" +
                "                    '451E',\n" +
                "                    '490A',\n" +
                "                    '491A',\n" +
                "                    '493A',\n" +
                "                    'A004',\n" +
                "                    'A005',\n" +
                "                    'A015',\n" +
                "                    'A016',\n" +
                "                    'A017',\n" +
                "                    'A019',\n" +
                "                    'A022',\n" +
                "                    'A023',\n" +
                "                    'A025',\n" +
                "                    'A027',\n" +
                "                    'A029',\n" +
                "                    'A030',\n" +
                "                    'A033',\n" +
                "                    'A034',\n" +
                "                    'A041',\n" +
                "                    'A042',\n" +
                "                    'A043',\n" +
                "                    'A044',\n" +
                "                    'A045',\n" +
                "                    'A046',\n" +
                "                    'A047',\n" +
                "                    'A048',\n" +
                "                    'A049',\n" +
                "                    'A050',\n" +
                "                    'A051',\n" +
                "                    'A052',\n" +
                "                    'A053',\n" +
                "                    'A057',\n" +
                "                    'A139',\n" +
                "                    'A140',\n" +
                "                    'A141',\n" +
                "                    'A143',\n" +
                "                    'A147',\n" +
                "                    'A149',\n" +
                "                    'A150',\n" +
                "                    'A152',\n" +
                "                    'A153',\n" +
                "                    'A154',\n" +
                "                    'A155',\n" +
                "                    'A156',\n" +
                "                    'A157',\n" +
                "                    'A158',\n" +
                "                    'A160',\n" +
                "                    'A161',\n" +
                "                    'A162',\n" +
                "                    'A163',\n" +
                "                    'A165',\n" +
                "                    'A169',\n" +
                "                    'A170',\n" +
                "                    'A175',\n" +
                "                    'A176',\n" +
                "                    'A186',\n" +
                "                    'A187',\n" +
                "                    'A188',\n" +
                "                    'A189',\n" +
                "                    'A190',\n" +
                "                    'A191',\n" +
                "                    'A192',\n" +
                "                    'A193',\n" +
                "                    'A197',\n" +
                "                    'A198',\n" +
                "                    'A199',\n" +
                "                    'A200',\n" +
                "                    'A201',\n" +
                "                    'A204',\n" +
                "                    'A210',\n" +
                "                    'A222',\n" +
                "                    'A236',\n" +
                "                    'A237',\n" +
                "                    'A242')";

        return   DataFrameUtil.getDataFrame(sqlContext, hql, "mainRelaTmp01");
    }

}
