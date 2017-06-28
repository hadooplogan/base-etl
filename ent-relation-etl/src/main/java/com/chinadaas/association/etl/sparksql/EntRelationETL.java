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
        sql.append(" WHERE  invtype in('20','21','22','30','35','36')  ");
        sql.append("   AND inv <> ''                                   ");
        sql.append(" UNION                                             ");
        sql.append(" SELECT case when (zspid='null' and  pripid<>'null' and name<>'') then concat_ws('-',pripid,name) else zspid end  key, name                         ");
        sql.append(" FROM e_pri_person_hdfs_ext_%s       ");
        sql.append(" WHERE name <> ''                                  ");
        return DataFrameUtil.getDataFrame(sqlContext, String.format(sql.toString(),date), "personInfoTmp01");
    }

    private DataFrame getPersonInfo02(HiveContext sqlContext) {
        StringBuffer sql = new StringBuffer();
        sql.append(" SELECT *, row_number() over(partition by key) rk  ");
        sql.append("   FROM personInfoTmp01                           ");
        return DataFrameUtil.getDataFrame(sqlContext, sql.toString(), "personInfoTmp02");
    }

    private DataFrame getPersonInfo03(HiveContext sqlContext) {
        StringBuffer sql = new StringBuffer();
        sql.append(" SELECT key,name FROM  personInfoTmp02 b WHERE b.rk=1 ");
        return DataFrameUtil.getDataFrame(sqlContext, sql.toString(), "personInfoTmp03");
    }

    //人员风险信息提示
    private DataFrame getPersonInfo04(HiveContext sqlContext) {
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
                "  from personInfoTmp03 a\n" +
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
        return DataFrameUtil.getDataFrame(sqlContext, String.format(hql,date,date), "personInfoTmp04");
    }

    private DataFrame getPersonInfo05(HiveContext sqlContext) {
       String hql = " select a.key,a.name, case when a.riskinfo1 is not null or a.riskinfo2 is not null  " +
               "                            then concat_ws(' ',a.riskinfo1,a.riskinfo2)  else '' " +
               "                            end  riskinfo,b.encode_v1 from personInfoTmp04 a left join " +
               "                            s_cif_indmap_hdfs_ext_%s b" +
               "                            on a.key=b.zspid";
        return DataFrameUtil.getDataFrame(sqlContext, String.format(hql,date), "personInfoTmp05");
    }

    /**
     * 企业节点数据
     * @param sqlContext
     * @return
     */
    public DataFrame getEntDataFrame(HiveContext sqlContext) {
        getEntInfo01(sqlContext);
        getEntInfo02(sqlContext);
        getEntInfo03(sqlContext);
        getEntInfo04(sqlContext);
        return getEntInfo05(sqlContext);
    }

    private DataFrame getEntInfo01(HiveContext sqlContext) {
        String hql = "SELECT                    pripid,\n" +
                "                               entname,\n" +
                "                               regno,\n" +
                "                               credit_code,\n" +
                "                               esdate,\n" +
                "                               industryphy,\n" +
                "                               regcap,\n" +
                "                               entstatus,\n" +
                "                               regcapcur,\n" +
                "                               enttype," +
                "                               tel," +
                "                               dom \n" +
                "                          FROM enterprisebaseinfocollect_hdfs_ext_%s\n" +
                "                         WHERE pripid <> '' ";
        return DataFrameUtil.getDataFrame(sqlContext,String.format(hql,date) , "entInfoTmp01");
    }

    private DataFrame getEntInfo02(HiveContext sqlContext) {
        String hql = " select *, row_number() over(partition by pripid) rk from entInfoTmp01 a ";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "entInfoTmp02");
    }

    private DataFrame getEntInfo03(HiveContext sqlContext) {
        String hql = "SELECT pripid,\n" +
                "       entname,\n" +
                "       regno,\n" +
                "       credit_code,\n" +
                "       esdate,\n" +
                "       industryphy,\n" +
                "       regcap,\n" +
                "       entstatus,\n" +
                "       regcapcur," +
                "       enttype," +
                "       tel," +
                "       dom \n" +
                "  FROM entInfoTmp02 b\n" +
                " WHERE b.rk = 1 ";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "entInfoTmp03",DataFrameUtil.CACHETABLE_EAGER);
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
                "  from e_pri_person_hdfs_ext_%s a\n";

        String hql2 = String.format(hql,date)+ " where (length(a.DOM) > 4 AND\n" +
                "       (a.dom LIKE '%号%' OR a.dom LIKE '%村%组%' OR a.dom LIKE '%楼%') and\n" +
                "       a.dom not like '%集体%')";
        return DataFrameUtil.getDataFrame(sqlContext,hql2 , "personAddrInfoTmp01");
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
                " inner join (select distinct dom,zspid from personAddrInfoTmp02) pr\n" +
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
        return DataFrameUtil.getDataFrame(sqlContext, hql, "telInfoTmp01",DataFrameUtil.CACHETABLE_EAGER);
    }

    private DataFrame getTelInfoDF02(HiveContext sqlContext) {
        String hql = "select tel\n" +
                "  from telInfoTmp01\n" +
                "   where tel is not null\n" +
                "   and tel <> ''\n" +
                "   and length(tel) > 5\n" +
                " group by tel\n" +
                "having count(distinct pripid) > 1 and count(distinct pripid) <= 5 ";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "telInfoTmp02");
    }

    /**
     * 企业tel节点关系
     * @param sqlContext
     * @return
     */
    public DataFrame getTelRelaInfoDF(HiveContext sqlContext) {
        return getTelRelaInfoDF02(sqlContext);
    }

    private DataFrame getTelRelaInfoDF02(HiveContext sqlContext) {
        String hql = " select me.pripid, ai.tel\n" +
                "  from telInfoTmp02 ai\n" +
                " inner join (select distinct tel, pripid from telInfoTmp01) me\n" +
                "    on ai.tel = me.tel\n" +
                " where ai.tel is not null\n" +
                "   and ai.tel <> ''";
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
                "                 ent.tel\n" +
                "            from telInfoTmp01 ent\n" +
                "            join telRelaInfoTmp02 dom\n" +
                "              on dom.pripid = ent.pripid) a\n" +
                "    join (select ent.entstatus,\n" +
                "                 ent.enttype,\n" +
                "                 ent.industryphy,\n" +
                "                 ent.pripid,\n" +
                "                 ent.tel\n" +
                "            from telInfoTmp01 ent\n" +
                "            join telRelaInfoTmp02 dom\n" +
                "              on dom.pripid = ent.pripid) b\n" +
                "      on a.tel = b.tel\n" +
                "     and a.pripid <> b.pripid ";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "telRelaInfoTmp111");
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
        return DataFrameUtil.getDataFrame(sqlContext, hql2, "entDomTmp01",DataFrameUtil.CACHETABLE_EAGER);
    }

    private DataFrame getEntDomInfoDF02(HiveContext sqlContext) {
        String hql = "select dom\n" +
                "  from entDomTmp01\n" +
                "   where dom is not null\n" +
                "   and dom <> ''\n" +
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
        return getEntDomInfoRelaDF02(sqlContext);
    }

    private DataFrame getEntDomInfoRelaDF02(HiveContext sqlContext) {
        String hql = "select me.pripid, ai.dom\n" +
                "  from entDomTmp02 ai\n" +
                " inner join (select distinct  dom, pripid from entDomTmp01) me\n" +
                "    on ai.dom = me.dom\n" +
                " where me.pripid is not null\n" +
                "   and me.pripid <> ''";
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
                "                 dom.dom\n" +
                "            from entDomTmp01 ent\n" +
                "            join entDomRelaTmp02 dom\n" +
                "              on dom.pripid = ent.pripid) a\n" +
                "    join (select ent.entstatus,\n" +
                "                 ent.enttype,\n" +
                "                 ent.industryphy,\n" +
                "                 ent.pripid,\n" +
                "                 dom.dom\n" +
                "            from entDomTmp01 ent\n" +
                "            join entDomRelaTmp02 dom\n" +
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
        String hql = "select distinct case\n" +
                "                  when (pri.zspid = 'null' and pri.pripid <> 'null' and pri.name <> '') then\n" +
                "                   concat_ws('-', pri.pripid, pri.name)\n" +
                "                  else\n" +
                "                   pri.zspid\n" +
                "                end key,pri.pripid," +
                "               case when ent.entstatus='2' or ent.entstatus='3' or ent.enttype ='4533' then 0.1 else 1.0 end  riskscore\n" +
                "  from e_pri_person_hdfs_ext_%s pri\n" +
                "  inner join entInfoTmp03  ent\n" +
                "  on pri.pripid = ent.pripid\n" +
                " where pri.name <> ''\n" +
                "   and pri.pripid <> 'null'\n" +
                "   and pri.pripid <> ''\n" +
                "   and pri.LEREPSIGN = '1'\n" ;
        return DataFrameUtil.getDataFrame(sqlContext, String.format(hql,date), "legalRelaTmp01");
    }

    /**
     * 任职关系
     * @param sqlContext
     * @return
     */
    public DataFrame getStaffRelaDF(HiveContext sqlContext) {
        String hql = "select distinct case\n" +
                "                  when (pri.zspid = 'null' and pri.pripid <> 'null' and pri.name <> '') then\n" +
                "                   concat_ws('-', pri.pripid, pri.name)\n" +
                "                  else\n" +
                "                     pri.zspid\n" +
                "                end startkey,\n" +
                "                pri.position,\n" +
                "                pri.pripid as endkey," +
                "                riskscore(2,concat_ws('|',ent.entstatus,ent.enttype,pri.position)) as riskscore" +
                "  from e_pri_person_hdfs_ext_%s pri \n" +
                "  inner join entInfoTmp03 ent \n" +
                "  on pri.pripid = ent.pripid \n" +
                " where pri.name <> ''\n" +
                "   and pri.pripid <> 'null'\n" +
                "   and pri.pripid <> ''";
        return DataFrameUtil.getDataFrame(sqlContext, String.format(hql,date), "staffRelaTmp01",DataFrameUtil.CACHETABLE_EAGER);
    }

    /**
     * 投资关系
     * @param sqlContext
     * @return
     */
    public DataFrame getInvRelaDF(HiveContext sqlContext) {
        getInvRelaDF02(sqlContext);
        getInvRelaDF03(sqlContext);
        return getInvRelaDF04(sqlContext);
    }


    private DataFrame getInvRelaDF02(HiveContext sqlContext) {
        String hql = "select distinct inv,\n" +
                "              condate,\n" +
                "              subconam,\n" +
                "              currency,\n" +
                "              conprop,\n" +
                "              blicno,\n" +
                "              pripid\n" +
                "from e_inv_investment_parquet \n";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "invRelaTmp02",DataFrameUtil.CACHETABLE_EAGER);
    }

    private DataFrame getInvRelaDF03(HiveContext sqlContext) {
        String hql = "select en.pripid   as startKey,\n" +
                "       hd.condate,\n" +
                "       hd.subconam,\n" +
                "       hd.currency,\n" +
                "       hd.conprop,\n" +
                "       hd.pripid   as endKey\n" +
                "  from (select distinct pripid, credit_code\n" +
                "                  from entInfoTmp03 \n" +
                "                 where credit_code <> '' ) en, (select * from invRelaTmp02 where blicno<>'' and length(blicno)>17) hd\n" +
                " where hd.blicno = en.credit_code\n" +
                "   and en.pripid <> hd.pripid\n" +
                "union all\n" +
                "select en.pripid   as startKey,\n" +
                "       hd.condate,\n" +
                "       hd.subconam,\n" +
                "       hd.currency,\n" +
                "       hd.conprop,\n" +
                "       hd.pripid   as endKey\n" +
                "  from (select distinct pripid, entname\n" +
                "                          from entInfoTmp03) en, (select * from invRelaTmp02 where inv<>'') hd\n" +
                " where hd.inv = en.entname";
        return DataFrameUtil.getDataFrame(sqlContext, String.format(hql), "invRelaTmp03");
    }

    private DataFrame getInvRelaDF04(HiveContext sqlContext) {
        String hql = "select distinct a.startKey, a.condate, a.subconam, a.currency, a.conprop, a.endKey,riskscore(1,concat_ws('|',b.entstatus,b.enttype,a.conprop)) as riskscore\n" +
                "  from invRelaTmp03 a join entInfoTmp03 b on a.endKey=b.pripid";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "invRelaTmp04",DataFrameUtil.CACHETABLE_EAGER);
    }


    /**
     * 企业控股股东
     * @param sqlContext
     * @return
     */
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

    private DataFrame getPersonJoinRelaDF01(HiveContext sqlContext) {
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
        return DataFrameUtil.getDataFrame(sqlContext, hql, "personJoinRelaTmp01",DataFrameUtil.CACHETABLE_EAGER);
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
        return DataFrameUtil.getDataFrame(sqlContext, hql, "invJoinRelaTmp02",DataFrameUtil.CACHETABLE_EAGER);
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


    public DataFrame getPersonInv(HiveContext sqlContext){
        String hql =
                " select distinct case\n" +
                "                  when (pri.zspid = 'null' and pri.pripid <> 'null' and pri.inv <> '') then\n" +
                "                   concat_ws('-', pri.pripid, pri.inv)\n" +
                "                  else\n" +
                "                   pri.zspid\n" +
                "                end startKey,\n" +
                "                pri.condate,\n" +
                "                pri.subconam,\n" +
                "                pri.currency,\n" +
                "                pri.conprop,\n" +
                "                pri.pripid as endKey," +
                "                riskscore(1,concat_ws('|',ent.entstatus,ent.enttype,pri.conprop)) as riskscore" +
                "  from e_inv_investment_parquet pri\n " +
                "  inner join entInfoTmp03 ent \n" +
                "   on pri.pripid=ent.pripid \n" +
                " where pri.inv <> '' \n" +
                "   and pri.invtype in('20','21','22','30','35','36')" +
                "   and pri.pripid <> 'null' \n" +
                "   and pri.pripid <> '' ";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "personInvTmp01",DataFrameUtil.CACHETABLE_EAGER);
    }


    /**
     * 自然人控股股东
     * @param sqlContext
     * @return
     */
    public DataFrame getPersonHoldRelaDF(HiveContext sqlContext) {
        getPersonHoldRelaDF01(sqlContext);
        return   getPersonHoldRelaDF02(sqlContext);
    }

    private DataFrame getPersonHoldRelaDF01(HiveContext sqlContext) {
        String hql = "select max(conprop) as conprop, endkey\n" +
                "          from personInvTmp01\n" +
                "         group by endkey";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "personHoldRelaTmp01");
    }

    private DataFrame getPersonHoldRelaDF02(HiveContext sqlContext) {
        String hql = "select b.startKey, b.condate, b.subconam, b.currency, b.conprop, b.endKey\n" +
                "  from invHoldRelaTmp01 a" +
                " inner join personInvTmp01 b\n" +
                "    on a.conprop = b.conprop\n" +
                "   and a.endkey = b.endkey";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "personHoldRelaTmp02");
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
                "select key, riskscore, pripid\n" +
                "  from legalRelaTmp01\n" +
                "union all\n" +
                "select startKey, riskscore, endKey\n" +
                "  from staffRelaTmp01 ";
        return  DataFrameUtil.getDataFrame(sqlContext, hql, "personMergeRelationTm01");
    }


    private DataFrame getPersonMergeRelaDF02(HiveContext sqlContext){
        String hql = " select startKey, sum(riskscore), endKey\n" +
                "from personMergeRelationTm01\n" +
                "group by startKey, endKey";
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
        String hql = "select startKey,sum(riskscore),endKey from invMergeRelationTm01 group by startKey,endKey";
        return  DataFrameUtil.getDataFrame(sqlContext, hql, "invMergeRelationTm01");
    }


    /*public DataFrame entDistinct(String temTable,HiveContext sqlContext){
       //重复的数据
        String sql = "select count(*), a.credit_code, a.entname\n" +
               "  from entInfoTmp03 a inner join "+temTable+" b on a.pripid=b.pripid\n" +
               " group by a.credit_code, a.entname\n" +
               "having count(*) > 1";
DataFrameUtil.getDataFrame(sqlContext)


        return null;
    }*/


    public void pairRDD2Parquet(HiveContext sqlContext, DataFrame df, String path) {
        DataFormatConvertUtil.deletePath(path);
        DataFrameUtil.saveAsCsv(df,path);
    }
}
