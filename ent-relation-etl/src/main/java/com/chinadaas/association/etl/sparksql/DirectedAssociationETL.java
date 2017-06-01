package com.chinadaas.association.etl.sparksql;
import com.chinadaas.common.util.DataFormatConvertUtil;
import com.chinadaas.common.util.DataFrameUtil;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

import java.io.Serializable;


public class DirectedAssociationETL implements Serializable {
    private static final long serialVersionUID = -2950730482818667986L;
    public static final String HIVE_SCHEMA = DataFormatConvertUtil.getSchema();

    /**
     * 个人节点数据
     * @param sqlContext
     * @return
     */
    public DataFrame getPersonDataFrame(HiveContext sqlContext) {
        getPersonInfo01(sqlContext);
        getPersonInfo02(sqlContext);
        return getPersonInfo03(sqlContext);
    }

    private DataFrame getPersonInfo01(HiveContext sqlContext) {
         StringBuffer sql = new StringBuffer();
        sql.append(" SELECT case when (zspid='null' and pripid<>'null'  and inv<>'') then concat_ws('-',pripid,inv) else zspid end key, inv AS name                  ");
        sql.append("   FROM default.e_inv_investment_hdfs_ext_20170508 ");
        sql.append(" WHERE  invtype in('20','21','22','30','35','36')  ");
        sql.append("   AND inv <> ''                                   ");
        sql.append(" UNION                                             ");
        sql.append(" SELECT case when (zspid='null' and  pripid<>'null' and name<>'') then concat_ws('-',pripid,name) else zspid end  key, name                         ");
        sql.append(" FROM default.e_pri_person_hdfs_ext_20170508       ");
        sql.append(" WHERE name <> ''                                  ");
        return DataFrameUtil.getDataFrame(sqlContext, sql.toString(), "personInfoTmp01");
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


    /**
     * 企业节点数据
     * @param sqlContext
     * @return
     */
    public DataFrame getEntDataFrame(HiveContext sqlContext) {
        getEntInfo01(sqlContext);
        getEntInfo02(sqlContext);
        return getEntInfo03(sqlContext);
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
                "                               regcapcur\n" +
                "                          FROM enterprisebaseinfocollect_hdfs_ext_20170508\n" +
                "                         WHERE pripid <> '' ";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "entInfoTmp01");
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
                "       regcapcur\n" +
                "  FROM entInfoTmp02 b\n" +
                " WHERE b.rk = 1 ";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "entInfoTmp03");
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
                "  from e_pri_person_hdfs_ext_20170508 a\n" +
                " where (length(a.DOM) > 4 AND\n" +
                "       (a.dom LIKE '%号%' OR a.dom LIKE '%村%组%' OR a.dom LIKE '%楼%') and\n" +
                "       a.dom not like '%集体%')";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "personAddrInfoTmp01");
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
                " inner join enterprisebaseinfocollect_hdfs_ext_20170508 ent\n" +
                "    on pre.pripid = ent.pripid\n" +
                " where ent.entstatus = '1' ";
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
        String hql = " select tel, pripid, entstatus\n" +
                "   from enterprisebaseinfocollect_hdfs_ext_20170508\n" +
                "  where tel regexp '^(13|15|18|17)[0-9]{9}$'\n" +
                "     or tel regexp '^[(|（]?(13|15|18|17)[0-9]{9}[-|(|)|）|\\\\|*| |,|，|/]{1,6}+(13|15|18|17)[0-9]{9}'\n" +
                "     or tel regexp '^[(|（]?(13|15|18|17)[0-9]{9}[-|(|)}）|\\\\|*| |,|，|/]{1,6}+(0[0-9]{2,3}[\\-|\\(\\\\|\\*|\\-|-|\\ ]?)?([2-9][0-9]{6,7})(\\-[0-9]{1,4})?'\n" +
                "     or tel regexp '^[(|（]?(0[0-9]{2,3}[-|)|)|）|\\\\|*|\\-|-|\\ }/]?)?([2-9]{1}[0-9]{1}{5,7})(\\-[0-9]{1,4})?'\n" +
                "     or tel regexp '^[(|（]?(0[0-9]{2,3}[-|)|)|）|\\\\|*|\\-|-|\\ ]?)?([2-9]{1}[0-9]{1}{5,7})(\\-[0-9]{1,4})?+(13|15|18|17)[0-9]{9}'\n" +
                "     or tel regexp\n" +
                "  '^[(|（]?(0[0-9]{2,3}[-|)|)|）|\\\\|*|\\-|-|\\ ]?)?([2-9]{1}[0-9]{1}{5,7})(\\-[0-9]{1,4})?[-|(|)}）|\\\\|*| |,|，|/]+[(|（]?(0[0-9]{2,3}[-|)|)|）|\\\\|*|\\-|-|\\ ]?)?([2-9]{1}[0-9]{1}{5,7})(\\-[0-9]{1,4})?' and entstatus = '1' \n";
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
        String hql = "select dom,pripid \n" +
                "  from enterprisebaseinfocollect_hdfs_ext_20170508 a\n" +
                " where (length(a.DOM) > 4 AND\n" +
                "       (a.dom LIKE '%号%' OR a.dom LIKE '%村%组%' OR a.dom LIKE '%楼%'))\n" +
                "   and ((length(a.entname) > 3) OR\n" +
                "       a.entname LIKE '%公司%' OR a.entname LIKE '%室%' OR\n" +
                "       a.entname LIKE '%企业%') and entstatus = '1'";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "entDomTmp01",DataFrameUtil.CACHETABLE_EAGER);
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
                "                end key,pri.pripid\n" +
                "  from e_pri_person_hdfs_ext_20170508 pri\n" +
                "  inner join enterprisebaseinfocollect_hdfs_ext_20170508 ent\n" +
                "  on pri.pripid = ent.pripid\n" +
                " where pri.name <> ''\n" +
                "   and pri.pripid <> 'null'\n" +
                "   and pri.pripid <> ''\n" +
                "   and pri.LEREPSIGN = '1'\n" ;
        return DataFrameUtil.getDataFrame(sqlContext, hql, "legalRelaTmp01");
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
                "                   pri.zspid\n" +
                "                end startkey,\n" +
                "                pri.position,\n" +
                "                pri.pripid as endkey" +
                "  from e_pri_person_hdfs_ext_20170508 pri \n" +
                "  inner join enterprisebaseinfocollect_hdfs_ext_20170508 ent \n" +
                "  on pri.pripid = ent.pripid \n" +
                " where pri.name <> ''\n" +
                "   and pri.pripid <> 'null'\n" +
                "   and pri.pripid <> ''";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "staffRelaTmp01");
    }

    /**
     * 投资关系
     * @param sqlContext
     * @return
     */
    public DataFrame getInvRelaDF(HiveContext sqlContext) {
        getInvRelaDF01(sqlContext);
        getInvRelaDF02(sqlContext);
        getInvRelaDF03(sqlContext);
        return getInvRelaDF04(sqlContext);
    }

    private DataFrame getInvRelaDF01(HiveContext sqlContext) {
        String hql = "select pripid, regno, credit_code,entname\n" +
                " from enterprisebaseinfocollect_hdfs_ext_20170508\n" +
                "where credit_code <> ''\n" +
                "group by pripid, regno, credit_code,entname\n";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "invRelaTmp01",DataFrameUtil.CACHETABLE_EAGER);
    }

    private DataFrame getInvRelaDF02(HiveContext sqlContext) {
        String hql = "select distinct inv,\n" +
                "              condate,\n" +
                "              subconam,\n" +
                "              currency,\n" +
                "              conprop,\n" +
                "              blicno,\n" +
                "              pripid\n" +
                "from e_inv_investment_hdfs_ext_20170508\n" +
                "where blicno <> ''";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "invRelaTmp02",DataFrameUtil.CACHETABLE_EAGER);
    }

    private DataFrame getInvRelaDF03(HiveContext sqlContext) {
        String hql = "select en.pripid   as startKey,\n" +
                "       hd.condate,\n" +
                "       hd.subconam,\n" +
                "       hd.currency,\n" +
                "       hd.conprop,\n" +
                "       hd.pripid   as endKey\n" +
                "  from invRelaTmp01 en, invRelaTmp02 hd\n" +
                " where hd.blicno = en.credit_code\n" +
                "   and en.pripid <> hd.pripid\n" +
                "union all\n" +
                "select en.pripid   as startKey,\n" +
                "       hd.condate,\n" +
                "       hd.subconam,\n" +
                "       hd.currency,\n" +
                "       hd.conprop,\n" +
                "       hd.pripid   as endKey\n" +
                "  from invRelaTmp01 en, invRelaTmp02 hd\n" +
                " where hd.blicno = en.regno\n" +
                "   and en.pripid <> hd.pripid\n" +
                "union all\n" +
                "select en.pripid   as startKey,\n" +
                "       hd.condate,\n" +
                "       hd.subconam,\n" +
                "       hd.currency,\n" +
                "       hd.conprop,\n" +
                "       hd.pripid   as endKey\n" +
                "  from invRelaTmp01 en, invRelaTmp02 hd\n" +
                " where hd.inv = en.entname";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "invRelaTmp03");
    }

    private DataFrame getInvRelaDF04(HiveContext sqlContext) {
        String hql = "select distinct startKey, condate, subconam, currency, conprop, endKey\n" +
                "  from invRelaTmp03 ";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "invRelaTmp04");
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
                "                pri.pripid as endKey" +
                "  from e_inv_investment_hdfs_ext_20170508 pri\n " +
                "  inner join enterprisebaseinfocollect_hdfs_ext_20170508 ent \n" +
                "   on pri.pripid=ent.pripid \n" +
                " where pri.inv <> '' \n" +
                "   and pri.invtype in('20','21','22','30','35','36')" +
                "   and pri.pripid <> 'null' \n" +
                "   and pri.pripid <> '' ";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "personInvTmp01");
    }



    public void pairRDD2Parquet(HiveContext sqlContext, DataFrame df, String path) {
        DataFormatConvertUtil.deletePath(path);
        DataFrameUtil.saveAsCsv(df,path);
    }



}
