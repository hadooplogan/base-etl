package com.chinadaas.association.etl.sparksql;
import com.chinadaas.association.util.DataFormatConvertUtil;
import com.chinadaas.association.util.DataFrameUtil;
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
        sql.append(" SELECT zspid AS key, inv AS name                  ");
        sql.append("   FROM default.e_inv_investment_hdfs_ext_20170508 ");
        sql.append(" WHERE  zspid <> 'null'                            ");
        sql.append("   AND inv <> ''                                   ");
        sql.append("   AND length(zspid) > 30                          ");
        sql.append(" UNION                                             ");
        sql.append(" SELECT zspid as key, name                         ");
        sql.append(" FROM default.e_pri_person_hdfs_ext_20170508       ");
        sql.append(" WHERE name <> ''                                  ");
        sql.append("   AND zspid <> 'null'                             ");
        sql.append("   AND LENGTH(zspid) > 30                         ");
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
                "                               entstatus\n" +
                "                          FROM enterprisebaseinfocollect_hdfs_ext_20170427\n" +
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
                "       entstatus\n" +
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
                "  from e_pri_person_hdfs_ext_20170427 a\n" +
                " where (length(a.DOM) > 4 AND\n" +
                "       (a.dom LIKE '%号%' OR a.dom LIKE '%村%组%' OR a.dom LIKE '%楼%') and\n" +
                "       a.dom not like '%集体%')";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "personAddrInfoTmp01");
    }

    private DataFrame getPseronAddrInfo02(HiveContext sqlContext) {
        String hql = "select pre.*\n" +
                "  from personAddrInfoTmp01 pre\n" +
                " inner join enterprisebaseinfocollect_hdfs_ext_20170427 ent\n" +
                "    on pre.pripid = ent.pripid\n" +
                " where ent.entstatus = '1' ";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "personAddrInfoTmp02");
    }

    private DataFrame getPseronAddrInfo03(HiveContext sqlContext) {
        String hql = "select dom\n" +
                "  from personAddrInfoTmp02 pri\n" +
                " where pri.dom is not null\n" +
                "   and pri.dom <> ''\n" +
                "   and zspid <> ''\n" +
                "   and zspid is not null\n" +
                " group by dom\n" +
                "having count(distinct zspid) > 1 and count(distinct zspid) < 5";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "personAddrInfoTmp03");
    }


    /**
     * 人员地址节点关系
     * @param sqlContext
     * @return
     */
    public DataFrame getPersonAddrRelaDF(HiveContext sqlContext) {
        getPseronAddrRela01(sqlContext);
        return getPseronAddrRela02(sqlContext);
    }

    private DataFrame getPseronAddrRela01(HiveContext sqlContext) {
        String hql = "SELECT DISTINCT dom, zspid FROM e_pri_person_hdfs_ext_20170427 ";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "personAddrRelaTmp01");
    }

    private DataFrame getPseronAddrRela02(HiveContext sqlContext) {
        String hql = "select pr.zspid, ai.dom\n" +
                "  from personAddrInfoTmp03 ai\n" +
                " inner join personAddrRelaTmp01 pr\n" +
                "    on pr.dom = ai.dom\n" +
                "   and pr.zspid is not null\n" +
                "   and pr.zspid <> ''";
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
                "   from enterprisebaseinfocollect_merge_20170427\n" +
                "  where tel regexp '^(13|15|18|17)[0-9]{9}$'\n" +
                "     or tel regexp '^[(|（]?(13|15|18|17)[0-9]{9}[-|(|)|）|\\\\|*| |,|，|/]{1,6}+(13|15|18|17)[0-9]{9}'\n" +
                "     or tel regexp '^[(|（]?(13|15|18|17)[0-9]{9}[-|(|)}）|\\\\|*| |,|，|/]{1,6}+(0[0-9]{2,3}[\\-|\\(\\\\|\\*|\\-|-|\\ ]?)?([2-9][0-9]{6,7})(\\-[0-9]{1,4})?'\n" +
                "     or tel regexp '^[(|（]?(0[0-9]{2,3}[-|)|)|）|\\\\|*|\\-|-|\\ }/]?)?([2-9]{1}[0-9]{1}{5,7})(\\-[0-9]{1,4})?'\n" +
                "     or tel regexp '^[(|（]?(0[0-9]{2,3}[-|)|)|）|\\\\|*|\\-|-|\\ ]?)?([2-9]{1}[0-9]{1}{5,7})(\\-[0-9]{1,4})?+(13|15|18|17)[0-9]{9}'\n" +
                "     or tel regexp\n" +
                "  '^[(|（]?(0[0-9]{2,3}[-|)|)|）|\\\\|*|\\-|-|\\ ]?)?([2-9]{1}[0-9]{1}{5,7})(\\-[0-9]{1,4})?[-|(|)}）|\\\\|*| |,|，|/]+[(|（]?(0[0-9]{2,3}[-|)|)|）|\\\\|*|\\-|-|\\ ]?)?([2-9]{1}[0-9]{1}{5,7})(\\-[0-9]{1,4})?'\n";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "telInfoTmp01");
    }

    private DataFrame getTelInfoDF02(HiveContext sqlContext) {
        String hql = "select tel\n" +
                "  from telInfoTmp01\n" +
                " where entstatus = '1'\n" +
                "   and tel is not null\n" +
                "   and tel <> ''\n" +
                "   and length(tel) > 5\n" +
                " group by tel\n" +
                "having count(distinct pripid) > 1 and count(distinct pripid) < 5 ";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "telInfoTmp02");
    }

    /**
     * 企业tel节点关系
     * @param sqlContext
     * @return
     */
    public DataFrame getTelRelaInfoDF(HiveContext sqlContext) {
        getTelRelaInfoDF01(sqlContext);
        return getTelRelaInfoDF02(sqlContext);
    }

    private DataFrame getTelRelaInfoDF01(HiveContext sqlContext) {
        String hql = "select distinct tel, pripid\n" +
                " from enterprisebaseinfocollect_merge_20170427\n";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "telRelaInfoTmp01");
    }

    private DataFrame getTelRelaInfoDF02(HiveContext sqlContext) {
        String hql = " select me.pripid, ai.tel\n" +
                "  from telInfoTmp02 ai\n" +
                " inner join telRelaInfoTmp01 me\n" +
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
        String hql = "select *\n" +
                "  from enterprisebaseinfocollect_merge_20170427 a\n" +
                " where (length(a.DOM) > 4 AND\n" +
                "       (a.dom LIKE '%号%' OR a.dom LIKE '%村%组%' OR a.dom LIKE '%楼%'))\n" +
                "   and ((a.entTYPE in ('10',\n" +
                "                       '11',\n" +
                "                       '12',\n" +
                "                       '13',\n" +
                "                       '14',\n" +
                "                       '15',\n" +
                "                       '31',\n" +
                "                       '32',\n" +
                "                       '33',\n" +
                "                       '34') AND length(a.entname) > 3) OR\n" +
                "       a.entname LIKE '%公司%' OR a.entname LIKE '%室%' OR\n" +
                "       a.entname LIKE '%企业%')";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "entDomTmp01");
    }

    private DataFrame getEntDomInfoDF02(HiveContext sqlContext) {
        String hql = "select dom\n" +
                "  from entDomTmp01\n" +
                " where entstatus = '1'\n" +
                "   and dom is not null\n" +
                "   and dom <> ''\n" +
                " group by dom\n" +
                "having count(distinct pripid) > 1 and count(distinct pripid) < 5 ";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "entDomTmp02");
    }

    /**
     * 企业dom节点关系
     * @param sqlContext
     * @return
     */
    public DataFrame getEntDomInfoRelaDF(HiveContext sqlContext) {
        getEntDomInfoRelaDF01(sqlContext);
        return getEntDomInfoRelaDF02(sqlContext);
    }

    private DataFrame getEntDomInfoRelaDF01(HiveContext sqlContext) {
        String hql = "select distinct dom, pripid\n" +
                " from enterprisebaseinfocollect_merge_20170427\n";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "entDomRelaTmp01");
    }

    private DataFrame getEntDomInfoRelaDF02(HiveContext sqlContext) {
        String hql = "select me.pripid, ai.dom\n" +
                "  from entDomTmp02 ai\n" +
                " inner join entDomRelaTmp01 me\n" +
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
        String hql = "select distinct zspid as key,pripid\n" +
                "  from e_pri_person_hdfs_ext_20170427\n" +
                " where (name <> '' and zspid <> 'null')\n" +
                "   and LEREPSIGN = '1'";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "legalRelaTmp01");
    }

    /**
     * 任职关系
     * @param sqlContext
     * @return
     */
    public DataFrame getStaffRelaDF(HiveContext sqlContext) {
        String hql = "select distinct zspid as startkey, position, pripid as endkey\n" +
                "  from e_pri_person_hdfs_ext_20170427\n" +
                " where (name <> '' and zspid <> 'null')";
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
                " from enterprisebaseinfocollect_hdfs_ext_20170427\n" +
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
                "from e_inv_investment_hdfs_ext_20170427\n" +
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
                "  from invRelaTmp03\n" +
                "union all\n" +
                "select distinct zspid    as startKey,\n" +
                "                condate,\n" +
                "                subconam,\n" +
                "                currency,\n" +
                "                conprop,\n" +
                "                pripid   as endKey\n" +
                "  from e_inv_investment_hdfs_ext_20170427\n" +
                " where zspid <> 'null'\n" +
                "   and inv <> ''";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "invRelaTmp04");
    }




    public void pairRDD2Parquet(HiveContext sqlContext, DataFrame df, String path) {
        DataFormatConvertUtil.deletePath(path);
        DataFrameUtil.saveAsCsv(df,path);
    }



}
