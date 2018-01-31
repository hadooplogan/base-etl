package com.chinadaas.association.etl.sparksql;

import com.chinadaas.association.etl.table.EntConvertData;
import com.chinadaas.common.util.DataFrameUtil;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import javax.swing.*;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class EntRelationETL implements Serializable {
    private static final long serialVersionUID = -2950730482818667986L;
    //public static final String HIVE_SCHEMA = DataFormatConvertUtil.getSchema();
    private String date;

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
    public Dataset getPersonDataFrame(SparkSession sqlContext) {
        getPersonInfo01(sqlContext);
        getPersonInfo02(sqlContext);
        getPersonInfo03(sqlContext);
        getPersonInfo04(sqlContext);
        return getPersonInfo05(sqlContext);
    }


    private Dataset getPersonInfo01(SparkSession sqlContext) {
        String hql ="select distinct key,name,zspid from "+
            " (SELECT case when (zspid='null' and pripid<>'null'  and inv<>'') then concat_ws('-',pripid,inv) else concat_ws('-', zspid, inv) end key, inv AS name,zspid  "+
            "   FROM e_inv_investment_parquet "+
            " WHERE  invtype in('20','21','22','30','35','36','77')  "+
            "   AND inv <> ''                                   "+
            " UNION                                             "+
            " SELECT case when (zspid='null' and pripid<>'null'  and inv<>'') then concat_ws('-',pripid,inv) else concat_ws('-', zspid, inv) end key, inv AS name,zspid  "+
            "   FROM e_inv_investment_parquet "+
            " WHERE  (invtype ='90' or  invtype ='')"+
            "   AND inv <> '' AND length(inv) < 4                                 "+
            " UNION                                             "+
            " SELECT case when (zspid='null' and  pripid<>'null' and name<>'') then concat_ws('-',pripid,name) else concat_ws('-', zspid, name) end  key, name ,zspid    "+
            " FROM entPersonTmp      "+
            " WHERE name <> ''                                  "+
            " UNION "+
            " select distinct "+
            " concat_ws('-', pri.pripid, pri.inv) "+
            "  as key,pri.inv as name, concat_ws('-', pri.pripid, pri.inv) as zspid "+
            " from entOrgRelatgionTmp01 pri where length(inv) < 4)";

        return DataFrameUtil.getDataFrame(sqlContext, hql.toString(), "personInfoTmp01");
    }


    //人员风险信息提示
    private Dataset getPersonInfo02(SparkSession sqlContext) {
        String hql = " select a.*,\n" +
                "       case\n" +
                "         when b.fsx_name is not null then\n" +
                "          concat_ws('-', '失信被执行人', b.sxbzr_json)\n" +
                "         else\n" +
                "          null\n" +
                "       end riskinfo1," +
                "      case\n" +
                "         when c.fss_name is not null then\n" +
                "          concat_ws('-', '被执行人', c.bzxr_json)\n" +
                "         else\n" +
                "          null\n" +
                "       end riskinfo2\n" +
                "  from personInfoTmp01 a\n" +
                "  left join sxbzr b\n" +
                "    on a.key = concat_ws('-',b.zspid,b.fsx_name)" +
                "  left join bzxr c\n" +
                "    on a.key = concat_ws('-',c.zspid,c.fss_name)" ;


        return DataFrameUtil.getDataFrame(sqlContext, hql, "personInfoTmp02");
    }

    private Dataset getPersonInfo03(SparkSession sqlContext) {
       String hql = " select a.key,a.name, case when a.riskinfo1 is not null or a.riskinfo2 is not null  " +
               "                            then concat_ws('|',a.riskinfo1,a.riskinfo2)  else '' " +
               "                            end  riskinfo,b.encode_v1 from personInfoTmp02 a left join " +
               "                            s_cif_indmap b" +
               "                            on a.zspid=b.zspid";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "personInfoTmp03");
    }

    private Dataset getPersonInfo04(SparkSession sqlContext) {
        StringBuffer sql = new StringBuffer();
        sql.append(" SELECT *, row_number() over(partition by key order by key) rk  ");
        sql.append("   FROM personInfoTmp03                           ");
        return DataFrameUtil.getDataFrame(sqlContext, sql.toString(), "personInfoTmp04");
    }

    private Dataset getPersonInfo05(SparkSession sqlContext) {
        StringBuffer sql = new StringBuffer();
        sql.append(" SELECT key,name,riskinfo,encode_v1 FROM  personInfoTmp04 b WHERE b.rk=1 ");
        return DataFrameUtil.getDataFrame(sqlContext, sql.toString(), "personInfoTmp05");
    }
    /**
     * 企业节点数据
     * @param sqlContext
     * @return
     */
    public Dataset getEntDataFrame(SparkSession sqlContext) {

        getEntInfo04(sqlContext);
        getEntInfo05(sqlContext);
        return getEntInfo06(sqlContext);
    }


    //企业风险提示
    private Dataset getEntInfo04(SparkSession sqlContext) {
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
                "         when b.pripid is not null then\n" +
                "          concat_ws('-', '企业经营异常名录', b.abnormity_json)\n" +
                "         else\n" +
                "          null\n" +
                "       end riskinfo1," +
                "         case\n" +
                "         when c.fsx_name is not null then\n" +
                "          concat_ws('-', '失信被执行人', c.sxbzr_json)\n" +
                "         else\n" +
                "          null\n" +
                "       end riskinfo2," +
                "       case\n" +
                "         when d.fss_name is not null then\n" +
                "          concat_ws('-', '被执行人', d.bzxr_json)\n" +
                "         else\n" +
                "          null\n" +
                "       end riskinfo3,\n" +
                "       case\n" +
                "         when e.pripid is not null then\n" +
                "          concat_ws('-', '严重违法', e.breaklaw_json)\n" +
                "         else\n" +
                "          null\n" +
                "       end riskinfo4\n" +
                "  from entInfoTmp03 a\n" +
                "  left join abnormity b\n" +
                "    on a.pripid = b.pripid " +
                "  left join  sxbzr c\n" +
                "    on a.entname = c.fsx_name " +
                "  left join bzxr d\n" +
                "    on a.entname = d.fss_name " +
                "  left join breaklaw e " +
                "    on a.pripid=e.pripid";
        return DataFrameUtil.getDataFrame(sqlContext, String.format(hql), "entInfoTmp04");
    }

    private Dataset getEntInfo05(SparkSession sqlContext) {
        String hql = "SELECT distinct pripid,\n" +
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
                "                 riskinfo3 is not null or" +
                "                 riskinfo4 is not null" +
                "            then concat_ws('|',riskinfo1,riskinfo2,riskinfo3,riskinfo4) " +
                "            else '' end riskinfo \n" +
                "  FROM entInfoTmp04 b\n";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "entInfoTmp05");
    }
    private Dataset getEntInfo06(SparkSession sqlContext) {
        String hql = "select a.*, case when b.pripid is null then '0' else '1' end as islist\n" +
                "  from entInfoTmp05 a\n" +
                "  left join comp_info_tmp b\n" +
                "    on a.pripid = b.pripid";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "entInfoTmp06");
    }



    /**
     * 人员地址节点
     * @param sqlContext
     * @return
     */
    public Dataset getPersonAddrDataFrame(SparkSession sqlContext) {
        getPseronAddrInfo01(sqlContext);
        getPseronAddrInfo02(sqlContext);
        return getPseronAddrInfo03(sqlContext);
    }

    private Dataset getPseronAddrInfo01(SparkSession sqlContext) {
        String hql = "select *\n" +
                "  from entPersonTmp a\n" +
               " where (length(a.DOM) > 4 AND\n" +
                "       (a.dom LIKE '%号%' OR a.dom LIKE '%村%组%' OR a.dom LIKE '%楼%') and\n" +
                "       a.dom not like '%集体%')";
        return DataFrameUtil.getDataFrame(sqlContext,hql , "personAddrInfoTmp01");
    }

    private Dataset getPseronAddrInfo02(SparkSession sqlContext) {
        String hql = "select pre.dom,case\n" +
                "                         when (pre.zspid = 'null' and pre.pripid <> 'null' and\n" +
                "                              pre.name <> '') then\n" +
                "                          concat_ws('-', pre.pripid, pre.name)\n" +
                "                         else\n" +
                "                          concat_ws('-', pre.zspid, pre.name) \n" +
                "                       end zspid\n" +
                "  from personAddrInfoTmp01 pre\n" +
                " inner join entInfoTmp03 ent\n" +
                "    on pre.pripid = ent.pripid" +
                "    where pre.name <> '' \n" ;
        return DataFrameUtil.getDataFrame(sqlContext, hql, "personAddrInfoTmp02",DataFrameUtil.CACHETABLE_EAGER);
    }

    private Dataset getPseronAddrInfo03(SparkSession sqlContext) {
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
    public Dataset getPersonAddrRelaDF(SparkSession sqlContext) {
        return getPseronAddrRela(sqlContext);
    }

    private Dataset getPseronAddrRela(SparkSession sqlContext) {
        String hql = "select pr.zspid, ai.dom\n" +
                "  from personAddrInfoTmp03 ai\n" +
                " inner join (select distinct dom,zspid from personAddrInfoTmp02 where zspid <>'null') pr\n" +
                "    on pr.dom = ai.dom " ;
        return DataFrameUtil.getDataFrame(sqlContext, hql, "personAddrRelaTmp01");
    }

    public Dataset getPersonAndPersonDomRelaDF01(SparkSession sqlContext){
        String hql="select collect_set(dom.zspid) as list,dom.dom as dtcpara\n" +
                "    from personAddrRelaTmp01 dom\n" +
                "   group by dom.dom";


        JavaRDD<EntConvertData> jdd = converDF2RDD(DataFrameUtil.getDataFrame(sqlContext, hql, "personDomRelaTmp11")) ;
        sqlContext.createDataFrame(jdd,EntConvertData.class).registerTempTable("persondomRelation");

        String hql2 = " select a.fromid as pripid ,\n" +
                "       a.para as dom,\n" +
                "       a.toid as entpripid,\n" +
                "       '1'  as riskscore,\n" +
                "       a.fromid as startid,\n" +
                "       a.toid as endid\n" +
                "          from persondomRelation a\n";
        return  DataFrameUtil.getDataFrame(sqlContext, hql2, "personDomRelaTmp0001");
    }





    /**
     * 企业tel节点
     * @param sqlContext
     * @return
     */
    public Dataset getTelInfoDF(SparkSession sqlContext) {
        getTelInfoDF01(sqlContext);
        return getTelInfoDF02(sqlContext);
    }

    private Dataset getTelInfoDF01(SparkSession sqlContext) {
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

    private Dataset getTelInfoDF02(SparkSession sqlContext) {
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
    public Dataset getTelRelaInfoDF(SparkSession sqlContext) {
        getTelRelaInfoDF02(sqlContext);
        Dataset df = distinctEntRelationDF("telRelaInfoTmp02",sqlContext);
        df.registerTempTable("telRelaResultInfoTmp02");
        return df;
    }

    private Dataset getTelRelaInfoDF02(SparkSession sqlContext) {
        String hql = " select me.pripid, ai.tel as dtcpara\n" +
                "  from telInfoTmp02 ai\n" +
                " inner join (select distinct tel, pripid from telInfoTmp01) me\n" +
                "    on ai.tel = me.tel\n" +
                " where ai.tel is not null\n" +
                "   and ai.tel <> ''" +
                "   and ai.tel <> 'null' ";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "telRelaInfoTmp02");
    }

    /*public Dataset getEntAndEntTelInfoDF(SparkSession sqlContext) {
        String hql = "select a.pripid,\n" +
                "         a.tel,\n" +
                "         b.pripid as entpripid,\n" +
                "         case\n" +
                "           when b.entstatus = '2' or b.entstatus = '3' or b.enttype = '0000' then\n" +
                "            0\n" +
                "           when a.industryphy = b.industryphy then\n" +
                "            0.5\n" +
                "           else\n" +
                "            0.3\n" +
                "         end riskscore,\n" +
                "           a.pripid as startid,"+
                "           b.pripid as endid "+
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
*/

    public JavaRDD<EntConvertData> converDF2RDD(Dataset ds){

        return ds.toJavaRDD().
                flatMap(new FlatMapFunction<Row,EntConvertData>() {

                    @Override
                    public Iterator<EntConvertData> call(Row row) throws Exception {
                        List<EntConvertData> ent = new ArrayList<>();

                        List<String> list =  row.getList(0);

                        String dom = row.getAs("dtcpara");
                        for(int i=0;i<list.size();i++){
                            for(int j=i+1;j<list.size();j++){
                                EntConvertData data = new EntConvertData();
                                data.setFromid(list.get(i));
                                data.setToid(list.get(j));
                                data.setPara(dom);
                                ent.add(data);
                            }
                    }
                        return ent.iterator();
                    }
                });

    }

    public Dataset getEntAndEntTelInfoDF01(SparkSession sqlContext) {
        String hql="select collect_set(dom.pripid) as list,dom.dtcpara\n" +
                "    from telRelaResultInfoTmp02 dom\n" +
                "   group by dom.dtcpara";

        JavaRDD<EntConvertData> jdd = converDF2RDD(DataFrameUtil.getDataFrame(sqlContext, hql, "enttelRelaTmp11")) ;
        sqlContext.createDataFrame(jdd,EntConvertData.class).registerTempTable("enttelRelation");

        String hql2 = " select a.fromid as pripid,\n" +
                "       a.para as tel,\n" +
                "       a.toid as entpripid,\n" +
                "       case\n" +
                "         when a.tentstatus = '2' or a.tentstatus = '3' or\n" +
                "              a.tenttype = '0000' then\n" +
                "          0\n" +
                "         when a.findustryphy = a.tindustryphy then\n" +
                "          0.5\n" +
                "         else\n" +
                "          0.3\n" +
                "       end riskscore,\n" +
                "       a.fromid as startid,\n" +
                "       a.toid as endid\n" +
                "  from (select dom.fromid,\n" +
                "               dom.toid,\n" +
                "               dom.para,\n" +
                "               ent.entstatus   as fentstatus,\n" +
                "               ent.enttype     as fenttype,\n" +
                "               ent.industryphy as findustryphy,\n" +
                "               tmp.entstatus   as tentstatus,\n" +
                "               tmp.enttype     as tenttype,\n" +
                "               tmp.industryphy as tindustryphy\n" +
                "          from enttelRelation dom\n" +
                "          left join telInfoTmp01 ent\n" +
                "            on dom.fromid = ent.pripid\n" +
                "          left join telInfoTmp01 tmp\n" +
                "            on dom.toid = tmp.pripid) a ";

        return  DataFrameUtil.getDataFrame(sqlContext, hql2, "telRelaInfoTmp1111");
    }




    /**
     * 企业dom节点
     * @param sqlContext
     * @return
     */
    public Dataset getEntDomInfoDF(SparkSession sqlContext) {
        getEntDomInfoDF01(sqlContext);
        return getEntDomInfoDF02(sqlContext);
    }

    private Dataset getEntDomInfoDF01(SparkSession sqlContext) {
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

    private Dataset getEntDomInfoDF02(SparkSession sqlContext) {
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
    public Dataset getEntDomInfoRelaDF(SparkSession sqlContext) {
        getEntDomInfoRelaDF02(sqlContext);
        Dataset df = distinctEntRelationDF("entDomRelaTmp02",sqlContext);
        df.cache().registerTempTable("entDomRelaResultTmp02");
        return df;
    }

    private Dataset getEntDomInfoRelaDF02(SparkSession sqlContext) {
        String hql = "select me.pripid, ai.dom as dtcpara\n" +
                "  from entDomTmp02 ai\n" +
                " inner join (select distinct  dom, pripid from entDomTmp01 where pripid is not null " +
                " and pripid <> '' and pripid <> 'null' ) me\n" +
                "    on ai.dom = me.dom\n" ;
        return DataFrameUtil.getDataFrame(sqlContext, hql, "entDomRelaTmp02");
    }




    /*public Dataset getEntAndEntDomInfoRelaDF(SparkSession sqlContext){
        String hql="select a.pripid,\n" +
                "         a.dom,\n" +
                "         b.pripid as entpripid,\n" +
                "         case\n" +
                "           when b.entstatus = '2' or b.entstatus = '3' or b.enttype = '0000' then\n" +
                "            0\n" +
                "           when a.industryphy = b.industryphy then\n" +
                "            0.5\n" +
                "           else\n" +
                "            0.3\n" +
                "         end riskscore,\n" +
                "         a.pripid as startid,\n" +
                "         b.pripid as endid \n" +
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
    }*/


    public Dataset getEntAndEntDomInfoRelaDF01(SparkSession sqlContext){
        String hql="select collect_set(dom.pripid) as list,dom.dtcpara\n" +
                "    from entDomRelaResultTmp02 dom\n" +
                "   group by dom.dtcpara";


        JavaRDD<EntConvertData> jdd = converDF2RDD(DataFrameUtil.getDataFrame(sqlContext, hql, "entDomRelaTmp11")) ;
        sqlContext.createDataFrame(jdd,EntConvertData.class).registerTempTable("entdomRelation");

        String hql2 = " select a.fromid as pripid ,\n" +
                "       a.para as dom,\n" +
                "       a.toid as entpripid,\n" +
                "       case\n" +
                "         when a.tentstatus = '2' or a.tentstatus = '3' or\n" +
                "              a.tenttype = '0000' then\n" +
                "          0\n" +
                "         when a.findustryphy = a.tindustryphy then\n" +
                "          0.5\n" +
                "         else\n" +
                "          0.3\n" +
                "       end riskscore,\n" +
                "       a.fromid as startid,\n" +
                "       a.toid as endid\n" +
                "  from (select dom.fromid,\n" +
                "               dom.toid,\n" +
                "               dom.para,\n" +
                "               ent.entstatus   as fentstatus,\n" +
                "               ent.enttype     as fenttype,\n" +
                "               ent.industryphy as findustryphy,\n" +
                "               tmp.entstatus   as tentstatus,\n" +
                "               tmp.enttype     as tenttype,\n" +
                "               tmp.industryphy as tindustryphy\n" +
                "          from entdomRelation dom\n" +
                "          left join entDomTmp01 ent\n" +
                "            on dom.fromid = ent.pripid\n" +
                "          left join entDomTmp01 tmp\n" +
                "            on dom.toid = tmp.pripid) a ";

        return  DataFrameUtil.getDataFrame(sqlContext, hql2, "entDomRelaTmp0001");
    }






    /**
     * 法人关系
     * @param sqlContext
     * @return
     */
    public Dataset getLegalRelaDF(SparkSession sqlContext) {
        getLegalRelaDF01(sqlContext);
        distinctEntRelationDF("legalRelaTmp01",sqlContext);
        return getLegalRelaDF02(sqlContext);
    }

    private Dataset getLegalRelaDF01(SparkSession sqlContext) {
        String hql = "select distinct case\n" +
                "                  when (pri.zspid = 'null' and pri.pripid <> 'null' and\n" +
                "                       pri.name <> '') then\n" +
                "                   concat_ws('-', pri.pripid, pri.name)\n" +
                "                  else\n" +
                "                   concat_ws('-', pri.zspid, pri.name)\n" +
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

    private Dataset getLegalRelaDF02(SparkSession sqlContext) {
        String hql = "select \n" +
                "       pri.dtcpara as zspid,\n" +
                "       pri.pripid, " +
                "        case\n" +
                "         when ent.entstatus = '2' or ent.entstatus = '3' or\n" +
                "              ent.enttype = '0000' or ent.entstatus = '21' " +
                "              or ent.entstatus = '22' then\n" +
                "          0.0\n" +
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
    public Dataset getStaffRelaDF(SparkSession sqlContext) {
        getStaffRelaDF011(sqlContext);
        getStaffRelaDF012(sqlContext);
        distinctEntRelationDF("staffRelaTmp012",sqlContext);

        return getStaffRelaDF02(sqlContext);
    }


    private Dataset getStaffRelaDF011(SparkSession sqlContext) {
        String hql = "select distinct case\n" +
                "                  when (pri.zspid = 'null' and pri.pripid <> 'null' and\n" +
                "                       pri.name <> '') then\n" +
                "                   concat_ws('-', pri.pripid, pri.name)\n" +
                "                  else\n" +
                "                   concat_ws('-', pri.zspid, pri.name)\n" +
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

    private Dataset getStaffRelaDF012(SparkSession sqlContext) {
        String hql = "select concat_ws('-',startkey,position) as dtcpara,pripid from staffRelaTmp011";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "staffRelaTmp012");
    }

    private Dataset getStaffRelaDF02(SparkSession sqlContext) {
        String hql = "select a.startkey,\n" +
                "            a.position,\n" +
                "       a.pripid as endkey,\n" +
                "       case when (a.position='' or lower(a.position)='null') " +
                "            then 'none'" +
                "            else riskscore(2,\n" +
                "                 concat_ws('|', ent.entstatus, ent.enttype, a.position)) end riskscore\n" +
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
    public Dataset getInvRelaDF(SparkSession sqlContext) {
        getInvRelaDF03(sqlContext);
        getInvRelaDF04(sqlContext);
        distinctEntRelationDF("invRelaTmp031",sqlContext);
        getInvRelaDF041(sqlContext);
        distinctEntRelationDF("invRelaTmp041",sqlContext);
        return getInvRelaDF05(sqlContext);
    }


    private Dataset getInvRelaDF03(SparkSession sqlContext) {
        getInvRelaDF001(sqlContext);
        getInvRelaDF002(sqlContext);
        getInvRelaDF003(sqlContext);
        getInvRelaDF004(sqlContext);
        getInvRelaDF005(sqlContext);
     return   getInvRelaDF006(sqlContext);
    }


    private Dataset getInvRelaDF001(SparkSession sqlContext){
        String hql = "select en.pripid   as startKey,\n" +
                "       en.entname,"+
                "       hd.condate,\n" +
                "       hd.subconam,\n" +
                "       hd.currency,\n" +
                "       hd.conprop,\n" +
                "       hd.pripid   as endKey\n" +
                "  from (select distinct pripid, entname\n" +
                "          from entInfoTmp03\n" +
                "         where entname <> ''\n" +
                "           and entname <> 'null') en,\n" +
                "       (select inv, condate, currency, subconam, conprop, blicno, pripid\n" +
                "          from e_inv_investment_parquet\n" +
                "         where inv <> '') hd\n" +
                " where hd.inv = en.entname";

       /* val hql =   "select en.pripid   as startKey,\n" +
                "       en.entname,"+
                "       hd.condate,\n" +
                "       hd.subconam,\n" +
                "       hd.currency,\n" +
                "       hd.conprop,\n" +
                "       hd.pripid   as endKey\n" +
                "  from (select distinct pripid, entname\n" +
                "          from entInfoTmp03\n" +
                "         where entname <> ''\n" +
                "           and entname <> 'null') en,\n" +
                "       (select inv, condate, currency, subconam, conprop, blicno, pripid\n" +
                "          from e_inv_investment_parquet\n" +
                "         where inv <> '') hd\n" +
                " where hd.inv = en.entname";
*/
        return DataFrameUtil.getDataFrame(sqlContext, hql, "invRelaTmp001");

    }



    private Dataset getInvRelaDF002(SparkSession sqlContext){
        String hql = "select a.inv,\n" +
                "       a.condate,\n" +
                "       a.currency,\n" +
                "       a.subconam,\n" +
                "       a.conprop,\n" +
                "       a.blicno,\n" +
                "       a.pripid\n" +
                "  from e_inv_investment_parquet a\n" +
                " where not exists  (select 1\n" +
                "          from invRelaTmp001 b\n" +
                "         where a.pripid = b.endKey\n" +
                "           and a.inv = b.entname)";

        /*val hql1 = "select a.inv,\n" +
                "       a.condate,\n" +
                "       a.currency,\n" +
                "       a.subconam,\n" +
                "       a.conprop,\n" +
                "       a.blicno,\n" +
                "       a.pripid\n" +
                "  from e_inv_investment_parquet a\n" +
                " where not exists  (select 1\n" +
                "          from invRelaTmp001 b\n" +
                "         where a.pripid = b.endKey\n" +
                "           and a.inv = b.entname)";
*/
        return DataFrameUtil.getDataFrame(sqlContext, hql, "invRelaTmp002");
    }

    private Dataset getInvRelaDF003(SparkSession sqlContext){
        String hql = "select en.pripid      as startKey,\n" +
                "       en.credit_code,\n" +
                "       hd.condate,\n" +
                "       hd.subconam,\n" +
                "       hd.currency,\n" +
                "       hd.conprop,\n" +
                "       hd.pripid      as endKey\n" +
                "  from (select distinct pripid, credit_code\n" +
                "          from entInfoTmp03\n" +
                "         where credit_code <> ''\n" +
                "           and credit_code <> 'null') en,\n" +
                "       (select inv, condate, currency, subconam, conprop, blicno, pripid\n" +
                "          from invRelaTmp002\n" +
                "         where blicno <> ''\n" +
                "           and length(blicno) > 17) hd\n" +
                " where hd.blicno = en.credit_code\n" +
                "   and en.pripid <> hd.pripid";

        /*val hql2 = "select en.pripid      as startKey,\n" +
                "       en.credit_code,\n" +
                "       hd.condate,\n" +
                "       hd.subconam,\n" +
                "       hd.currency,\n" +
                "       hd.conprop,\n" +
                "       hd.pripid      as endKey\n" +
                "  from (select distinct pripid, credit_code\n" +
                "          from entInfoTmp03\n" +
                "         where credit_code <> ''\n" +
                "           and credit_code <> 'null') en,\n" +
                "       (select inv, condate, currency, subconam, conprop, blicno, pripid\n" +
                "          from invRelaTmp002\n" +
                "         where blicno <> ''\n" +
                "           and length(blicno) > 17) hd\n" +
                " where hd.blicno = en.credit_code\n" +
                "   and en.pripid <> hd.pripid";*/
        return DataFrameUtil.getDataFrame(sqlContext, hql, "invRelaTmp003");

    }

    private Dataset getInvRelaDF004(SparkSession sqlContext){
        String hql = "select a.inv,\n" +
                "       a.condate,\n" +
                "       a.currency,\n" +
                "       a.subconam,\n" +
                "       a.conprop,\n" +
                "       a.blicno,\n" +
                "       a.pripid\n" +
                "  from invRelaTmp002 a\n" +
                " where not exists  (select 1\n" +
                "          from invRelaTmp003 b\n" +
                "         where a.pripid = b.endKey\n" +
                "           and a.blicno = b.credit_code)";

       /* val hql3 =  "select a.inv,\n" +
                "       a.condate,\n" +
                "       a.currency,\n" +
                "       a.subconam,\n" +
                "       a.conprop,\n" +
                "       a.blicno,\n" +
                "       a.pripid\n" +
                "  from invRelaTmp002 a\n" +
                " where not exists  (select 1\n" +
                "          from invRelaTmp003 b\n" +
                "         where a.pripid = b.endKey\n" +
                "           and a.blicno = b.credit_code)";*/
        return DataFrameUtil.getDataFrame(sqlContext, hql, "invRelaTmp004");
    }


    private Dataset getInvRelaDF005(SparkSession sqlContext){
        String hql = "select distinct en.pripid   as startKey,\n" +
                "       hd.condate,\n" +
                "       hd.subconam,\n" +
                "       hd.currency,\n" +
                "       hd.conprop,\n" +
                "       hd.pripid   as endKey\n" +
                "  from S_EN_USEDNAME en,\n" +
                "       (select inv, condate, currency, subconam, conprop, blicno, pripid\n" +
                "          from invRelaTmp004\n" +
                "         where inv <> '') hd\n" +
                " where hd.inv = en.usedname";

/*
        val hql4 = "select distinct en.pripid   as startKey,\n" +
                "       hd.condate,\n" +
                "       hd.subconam,\n" +
                "       hd.currency,\n" +
                "       hd.conprop,\n" +
                "       hd.pripid   as endKey\n" +
                "  from S_EN_USEDNAME en,\n" +
                "       (select inv, condate, currency, subconam, conprop, blicno, pripid\n" +
                "          from invRelaTmp004\n" +
                "         where inv <> '') hd\n" +
                " where hd.inv = en.usedname";*/

        return DataFrameUtil.getDataFrame(sqlContext, hql, "invRelaTmp005");
    }

    private Dataset getInvRelaDF006(SparkSession sqlContext){
        String hql = "select startKey, condate, subconam, currency, conprop, endKey\n" +
                "  from invRelaTmp001\n" +
                "union\n" +
                "select startKey, condate, subconam, currency, conprop, endKey\n" +
                "  from invRelaTmp003\n" +
                "union\n" +
                "select startKey, condate, subconam, currency, conprop, endKey\n" +
                "  from invRelaTmp005";


       /* val hq5 = "select startKey, condate, subconam, currency, conprop, endKey\n" +
                "  from invRelaTmp001\n" +
                "union\n" +
                "select startKey, condate, subconam, currency, conprop, endKey\n" +
                "  from invRelaTmp003\n" +
                "union\n" +
                "select startKey, condate, subconam, currency, conprop, endKey\n" +
                "  from invRelaTmp005";*/
        return DataFrameUtil.getDataFrame(sqlContext, hql, "invRelaTmp03",DataFrameUtil.CACHETABLE_PARQUET);

    }


    private Dataset getInvRelaDF04(SparkSession sqlContext) {
        String hql = "select concat_ws('-',startKey,condate,subconam,currency,conprop) as dtcpara, endKey as pripid from invRelaTmp03 ";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "invRelaTmp031");
    }


    private Dataset getInvRelaDF041(SparkSession sqlContext){
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
    private Dataset getInvRelaDF05(SparkSession sqlContext) {
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


    /**
     * 企业参股股东
     * @param sqlContext
     * @return
     */
    public Dataset getInvJoinRelaDF(SparkSession sqlContext) {
        getPersonJoinRelaDF01(sqlContext);
        getInvJoinRelaDF01(sqlContext);
        getInvJoinRelaDF02(sqlContext);
        getInvJoinRelaDF03(sqlContext);
        getInvJoinRelaDF04(sqlContext);
     return   getInvJoinRelaDF05(sqlContext);
    }

    public Dataset getPersonJoinRelaDF01(SparkSession sqlContext) {
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

    private Dataset getInvJoinRelaDF01(SparkSession sqlContext) {
        String hql = "select startkey,endkey\n" +
                "  from invRelaTmp04 \n" +
                " where conprop > 0.2\n" +
                "   and conprop < 0.5";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "invJoinRelaTmp01");
    }

    private Dataset getInvJoinRelaDF02(SparkSession sqlContext) {
        String hql = "select a.startkey, a.endkey, b.startkey as person_id\n" +
                "  from invJoinRelaTmp01 a\n" +
                "  join personJoinRelaTmp01  b\n" +
                "    on a.startkey = b.endkey";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "invJoinRelaTmp02",DataFrameUtil.CACHETABLE_LAZY);
    }

    private Dataset getInvJoinRelaDF03(SparkSession sqlContext) {
        String hql = "select a.endkey, b.startkey as person_id\n" +
                "   from invJoinRelaTmp02 a\n" +
                "   join personJoinRelaTmp01 b\n" +
                "     on a.endkey = b.endkey";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "invJoinRelaTmp03");
    }
    private Dataset getInvJoinRelaDF04(SparkSession sqlContext) {
        String hql = " select distinct a.startkey, a.endkey\n" +
                "   from invJoinRelaTmp02 a\n" +
                "   join invJoinRelaTmp03 b\n" +
                "     on a.endkey = b.endkey\n" +
                "    and a.person_id = b.person_id";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "invJoinRelaTmp04");
    }
     private Dataset getInvJoinRelaDF05(SparkSession sqlContext) {
        String hql = "select distinct a.* from invRelaTmp04 a join invJoinRelaTmp04 b on  a.startkey=b.startkey and a.endkey=b.endkey";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "invJoinRelaTmp05");
    }


    /**
     * 人员投资
     * @param sqlContext
     * @return
     */
    public Dataset getPersonInv(SparkSession sqlContext){
        getPersonInv01(sqlContext);
        getPersonInv02(sqlContext);
        distinctEntRelationDF("personInvTmp012",sqlContext);

        return getPersonInv03(sqlContext);
    }

    private Dataset getPersonInv01(SparkSession sqlContext){
        String hql =
                " select distinct case\n" +
                        "                  when (pri.zspid = 'null' and pri.pripid <> 'null' and\n" +
                        "                       pri.inv <> '') then\n" +
                        "                   concat_ws('-', pri.pripid, pri.inv)\n" +
                        "                  else\n" +
                        "                   concat_ws('-', pri.zspid, pri.inv)\n" +
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


    private Dataset getPersonInv02(SparkSession sqlContext){
        String hql =
                " select concat_ws('-',startKey,condate,subconam,currency,conprop) as dtcpara, endKey as pripid from personInvTmp011";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "personInvTmp012");
    }

    private Dataset getPersonInv03(SparkSession sqlContext){
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
    public Dataset getPersonJoinRelaDF(SparkSession sqlContext) {
        getPersonJoinRelaDF02(sqlContext);
        return   getPersonJoinRelaDF03(sqlContext);
    }



    private Dataset getPersonJoinRelaDF02(SparkSession sqlContext) {
        String hql = "select startkey,endkey\n" +
                "  from personInvTmp01\n" +
                " where conprop > 0.2\n" +
                "   and conprop < 0.5 ";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "personJoinRelaTmp02");
    }

    private Dataset getPersonJoinRelaDF03(SparkSession sqlContext) {
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
     * 重复数据的处理
     * @param tableName
     * @param sqlContext
     * @return
     */
    public Dataset distinctEntRelationDF(String tableName,SparkSession sqlContext){
        distinctEntRelationDF01(tableName,sqlContext);
        distinctEntRelationDF02(tableName,sqlContext);
        distinctEntRelationDF03(tableName,sqlContext);
        distinctEntRelationDF04(tableName,sqlContext);
        distinctEntRelationDF05(tableName,sqlContext);
        distinctEntRelationDF06(tableName,sqlContext);
        distinctEntRelationDF071(tableName,sqlContext);
        Dataset df= distinctEntRelationDF08(tableName,sqlContext);
        return df;
    }


        private Dataset distinctEntRelationDF01(String tableName,SparkSession sqlContext){
        String hql = "select a.pripid, a.entstatus, a.entname, a.credit_code, a.regno, b.dtcpara\n" +
                "  from entInfoTmp03 a\n" +
                "  join "+tableName+" b\n" +
                "    on a.pripid = b.pripid";
        return DataFrameUtil.getDataFrame(sqlContext,hql,tableName+"tmp01",DataFrameUtil.CACHETABLE_PARQUET);
        }

    private Dataset distinctEntRelationDF02(String tableName,SparkSession sqlContext){
        String hql = "select  entname, credit_code, regno, dtcpara\n" +
                "  from "+tableName+"tmp01\n" +
                " group by entname, credit_code, regno, dtcpara\n" +
                " having count(1) > 1";
        return DataFrameUtil.getDataFrame(sqlContext,hql,tableName+"tmp02",DataFrameUtil.CACHETABLE_PARQUET);
    }

    private Dataset distinctEntRelationDF03(String tableName,SparkSession sqlContext){
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

    private Dataset distinctEntRelationDF04(String tableName,SparkSession sqlContext){
        String hql = "select  entname, credit_code, regno, dtcpara\n" +
                "  from "+tableName+"tmp03 \n" +
                " group by entname, credit_code, regno, dtcpara\n" +
                "having count(1) > 1";

        return DataFrameUtil.getDataFrame(sqlContext,hql,tableName+"tmp04");
    }

    private Dataset distinctEntRelationDF05(String tableName,SparkSession sqlContext){
        String hql = "select a.*\n" +
                "  from "+tableName+"tmp01 a\n" +
                "  join "+tableName+"tmp04 b\n" +
                "    on a.credit_code = b.credit_code\n" +
                "   and a.regno = b.regno\n" +
                "   and a.entname = b.entname\n" +
                "   and a.dtcpara = b.dtcpara";

        return DataFrameUtil.getDataFrame(sqlContext,hql,tableName+"tmp05");
    }

    private Dataset distinctEntRelationDF06(String tableName,SparkSession sqlContext){
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

    private Dataset distinctEntRelationDF071(String tableName,SparkSession sqlContext){
        String hql =
                "select b.*\n" +
                        "  from (select  entname, credit_code, regno, dtcpara\n" +
                        "          from "+tableName+"tmp03\n" +
                        "         group by entname, credit_code, regno, dtcpara\n" +
                        "        having count(1) = 1) a\n" +
                        "  join "+tableName+"tmp01 b\n" +
                        "    on a.entname = b.entname\n" +
                        "   and a.credit_code = b.credit_code\n" +
                        "   and a.regno = b.regno\n" +
                        "   and a.dtcpara = b.dtcpara\n" +
                        " where entstatus = '1' ";
        System.out.println(hql);

        return DataFrameUtil.getDataFrame(sqlContext,hql,tableName+"tmp071");
    }

    private Dataset distinctEntRelationDF08(String tableName,SparkSession sqlContext){
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
    public Dataset getBranchRelation(SparkSession sqlContext){
        String hql = "select pripid,brn_rpripid from  s_en_brn_org_hdfs_ext_%s where brn_rpripid <> 'null' " +
                "     union " +
                "     select a.pripid,b.pripid as brn_rpripid from s_en_brn_org_hdfs_ext_%s a join entInfoTmp03 b" +
                "     on a.brn_entname=b.entname and a.brn_regno=b.regno" +
                "     where a.brn_rpripid<>'null'" +
                "     and  a.brn_entname <>'null'";
        return DataFrameUtil.getDataFrame(sqlContext,String.format(hql,date,date),"branchRelationTmp01");
    }


    public Dataset getEntOrgRelatgion(SparkSession sqlContext){
        getEntOrgRelatgion01(sqlContext);
        return getEntOrgRelatgion02(sqlContext);
    }


    //
    private Dataset getEntOrgRelatgion01(SparkSession sqlContext){
        String hql =
                "select c.condate, c.inv, c.subconam, c.currency, c.conprop, c.pripid,c.zspid \n" +
                "  from (select a.condate, a.inv, a.subconam, a.currency, a.conprop, a.pripid,b.entname,a.zspid\n" +
                "          from e_inv_investment_parquet a\n" +
                "          left join (select a.startKey, a.endKey, b.entname,b.entname_old,b.credit_code\n" +
                "                      from invRelaTmp04 a\n" +
                "                      join entInfoTmp03 b\n" +
                "                        on a.startKey = b.pripid) b\n" +
                "           on a.pripid = b.endKey " +
                "           and (a.inv = b.entname or a.inv = b.entname_old or (a.blicno=b.credit_code and a.blicno<>''))\n" +
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


        return DataFrameUtil.getDataFrame(sqlContext,hql,"entOrgRelatgionTmp01",DataFrameUtil.CACHETABLE_PARQUET);
    }



    private Dataset getEntOrgRelatgion02(SparkSession sqlContext){
        String hql = " select distinct md5(org.inv) as startKey,\n" +
                "       org.condate,\n" +
                "       org.subconam,\n" +
                "       org.currency,\n" +
                "       org.conprop,\n" +
                "       org.pripid as endKey," +
                "        riskscore(1,concat_ws('|', ent.entstatus, ent.enttype, org.conprop)) as riskscore\n" +
                "  from entOrgRelatgionTmp01 org " +
                "  join  entInfoTmp03 ent " +
                "  on  org.pripid = ent.pripid \n" +
                " where length(org.inv) >= 4  " +
                "  and not exists (select 1 from S_EN_USEDNAME u where u.usedname=org.inv)";

       /* val hq3 = " select distinct md5(org.inv) as startKey,\n" +
                "       org.condate,\n" +
                "       org.subconam,\n" +
                "       org.currency,\n" +
                "       org.conprop,\n" +
                "       org.pripid as endKey" +
                "  from entOrgRelatgionTmp01 org " +
                "  join  entInfoTmp03 ent " +
                "  on  org.pripid = ent.pripid \n" +
                " where length(org.inv) >= 4  " +
                "  and not exists (select 1 from S_EN_USEDNAME u where u.usedname=org.inv)";*/

        return DataFrameUtil.getDataFrame(sqlContext,hql,"entOrgRelatgionTmp02");
    }

    /**
     * 企业相关关系合并为一条关系（inv，疑似关系）
     * @param sqlContext
     * @return
     */
    public Dataset getInvMergeRelationDF(SparkSession sqlContext) {
        getInvMergeRelationDF01(sqlContext);
        return   getInvMergeRelationDF02(sqlContext);
    }


    private Dataset getInvMergeRelationDF01(SparkSession sqlContext){
        String hql = "select startKey, riskscore, endKey\n" +
                "  from entinvmerge\n" +
                "union all\n" +
                "select pripid as startKey, riskscore, entpripid as endKey\n" +
                "  from entDomRelaTmp11\n" +
                "union all\n" +
                "select pripid as startKey, riskscore, entpripid as endKey\n" +
                "  from telRelaInfoTmp111 " ;
        return  DataFrameUtil.getDataFrame(sqlContext, hql, "invMergeRelationTm01");
    }


    public Dataset getOrgInvMerge(SparkSession sqlContext){
        String hql = " select startKey, riskscore, endKey  from  entOrgRelatgionTmp02";
        return  DataFrameUtil.getDataFrame(sqlContext, hql, "orginvmerge01");
    }

    private Dataset getInvMergeRelationDF02(SparkSession sqlContext){

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
     * 企业相关关系合并为一条关系--实质关系（merge_sz）
     * @param sqlContext
     * @return
     */
    public Dataset getEntMergeSZRelationDF(SparkSession sqlContext) {
        String hql = " select startkey,riskscore,endkey from entholdmerge";

        return DataFrameUtil.getDataFrame(sqlContext, hql, "entmergesz");
    }

    /**
     * 组织机构关关系合并为一条关系--实质关系（merge_sz）
     * @param sqlContext
     * @return
     */
    public Dataset getOrgMergeSZRelationDF(SparkSession sqlContext) {
        String hql = " select startkey,riskscore,endkey from orgholdmerge";

        return DataFrameUtil.getDataFrame(sqlContext, hql, "orgmergesz");
    }


    /**
     * 人员相关关系合并为一条关系（personhold，疑似关系,重要任职）--实质关系（merge_sz）
     * @param sqlContext
     * @return
     */
    public Dataset getPersonMergeSZRelationDF(SparkSession sqlContext) {
        getInvMergeSZRelationDF01(sqlContext);
        return   getInvMergeSZRelationDF02(sqlContext);
    }


    private Dataset getInvMergeSZRelationDF01(SparkSession sqlContext){

        String hql = " select startkey,riskscore,endkey from personhold_sz " +
                "      union " +
                "      select startkey,riskscore,endkey from mainstaffRelationTmp" +
                "      union " +
                "      select zspid as startKey, riskscore, pripid as endKey\n" +
                "  from legalRelaTmp02\n";

        return  DataFrameUtil.getDataFrame(sqlContext, hql, "invmergeszrelation");
    }


    private Dataset getInvMergeSZRelationDF02(SparkSession sqlContext){

        String hql = " select a.startKey,\n" +
                "       case\n" +
                "         when a.riskscore > 1 then\n" +
                "          1\n" +
                "         else\n" +
                "          a.riskscore\n" +
                "       end riskscore,\n" +
                "       a.endKey\n" +
                "  from (select startKey, sum(riskscore) as riskscore, endKey\n" +
                "          from invmergeszrelation\n" +
                "         group by startKey, endKey) a";

        return  DataFrameUtil.getDataFrame(sqlContext, hql, "invMergeRelationTm");
    }


    public Dataset getpersonOrgRelation(SparkSession sqlContext){

        sqlContext.read().load("/relation/cachetable/entOrgRelatgionTmp01").registerTempTable("entOrgRelatgionTmp01");

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


    /**
     * 人员相关关系合并为一条关系（inv，staff，legal,org）
     * @param sqlContext
     * @return
     */
    public Dataset getPersonMergeRelaDF(SparkSession sqlContext) {
        getPersonMergeRelaDF01(sqlContext);
        return   getPersonMergeRelaDF02(sqlContext);
    }

    private Dataset getPersonMergeRelaDF01(SparkSession sqlContext){
        String hql =
                "select zspid as startKey, riskscore, pripid as endKey\n" +
                "  from legalRelaTmp02\n" +
                "union all\n" +
                "select startKey, riskscore, endKey\n" +
                "  from staffRelationTmp " +
                " union all" +
                " select startKey,riskscore,endKey from personInvmerge";
        return  DataFrameUtil.getDataFrame(sqlContext, hql, "personMergeRelationTm01");
    }


    private Dataset getPersonMergeRelaDF02(SparkSession sqlContext){
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


    private Dataset getPersonMergeSZRelaDF02(SparkSession sqlContext){
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


    public Dataset getOrgNode(SparkSession sqlContext){
        String hql ="select distinct key,name,type,entstatus from" +
                "(select distinct md5(inv) as key, inv as name,'org' as type,'1' as entstatus\n" +
                "   from entOrgRelatgionTmp01\n" +
                "  where length(inv) >= 4" +
                "  union " +
                "   select md5(name) as key,name,type,entstatus from person_and_ent02 where length(name)>=4)";
        return DataFrameUtil.getDataFrame(sqlContext,hql,"orgNodeTmpe01");
    }

    private Dataset getHoldRelation01(SparkSession sqlContext){
        String hql = "select a.*,'person' as flag from personInv a" +
                "  union all " +
                "    select b.* ,'ent' as flag from invRelaTmp04 b    " +
                "   union all " +
                "    select c.*,'entorg' as flag from  orgInv c" ;
        return DataFrameUtil.getDataFrame(sqlContext, hql, "holdRelationTmp01",DataFrameUtil.CACHETABLE_LAZY);
    }

    private Dataset getHoldRelation02(SparkSession sqlContext){
        String hql = "select max(conprop) as conprop, endkey\n" +
                "          from holdRelationTmp01\n" +
                "         group by endkey";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "holdRelationTmp02");
    }

    private Dataset getHoldRelation03(SparkSession sqlContext){
        String hql = "select b.startKey, b.condate, b.subconam, b.currency, b.conprop, b.endKey,b.flag,b.riskscore \n" +
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
    public Dataset getPersonHoldRelaDF(SparkSession sqlContext) {
        getHoldRelation(sqlContext);
        return  getPersonHoldRelaDF01(sqlContext);
//          getPersonHoldRelaDF02(sqlContext);
    }

    private Dataset getPersonHoldRelaDF01(SparkSession sqlContext) {
        String hql = " select distinct b.startKey, b.condate, b.subconam, b.currency, b.conprop, b.endKey,b.riskscore from holdRelationTmp011 b where b.flag='person' and  b.conprop<>0.0";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "personHoldRelaTmp01");
    }



    public Dataset getHoldRelation(SparkSession sqlContext){
        getHoldRelation01(sqlContext);
        getHoldRelation02(sqlContext);
        return   getHoldRelation03(sqlContext);

    }


    /**
     * 企业控股股东
     * @param sqlContext
     * @return
     */
    public Dataset getInvHoldRelaDF(SparkSession sqlContext) {
          return getInvHoldRelaDF01(sqlContext);
    }

    private Dataset getInvHoldRelaDF01(SparkSession sqlContext) {
        String hql = " select distinct b.startKey, b.condate, b.subconam, b.currency, b.conprop, b.endKey,b.riskscore from holdRelationTmp011 b where b.flag='ent' and b.conprop<>0.0";
        return DataFrameUtil.getDataFrame(sqlContext, hql, "invHoldRelaTmp01");
    }



    /**
     * 组织机构控股股东
     * @param sqlContext
     * @return
     */
    public Dataset getOrgHoldRelaDF(SparkSession sqlContext) {
        String hql =" select distinct b.startKey, b.condate, b.subconam, b.currency, b.conprop, b.endKey,b.riskscore from holdRelationTmp011 b where b.flag='entorg' and  (b.conprop<>0.0 or b.startKey like '%-国务院国有资产监督管理委员会') ";
        return   DataFrameUtil.getDataFrame(sqlContext, hql, "orgHoldRelaTmp01");
    }

    /**
     * 获取主要管理职位
     */
    public Dataset mainStaff(SparkSession sqlContext){
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

    //上市公司匹配工商数据
    public Dataset beListedInfo(SparkSession sqlContext){
        String hql="select d.entname, d.pripid, d.compcode\n" +
                "  from (select b.entname, b.pripid, a.compcode\n" +
                "          from (select orgcode, compname, bizlicenseno, compcode\n" +
                "                  from TQ_COMP_INFO\n" +
                "                 where isbranch = 0\n" +
                "                   and islist = 1" +
                "                   and orgcode<>''" +
                "                   and orgcode<>'null'" +
                "                   and orgcode is not null ) a,\n" +
                "               entInfoTmp03 b\n" +
                "         where regexp_replace(a.orgcode, '-', '') = b.licid\n" +
                "        union all\n" +
                "        select b.entname, b.pripid, a.compcode\n" +
                "          from (select orgcode, compname, bizlicenseno, compcode\n" +
                "                  from TQ_COMP_INFO\n" +
                "                 where isbranch = 0\n" +
                "                   and islist = 1" +
                "                   and compname<>'') a\n" +
                "          join entInfoTmp03 b\n" +
                "            on a.compname = b.entname\n" +
                "        union all\n" +
                "        select b.entname, b.pripid, a.COMPCODE\n" +
                "          from (select orgcode, compname, bizlicenseno, compcode\n" +
                "                  from TQ_COMP_INFO\n" +
                "                 where isbranch = 0\n" +
                "                   and islist = 1" +
                "                   and bizlicenseno<> ''" +
                "                   and bizlicenseno is not null" +
                "                   and bizlicenseno<>'null') a,\n" +
                "               entInfoTmp03 b\n" +
                "         where a.bizlicenseno = b.regno) d\n" +
                " group by d.entname, d.pripid, d.compcode";


        return   DataFrameUtil.getDataFrame(sqlContext, hql, "comp_info_tmp",DataFrameUtil.CACHETABLE_PARQUET);
    }

    //十大股东节点
    public Dataset beTenInvInfo(SparkSession sqlContext){
        String hql="select MD5(concat_ws('-',pripid, '十大股东')) as tenid,\n" +
                "            '十大股东' as tenname\n" +
                "       from comp_info_tmp";

        return   DataFrameUtil.getDataFrame(sqlContext, hql, "teninv_tmp");
    }

    //十大股东关系
    public Dataset beTenInvRelationInfo(SparkSession sqlContext){
        String hql="select MD5(concat_ws('-',pripid, '十大股东')) as tenid, pripid\n" +
                "from comp_info_tmp";

        return   DataFrameUtil.getDataFrame(sqlContext, hql, "teninv_relation_tmp");
    }

    //上市公司股东
     private Dataset beListedEntInfo01(SparkSession sqlContext){
        String hql="select a.compcode, a.shholdername, a.shholdertype,round(a.holderrto, 1) as holderrto,a.sharestype,a.holderamt\n" +
                " from TQ_SK_SHAREHOLDER a\n" +
                " join (select compcode, max(enddate) as enddate \n" +
                "         from TQ_SK_SHAREHOLDER\n" +
                "        group by compcode) b\n" +
                "   on a.compcode = b.compcode\n" +
                "  and a.enddate = b.enddate";

        return   DataFrameUtil.getDataFrame(sqlContext, hql, "listed_ent_tmp");
    }

    //企业和股东(人和企业)
    private Dataset beListedPersonInvInfo01(SparkSession sqlContext){
        String hql="select distinct b.pripid, a.shholdername,a.shholdertype,a.holderrto,a.sharestype,a.holderamt\n" +
                "               from listed_ent_tmp a\n" +
                "               join comp_info_tmp b\n" +
                "                 on a.compcode = b.compcode\n" ;

        return   DataFrameUtil.getDataFrame(sqlContext, hql, "listed_ent_tmp01",DataFrameUtil.CACHETABLE_PARQUET);
    }

    //企业股东
    private Dataset beListedEntInvInfo(SparkSession sqlContext){
        String hql="select b.pripid,a.shholdername,a.holderrto,a.sharestype,a.holderamt, a.pripid as topripid\n" +
                "from (select a.pripid, a.shholdername,a.holderrto,a.sharestype,a.holderamt\n" +
                "        from listed_ent_tmp01 a\n" +
                "       where a.shholdertype <> 5) a,\n" +
                " entInfoTmp03 b\n" +
                "where b.entname = a.shholdername";

        return   DataFrameUtil.getDataFrame(sqlContext, hql, "listed_entinv_tmp");
    }



    //人员股东
    private Dataset beListedPersonInvInfo(SparkSession sqlContext){
        String hql="select case when a.zspid<>'' and a.zspid<>'null' then concat_ws('-',a.zspid,b.shholdername) else concat_ws('-',a.pripid,b.shholdername)" +
                "    end zspid, b.shholdername,b.holderrto,b.sharestype,b.holderamt,a.pripid\n" +
                "    from e_inv_investment_parquet a,\n" +
                "    (select c.pripid, c.shholdername,c.holderrto,c.sharestype,c.holderamt\n" +
                "               from listed_ent_tmp01 c\n" +
                "              where c.shholdertype = 5) b\n" +
                "   where a.pripid = b.pripid\n" +
                "     and a.inv = b.shholdername ";

        return   DataFrameUtil.getDataFrame(sqlContext, hql, "listed_personinv_tmp");
    }
    //十大股东（个人and企业）
    public Dataset beTenInvRelationInfo2(SparkSession sqlContext){

        String hql="select zspid as zsid, pripid as topripid, shholdername,holderrto,sharestype,holderamt,'1' as type\n" +
                "  from listed_personinv_tmp\n" +
                "  union all\n" +
                "  select pripid as zsid, topripid, shholdername,holderrto,sharestype,holderamt,'2' as type\n" +
                "  from listed_entinv_tmp";

        return   DataFrameUtil.getDataFrame(sqlContext, hql, "person_and_ent",DataFrameUtil.CACHETABLE_PARQUET);
    }

    public Dataset beListedEntInfo(SparkSession sqlContext){
        beListedEntInfo01(sqlContext);
        beListedPersonInvInfo01(sqlContext);
        beListedEntInvInfo(sqlContext);
        beListedPersonInvInfo(sqlContext);
        beTenInvRelationInfo2(sqlContext);
        return beTenEntInvRelationInfo(sqlContext);
    }


    //组织结构企业节点
    public Dataset beTenEntOrg(SparkSession sqlContext){
        beTenInvOrg(sqlContext);
        String hql="select md5(name) as key,name,type,entstatus from person_and_ent02 where length(name)>=4";
        return   DataFrameUtil.getDataFrame(sqlContext, hql, "ten_ent_org");
    }

    //组织机构节点
    private Dataset beTenInvOrg(SparkSession sqlContext){

        String hql="select distinct concat_ws('-', a.pripid, a.shholdername) as key,\n" +
                "                a.shholdername as name,\n" +
                "                'org' as type," +
                "                '1' as entstatus\n" +
                "  from listed_ent_tmp01 a\n" +
                " where not exists (select 1\n" +
                "          from person_and_ent b\n" +
                "         where b.topripid = a.pripid\n" +
                "           and b.shholdername = a.shholdername)";
        return   DataFrameUtil.getDataFrame(sqlContext, hql, "person_and_ent02",DataFrameUtil.CACHETABLE_PARQUET);
    }


    //人员节点
    public Dataset beTenPersonOrg(SparkSession sqlContext){

        String hql="select key,name,'' as riskinfo,'' as code from person_and_ent02 where length(name) < 4 ";
        return   DataFrameUtil.getDataFrame(sqlContext, hql, "ten_person_org");
    }



    /**
     * 十大流通股东组织机构股东关系（与目标企业直接连接 listedinv）
     *
     */
    public Dataset beListedOrgRelation(SparkSession sqlContext){

        beTenInvOrgRelation(sqlContext);
        return   beTenInvOrgRelation01(sqlContext);
    }


    //组织机构关系 listedinv
    private Dataset beTenInvOrgRelation(SparkSession sqlContext){

        String hql="select distinct concat_ws('-', a.pripid, a.shholdername) as key," +
                " a.holderrto,a.sharestype,a.holderamt,a.pripid,a.shholdername\n" +
                "   from listed_ent_tmp01 a\n" +
                "  where not exists (select 1\n" +
                "           from person_and_ent b\n" +
                "          where b.topripid = a.pripid\n" +
                "            and b.shholdername = a.shholdername)";

        Dataset df = DataFrameUtil.getDataFrame(sqlContext, hql, "person_and_ent03");


        return   df;
    }

    //组织机构关系
    private Dataset beTenInvOrgRelation01(SparkSession sqlContext){

        String hql="select md5(a.shholdername) as zsid, a.holderrto,a.sharestype,a.holderamt,a.pripid as topripid,riskscore(1,concat_ws('|',b.entstatus,b.enttype,a.holderrto/100)) as riskscore " +
                "   from person_and_ent03 a " +
                "   join entInfoTmp03 b" +
                "   on a.pripid = b.pripid " +
                "where length(a.shholdername)>=4";
        Dataset df = DataFrameUtil.getDataFrame(sqlContext, hql, "org_ent_listed");
        return   df;
    }


    /**
     * 十大流通股东人员股东关系(与目标企业直接相连 listedinv)
     */

    public Dataset beListesPersonRelation(SparkSession sqlContext){

        beListesPersonRelation01(sqlContext);
        return beListesPersonRelation02(sqlContext);
    }

    public Dataset beListesPersonRelation01(SparkSession sqlContext){

        String hql="select zsid,holderrto,sharestype,holderamt, topripid from person_and_ent where type='1'" +
                "   union " +
                "   select a.key as zsid, a.holderrto,a.sharestype,a.holderamt,a.pripid as topripid from person_and_ent03 a where length(a.shholdername)<4";

        return   DataFrameUtil.getDataFrame(sqlContext, hql, "org_person_listed");
    }

    public Dataset beListesPersonRelation02(SparkSession sqlContext){

        String hql="select a.zsid,a.holderrto,a.sharestype,a.holderamt, a.topripid, " +
                "   riskscore(1,concat_ws('|',b.entstatus,b.enttype,a.holderrto/100)) as riskscore"+
                "   from org_person_listed  a " +
                "   join entInfoTmp03 b " +
                "   on a.topripid = b.pripid ";

        return   DataFrameUtil.getDataFrame(sqlContext, hql, "org_person_listed");
    }


    /**
     * 十大流通股东企业股东关系(与目标企业直接相连 listedinv)
     */
    public Dataset beListesEntRelation(SparkSession sqlContext){

        String hql="select a.zsid,a.holderrto,a.sharestype,a.holderamt,a.topripid, " +
                "           riskscore(1,concat_ws('|',b.entstatus,b.enttype,a.holderrto/100)) as riskscore "+
                "           from person_and_ent  a " +
                "           join entInfoTmp03 b " +
                "           on a.topripid=b.pripid " +
                "           where a.type='2' ";
        return   DataFrameUtil.getDataFrame(sqlContext, hql, "org_person_listed01");
    }

    /**
     * 十大流通股东企业股东关系（与虚拟节点相连 listedinvnode）
     */
    public Dataset beTenEntInvRelationInfo(SparkSession sqlContext){

        String hql="select a.pripid,a.holderrto,a.sharestype,a.holderamt, b.tenid\n" +
                "  from listed_entinv_tmp a\n" +
                "  join teninv_relation_tmp b\n" +
                "    on a.topripid = b.pripid";

        return   DataFrameUtil.getDataFrame(sqlContext, hql, "ent_ten_inv_relation");
    }

    /**
     * 十大流通股东组织结构股东关系（与虚拟节点相连 listedinvnode）
     */
    public Dataset beTenOrgRelation(SparkSession sqlContext){
        String hql="select a.zsid, a.holderrto,a.sharestype,a.holderamt,c.tenid  from org_ent_listed a" +
                "   join teninv_relation_tmp c" +
                "   on a.topripid = c.pripid ";

        return   DataFrameUtil.getDataFrame(sqlContext, hql, "person_and_ent04");
    }

    /**
     *
     *十大股东人员股东关系（与虚拟节点相连 listedinvnode）
     */
    public Dataset beTenPersonInvRelationInfo(SparkSession sqlContext){

        String hql="select a.zsid, a.holderrto,a.sharestype,a.holderamt,b.tenid\n" +
                "  from org_person_listed a\n" +
                "  join teninv_relation_tmp b\n" +
                "    on a.topripid = b.pripid ";

        return   DataFrameUtil.getDataFrame(sqlContext, hql, "person_ten_inv_relation");
    }



    //十大股东控股
    public Dataset beTenInvHold(SparkSession sqlContext){

        beTenInvHold01(sqlContext);
        beTenInvHold02(sqlContext);
        return beTenInvHold03(sqlContext);
    }

    //十大股东控股
    public Dataset beTenInvHold01(SparkSession sqlContext){

        String hql="select a.compcode, a.shholdername, a.shholdertype,a.holderrto,a.sharestype,a.holderamt\n" +
                "  from listed_ent_tmp a,\n" +
                "       (select compcode, max(holderrto) as holderrto\n" +
                "          from listed_ent_tmp where shholdername<>'香港中央结算(代理人)有限公司'\n" +
                "         group by compcode) b\n" +
                " where a.compcode = b.compcode\n" +
                "   and a.holderrto = b.holderrto" ;

        return   DataFrameUtil.getDataFrame(sqlContext, hql, "be_teninv_hold");
    }


    public Dataset beTenInvHold02(SparkSession sqlContext){

        String hql="select a.shholdername,a.holderrto,a.sharestype,a.holderamt, b.pripid\n" +
                " from be_teninv_hold a\n" +
                " join comp_info_tmp b\n" +
                "   on a.compcode = b.compcode";

        return   DataFrameUtil.getDataFrame(sqlContext, hql, "ten_inv_hold01");
    }

    public Dataset beTenInvHold03(SparkSession sqlContext){

        String hql="select case\n" +
                "          when b.zsid is null then\n" +
                "           concat_ws('-',a.pripid, a.shholdername)\n" +
                "          else\n" +
                "           b.zsid\n" +
                "        end zsid,\n" +
                "        a.holderrto,a.sharestype,a.holderamt,\n" +
                "        a.pripid,\n" +
                "        case when b.type is null then '3'" +
                "        else" +
                "        b.type end type,\n" +
                "        a.shholdername\n" +
                "   from ten_inv_hold01 a\n" +
                "   left join person_and_ent b\n" +
                "     on a.shholdername = b.shholdername" +
                "     and a.pripid=b.topripid";

        return   DataFrameUtil.getDataFrame(sqlContext, hql, "ten_inv_hold02",DataFrameUtil.CACHETABLE_PARQUET);
    }


    public Dataset beTenPersonHold(SparkSession sqlContext){
        beTenPersonHold01(sqlContext);
        return beTenPersonHold02(sqlContext);
    }

    public Dataset beTenPersonHold01(SparkSession sqlContext){
        beTenInvHold(sqlContext);
        String hql = "select zsid,holderrto,sharestype,holderamt,pripid from ten_inv_hold02 where type='1'" +
                "     union " +
                "     select zsid,holderrto,sharestype,holderamt,pripid from ten_inv_hold02 where type='3' and length(shholdername)<4";

        return   DataFrameUtil.getDataFrame(sqlContext, hql, "ten_person_hold");
    }

    public Dataset beTenPersonHold02(SparkSession sqlContext){
        String hql = "select a.zsid,a.holderrto,a.sharestype,a.holderamt,a.pripid ," +
                "      riskscore(1,concat_ws('|',b.entstatus,b.enttype,a.holderrto/100)) as riskscore"+
                "      from  ten_person_hold a " +
                "      join entInfoTmp03 b " +
                "       on   a.pripid=b.pripid";

        return   DataFrameUtil.getDataFrame(sqlContext, hql, "ten_person_hold01");
    }



    public Dataset beTenEntHold(SparkSession sqlContext){
        String hql = "select a.zsid,a.holderrto,a.sharestype,a.holderamt,a.pripid," +
                "       riskscore(1,concat_ws('|',b.entstatus,b.enttype,a.holderrto/100)) as riskscore"+
                " from ten_inv_hold02 a " +
                "  join entInfoTmp03 b " +
                "   on a.pripid=b.pripid" +
                "   where a.type='2' ";

        return   DataFrameUtil.getDataFrame(sqlContext, hql, "ten_ent_hold");
    }

    public Dataset beTenOrgHold(SparkSession sqlContext){
        String hql = "select md5(a.shholdername) as zsid,a.holderrto,a.sharestype,a.holderamt,a.pripid, " +
                "          riskscore(1,concat_ws('|',b.entstatus,b.enttype,a.holderrto/100)) as riskscore" +
                "   from ten_inv_hold02 a" +
                "   join entInfoTmp03 b" +
                "   on a.pripid=b.pripid " +
                " where a.type='3' and length(a.shholdername)>=4";
        return   DataFrameUtil.getDataFrame(sqlContext, hql, "ten_org_hold");
    }


    public Dataset getPersonMergeToTenListed(SparkSession sqlContext){
        return DataFrameUtil.getDataFrame(sqlContext,getHqlString("listedinvperson","personInv"),"personInvmerge");
    }

    public Dataset getEntMergeToTenListed(SparkSession sqlContext){
        return DataFrameUtil.getDataFrame(sqlContext,getHqlString("listedinvent","invRelaTmp04"),"entinvmerge");
    }

    public Dataset getOrgMergeToTenListed(SparkSession sqlContext){
        return DataFrameUtil.getDataFrame(sqlContext,getHqlString("listedinvorg","orgInv"),"orginvmerge");
    }

    public Dataset getpersonHoldMergeToTenListed(SparkSession sqlContext){
        return DataFrameUtil.getDataFrame(sqlContext,getHoldHqlString("listedholdperson","personhold"),"personmerge");
    }

    public Dataset getEntHoldMergeToTenListed(SparkSession sqlContext){
        return DataFrameUtil.getDataFrame(sqlContext,getHoldHqlString("listedholdent","enthold"),"entholdmerge");
    }

    public Dataset getOrgHoldMergeToTenListed(SparkSession sqlContext){
        return DataFrameUtil.getDataFrame(sqlContext,getHoldHqlString("listedholdorg","orghold"),"orgholdmerge");
    }


    private String getHqlString(String listedtable,String table){

        String hql = "select a.startKey,\n" +
                "       a.riskscore,\n" +
                "       a.endkey,\n" +
                "       '' as holderrto,\n" +
                "       '' as sharestype,\n" +
                "       '' as holderamt,\n" +
                "       a.condate,\n" +
                "       a.subconam,\n" +
                "       a.currency,\n" +
                "       a.conprop\n" +
                "  from "+table+" a\n" +
//                " where not exists (select 1 from "+listedtable+" b where a.endkey = b.topripid)\n" +
                " where not exists (select 1 from comp_info_tmp b where a.endkey = b.pripid)\n" +
                "union\n" +
                "select a.zsid as startKey,\n" +
                "       a.riskscore,\n" +
                "       a.topripid as endkey,\n" +
                "       a.holderrto,\n" +
                "       a.sharestype,\n" +
                "       a.holderamt,\n" +
                "       '' as condate,\n" +
                "       '' as subconam,\n" +
                "       '' as currency,\n" +
                "       '' as conprop\n" +
                "  from "+listedtable+" a ";

        return hql;
    }


    private String getHoldHqlString(String listedtable,String table){

        String hql = "select a.startKey,\n" +
                "       a.riskscore,\n" +
                "       a.endKey,\n" +
                "       '' as holderrto,\n" +
                "       '' as sharestype,\n" +
                "       '' as holderamt,\n" +
                "       a.condate,\n" +
                "       a.subconam,\n" +
                "       a.currency,\n" +
                "       a.conprop\n" +
                "  from "+table+" a\n" +
//                " where not exists (select 1 from "+listedtable+" b where a.endKey = b.pripid)\n" +
                " where not exists (select 1 from comp_info_tmp b where a.endkey = b.pripid)\n" +
                " union\n" +
                "select a.zsid,\n" +
                "       a.riskscore,\n" +
                "       a.pripid,\n" +
                "       a.holderrto,\n" +
                "       a.sharestype,\n" +
                "       a.holderamt,\n" +
                "       '' as condate,\n" +
                "       '' as subconam,\n" +
                "       '' as currency,\n" +
                "       '' as conprop\n" +
                "  from "+listedtable+" a";

/*
        val hql = "select a.startKey,\n" +
                "       a.riskscore,\n" +
                "       a.endKey,\n" +
                "       '' as holderrto,\n" +
                "       '' as sharestype,\n" +
                "       '' as holderamt,\n" +
                "       a.condate,\n" +
                "       a.subconam,\n" +
                "       a.currency,\n" +
                "       a.conprop\n" +
                "  from orghold a\n" +
//                " where not exists (select 1 from "+listedtable+" b where a.endKey = b.pripid)\n" +
                " where not exists (select 1 from comp_info_tmp b where a.endkey = b.pripid)\n" +
                " union\n" +
                "select a.zsid,\n" +
                "       a.riskscore,\n" +
                "       a.pripid,\n" +
                "       a.holderrto,\n" +
                "       a.sharestype,\n" +
                "       a.holderamt,\n" +
                "       '' as condate,\n" +
                "       '' as subconam,\n" +
                "       '' as currency,\n" +
                "       '' as conprop\n" +
                "  from tenorg a";*/


        return hql;
    }
}
