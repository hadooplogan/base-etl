package com.chinadaas.association.etl.table;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.chinadaas.association.etl.sparksql.Hdfs2EsETL;
import com.google.common.base.Optional;
import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

/**
 * Created by gongxs01 on 2017/6/2.
 */
public class EntBaseInfoEO implements Serializable {


    private static final long serialVersionUID = -191313031740635751L;

    public EntBaseInfoEO(String s_ext_nodenum, String pripid, String entname, String regno, String enttype, String industryphy, String industryco,
                         String abuitem, String opfrom, String opto, String postalcode, String tel, String email, String esdate, String apprdate, String regorg,
                         String entstatus, String regcap, String opscope, String opform, String dom, String reccap, String regcapcur,
                         String forentname, String country, String entname_old, String name, String ancheyear, String candate, String revdate,
                         String licid, String credit_code, String tax_code, String zspid) {
        this.s_ext_nodenum = s_ext_nodenum;
        this.pripid = pripid;
        this.entname = entname;
        this.regno = regno;
        this.enttype = enttype;
        this.industryphy = industryphy;
        this.industryco = industryco;
        this.abuitem = abuitem;
        this.opfrom = opfrom;
        this.opto = opto;
        this.postalcode = postalcode;
        this.tel = tel;
        this.email = email;
        this.esdate = esdate;
        this.apprdate = apprdate;
        this.regorg = regorg;
        this.entstatus = entstatus;
        this.regcap = regcap;
        this.opscope = opscope;
        this.opform = opform;
        this.dom = dom;
        this.reccap = reccap;
        this.regcapcur = regcapcur;
        this.forentname = forentname;
        this.country = country;
        this.entname_old = entname_old;
        this.name = name;
        this.ancheyear = ancheyear;
        this.candate = candate;
        this.revdate = revdate;
        this.licid = licid;
        this.credit_code = credit_code;
        this.tax_code = tax_code;
        this.zspid = zspid;


    }



    public static JavaRDD<EntBaseInfoEO> convertData(HiveContext sqlContext) {

        Hdfs2EsETL hdfs = new Hdfs2EsETL();
        //投资企业
        JavaPairRDD<String, Iterable<JSONObject>> inv = hdfs.getEntInvDf(sqlContext).toJavaRDD().mapToPair(new PairFunction<Row, String, JSONObject>() {
            @Override
            public Tuple2<String, JSONObject> call(Row row) throws Exception {

                return new Tuple2(row.getAs("pripid").toString(),  JSONObject.parseObject(JSON.toJSONString(converInvMap(row),true)));
            }
        }).groupByKey();

        //管理人员
        JavaPairRDD<String, Iterable<JSONObject>> person = hdfs.getPersonManagerDf(sqlContext).toJavaRDD().mapToPair(new PairFunction<Row, String, JSONObject>() {
            @Override
            public Tuple2<String, JSONObject> call(Row row) throws Exception {

                return new Tuple2<>(row.getAs("pripid").toString(), JSONObject.parseObject(JSON.toJSONString(converPersonMap(row),true)));
            }
        }).groupByKey();

        JavaPairRDD<String, EntBaseInfoEO> entda = hdfs.getEntDataFrame(sqlContext).toJavaRDD().mapToPair(new PairFunction<Row, String, EntBaseInfoEO>() {
            @Override
            public Tuple2<String, EntBaseInfoEO> call(Row row) throws Exception {
                return new Tuple2(row.getAs("pripid").toString(), new EntBaseInfoEO(row.getString(0),
                        row.getString(1), row.getString(2), row.getString(3), row.getString(4),
                        row.getString(5), row.getString(6), row.getString(7), row.getString(8),
                        row.getString(9), row.getString(10), row.getString(11), row.getString(12),
                        row.getString(13), row.getString(14), row.getString(15), row.getString(16),
                        row.getString(17), row.getString(18), row.getString(19), row.getString(20),
                        row.getString(21), row.getString(22), row.getString(23), row.getString(24),
                        row.getString(25), row.getString(26), row.getString(27), row.getString(28),
                        row.getString(29), row.getString(30), row.getString(31), row.getString(32),
                        row.getString(33)));
            }
        });

        //主体企业
        JavaRDD<EntBaseInfoEO> entEsData = entda.leftOuterJoin(inv).leftOuterJoin(person).map(
                new Function<Tuple2<String, Tuple2<Tuple2<EntBaseInfoEO, Optional<Iterable<JSONObject>>>,
                        Optional<Iterable<JSONObject>>>>, EntBaseInfoEO>() {
                    @Override
                    public EntBaseInfoEO call(Tuple2<String, Tuple2<Tuple2<EntBaseInfoEO,
                            Optional<Iterable<JSONObject>>>, Optional<Iterable<JSONObject>>>>
                                                      tupleEs) throws Exception {
                        EntBaseInfoEO entEO = tupleEs._2()._1()._1();
                        if (tupleEs._2()._1()._2().isPresent()) {
                            entEO.setInv(IteratorUtils.toList(tupleEs._2()._1()._2().get().iterator()));
                        }
                        if (tupleEs._2()._2().isPresent()) {
                            entEO.setPerson(IteratorUtils.toList(tupleEs._2()._2().get().iterator()));
                        }
                        return entEO;
                    }
                });
        return entEsData;
    }

    private static Map<String,String> converInvMap(Row row){
        Map<String,String> invMap = new HashMap<String,String>(20);
        invMap.put("s_ext_nodenum",row.getString(0));
        invMap.put("pripid",row.getString(1));
        invMap.put("invid",row.getString(2));
        invMap.put("inv",row.getString(3));
        invMap.put("invtype",row.getString(4));
        invMap.put("certype",row.getString(5));
        invMap.put("cerno",row.getString(6));
        invMap.put("blictype",row.getString(7));
        invMap.put("blicno",row.getString(8));
        invMap.put("country",row.getString(9));
        invMap.put("currency",row.getString(10));
        invMap.put("subconam",row.getString(11));
        invMap.put("acconam",row.getString(12));
        invMap.put("conprop",row.getString(13));
        invMap.put("conform",row.getString(14));
        invMap.put("condate",row.getString(15));
        invMap.put("conam",row.getString(16));
        invMap.put("cerno_old",row.getString(17));
        invMap.put("zspid",row.getString(18));
        return invMap;
    }

    private static Map<String,String> converPersonMap(Row row){
        Map<String,String> personMap = new HashMap<String,String>(20);
        personMap.put("s_ext_nodenum",row.getString(0));
        personMap.put("pripid",row.getString(1));
        personMap.put("name",row.getString(2));
        personMap.put("certype",row.getString(3));
        personMap.put("cerno",row.getString(4));
        personMap.put("sex",row.getString(5));
        personMap.put("natdate",row.getString(6));
        personMap.put("lerepsign",row.getString(7));
        personMap.put("country",row.getString(8));
        personMap.put("position",row.getString(9));
        personMap.put("offhfrom",row.getString(10));
        personMap.put("offhto",row.getString(11));
        personMap.put("zspid",row.getString(12));
        return personMap;
    }



    private String s_ext_nodenum;
    private String pripid;
    private String entname;
    private String regno;
    private String enttype;
    private String industryphy;
    private String industryco;
    private String abuitem;
    private String opfrom;
    private String opto;
    private String postalcode;
    private String tel;
    private String email;
    private String esdate;
    private String apprdate;
    private String regorg;
    private String entstatus;
    private String regcap;
    private String opscope;
    private String opform;
    private String dom;
    private String reccap;
    private String regcapcur;
    private String forentname;
    private String country;
    private String entname_old;
    private String name;
    private String ancheyear;
    private String candate;
    private String revdate;
    private String licid;
    private String credit_code;
    private String tax_code;
    private String zspid;

    private List<JSONObject> inv;

    private List<JSONObject> person;

    public List<JSONObject> getInv() {
        return inv;
    }

    public void setInv(List<JSONObject> inv) {
        this.inv = inv;
    }

    public List<JSONObject> getPerson() {
        return person;
    }

    public void setPerson(List<JSONObject> person) {
        this.person = person;
    }

    public String getS_ext_nodenum() {
        return s_ext_nodenum;
    }

    public void setS_ext_nodenum(String s_ext_nodenum) {
        this.s_ext_nodenum = s_ext_nodenum;
    }

    public String getPripid() {
        return pripid;
    }

    public void setPripid(String pripid) {
        this.pripid = pripid;
    }

    public String getEntname() {
        return entname;
    }

    public void setEntname(String entname) {
        this.entname = entname;
    }

    public String getRegno() {
        return regno;
    }

    public void setRegno(String regno) {
        this.regno = regno;
    }

    public String getEnttype() {
        return enttype;
    }

    public void setEnttype(String enttype) {
        this.enttype = enttype;
    }

    public String getIndustryphy() {
        return industryphy;
    }

    public void setIndustryphy(String industryphy) {
        this.industryphy = industryphy;
    }

    public String getIndustryco() {
        return industryco;
    }

    public void setIndustryco(String industryco) {
        this.industryco = industryco;
    }

    public String getAbuitem() {
        return abuitem;
    }

    public void setAbuitem(String abuitem) {
        this.abuitem = abuitem;
    }

    public String getOpfrom() {
        return opfrom;
    }

    public void setOpfrom(String opfrom) {
        this.opfrom = opfrom;
    }

    public String getOpto() {
        return opto;
    }

    public void setOpto(String opto) {
        this.opto = opto;
    }

    public String getPostalcode() {
        return postalcode;
    }

    public void setPostalcode(String postalcode) {
        this.postalcode = postalcode;
    }

    public String getTel() {
        return tel;
    }

    public void setTel(String tel) {
        this.tel = tel;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getEsdate() {
        return esdate;
    }

    public void setEsdate(String esdate) {
        this.esdate = esdate;
    }

    public String getApprdate() {
        return apprdate;
    }

    public void setApprdate(String apprdate) {
        this.apprdate = apprdate;
    }

    public String getRegorg() {
        return regorg;
    }

    public void setRegorg(String regorg) {
        this.regorg = regorg;
    }

    public String getEntstatus() {
        return entstatus;
    }

    public void setEntstatus(String entstatus) {
        this.entstatus = entstatus;
    }

    public String getRegcap() {
        return regcap;
    }

    public void setRegcap(String regcap) {
        this.regcap = regcap;
    }

    public String getOpscope() {
        return opscope;
    }

    public void setOpscope(String opscope) {
        this.opscope = opscope;
    }

    public String getOpform() {
        return opform;
    }

    public void setOpform(String opform) {
        this.opform = opform;
    }

    public String getDom() {
        return dom;
    }

    public void setDom(String dom) {
        this.dom = dom;
    }

    public String getReccap() {
        return reccap;
    }

    public void setReccap(String reccap) {
        this.reccap = reccap;
    }

    public String getRegcapcur() {
        return regcapcur;
    }

    public void setRegcapcur(String regcapcur) {
        this.regcapcur = regcapcur;
    }

    public String getForentname() {
        return forentname;
    }

    public void setForentname(String forentname) {
        this.forentname = forentname;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getEntname_old() {
        return entname_old;
    }

    public void setEntname_old(String entname_old) {
        this.entname_old = entname_old;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAncheyear() {
        return ancheyear;
    }

    public void setAncheyear(String ancheyear) {
        this.ancheyear = ancheyear;
    }

    public String getCandate() {
        return candate;
    }

    public void setCandate(String candate) {
        this.candate = candate;
    }

    public String getRevdate() {
        return revdate;
    }

    public void setRevdate(String revdate) {
        this.revdate = revdate;
    }

    public String getLicid() {
        return licid;
    }

    public void setLicid(String licid) {
        this.licid = licid;
    }

    public String getCredit_code() {
        return credit_code;
    }

    public void setCredit_code(String credit_code) {
        this.credit_code = credit_code;
    }

    public String getTax_code() {
        return tax_code;
    }

    public void setTax_code(String tax_code) {
        this.tax_code = tax_code;
    }

    public String getZspid() {
        return zspid;
    }

    public void setZspid(String zspid) {
        this.zspid = zspid;
    }

}
