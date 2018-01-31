package com.chinadaas.association.etl.table;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.chinadaas.association.etl.sparksql.Hdfs2EsETL;
import com.chinadaas.common.util.TimeUtil;
import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
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
                         String licid, String credit_code, String tax_code, String zspid,String empnum,String cerno,String oriregno,String entitytype,
                         String encode_v1,String shortname,String s_ext_sequence,String data_date,String docid,String cbuitem) {
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
        this.empnum=empnum;
        this.cerno=cerno;
        this.oriregno=oriregno;
        this.entitytype=entitytype;
        this.pinyinname=entname;
        this.exactentname=entname;

//        this.entname_smart=entname;

        this.encode_v1=encode_v1;
        this.shortname=shortname;
        this.docid = docid;
        this.s_ext_sequence=s_ext_sequence;
        this.data_date = data_date;
        this.write_date = TimeUtil.getNowStr();
        this.cbuitem =  cbuitem;

    }

    public static JavaRDD<EntBaseInfoEO>  convertEntData(SparkSession sqlContext, Hdfs2EsETL hdfs) {
        //股东
        JavaPairRDD<String, Iterable<JSONObject>> inv = getJavaRddEntInv(hdfs.getEntInvDf(sqlContext));
        //管理人员
        JavaPairRDD<String, Iterable<JSONObject>> person = getJavaRddPerson(hdfs.getPersonManagerDf(sqlContext));
        //主体企业
        JavaPairRDD<String, EntBaseInfoEO> entda = getJavaRddEnt(hdfs.getEntDataFrame(sqlContext));


        JavaPairRDD<String,EntBaseInfoEO> entInv = entda.leftOuterJoin(inv).mapToPair(new PairFunction<Tuple2<String,
                        Tuple2<EntBaseInfoEO, Optional<Iterable<JSONObject>>>>, String, EntBaseInfoEO>() {
            @Override
            public Tuple2<String, EntBaseInfoEO> call(Tuple2<String, Tuple2<EntBaseInfoEO,
                                Optional<Iterable<JSONObject>>>> stringTuple2Tuple2) throws Exception {
                EntBaseInfoEO entEO = stringTuple2Tuple2._2()._1();

                if(stringTuple2Tuple2._2()._2().isPresent()){
                    entEO.setInv(IteratorUtils.toList(stringTuple2Tuple2._2()._2().get().iterator()));
                }
                return new Tuple2(stringTuple2Tuple2._1,entEO);
            }
        });

        return convertPersonData(entInv,person);
    }

    private static JavaRDD<EntBaseInfoEO> convertPersonData(JavaPairRDD<String, EntBaseInfoEO> ent,
                                                          JavaPairRDD<String, Iterable<JSONObject>> person) {
        JavaRDD<EntBaseInfoEO> entEsData =  ent.leftOuterJoin(person).map(
                new Function<Tuple2<String, Tuple2<EntBaseInfoEO, Optional<Iterable<JSONObject>>>>, EntBaseInfoEO>() {
                    @Override
                    public EntBaseInfoEO call(Tuple2<String, Tuple2<EntBaseInfoEO, Optional<Iterable<JSONObject>>>> stringTuple2Tuple2) throws Exception {
                        EntBaseInfoEO entperson = stringTuple2Tuple2._2()._1();
                        if(stringTuple2Tuple2._2()._2().isPresent()){
                            entperson.setPerson(IteratorUtils.toList(stringTuple2Tuple2._2()._2().get().iterator()));
                        }
                        return entperson;
                    }
                });
        return entEsData;
    }

    public static JavaRDD<EntBaseInfoEO> convertGtEntData(SparkSession sqlContext, Hdfs2EsETL hdfs) {
        //管理人员
        JavaPairRDD<String, Iterable<JSONObject>> person = getJavaRddPerson(hdfs.getGtPersonManagerDf(sqlContext));
        //主体企业
        JavaPairRDD<String, EntBaseInfoEO> entda = getJavaRddEnt(hdfs.getGtEntBaseInfo(sqlContext));

        return convertPersonData(entda,person);
    }



    private static JavaPairRDD<String, Iterable<JSONObject>> getJavaRddEntInv(Dataset df){
       return df.toJavaRDD().mapToPair(new PairFunction<Row, String, JSONObject>() {
            @Override
            public Tuple2<String, JSONObject> call(Row row) throws Exception {
                return new Tuple2(row.getAs("pripid").toString(),
                        JSONObject.parseObject(JSON.toJSONString(converInvMap(row),true)));
            }
        }).groupByKey();
    }


    private static JavaPairRDD<String, Iterable<JSONObject>> getJavaRddPerson(Dataset df){
        return df.toJavaRDD().mapToPair(new PairFunction<Row, String, JSONObject>() {
            @Override
            public Tuple2<String, JSONObject> call(Row row) throws Exception {
                return new Tuple2<>(row.getAs("pripid").toString(),
                        JSONObject.parseObject(JSON.toJSONString(converPersonMap(row),true)));
            }
        }).groupByKey();
    }

    public static JavaPairRDD<String, EntBaseInfoEO> getJavaRddEnt(Dataset df){
        return df.toJavaRDD().mapToPair(new PairFunction<Row, String, EntBaseInfoEO>() {
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
                        row.getString(33), row.getString(34), row.getString(35), row.getString(36),
                        row.getString(37),row.getString(38),row.getString(39),row.getString(40),row.getString(41),
                        row.getString(42),row.getString(43)));
            }
        });
    }


    public static Map<String,String> converInvMap(Row row){

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
        invMap.put("conprop",row.getAs("conprop")==null?"0.0":row.getAs("conprop").toString());
        invMap.put("conform",row.getString(14));
        invMap.put("condate",row.getString(15));
        invMap.put("conam",row.getString(16));
        invMap.put("cerno_old",row.getString(17));
        invMap.put("zspid",row.getString(18));
        invMap.put("encode_v1",row.getString(19));
        invMap.put("s_ext_sequence",row.getString(20));

        return invMap;
    }


    public static Map<String,String> converPersonMap(Row row){
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
        personMap.put("encode_v1",row.getString(13));
        personMap.put("s_ext_sequence",row.getString(14));
        return personMap;
    }


    @Override
    public String toString() {
        return "EntBaseInfoEO{" +
                "s_ext_nodenum='" + s_ext_nodenum + '\'' +
                ", pripid='" + pripid + '\'' +
                ", entname='" + entname + '\'' +
                ", regno='" + regno + '\'' +
                ", enttype='" + enttype + '\'' +
                ", industryphy='" + industryphy + '\'' +
                ", industryco='" + industryco + '\'' +
                ", abuitem='" + abuitem + '\'' +
                ", opfrom='" + opfrom + '\'' +
                ", opto='" + opto + '\'' +
                ", postalcode='" + postalcode + '\'' +
                ", tel='" + tel + '\'' +
                ", email='" + email + '\'' +
                ", esdate='" + esdate + '\'' +
                ", apprdate='" + apprdate + '\'' +
                ", regorg='" + regorg + '\'' +
                ", entstatus='" + entstatus + '\'' +
                ", regcap='" + regcap + '\'' +
                ", opscope='" + opscope + '\'' +
                ", opform='" + opform + '\'' +
                ", dom='" + dom + '\'' +
                ", reccap='" + reccap + '\'' +
                ", regcapcur='" + regcapcur + '\'' +
                ", forentname='" + forentname + '\'' +
                ", country='" + country + '\'' +
                ", entname_old='" + entname_old + '\'' +
                ", name='" + name + '\'' +
                ", ancheyear='" + ancheyear + '\'' +
                ", candate='" + candate + '\'' +
                ", revdate='" + revdate + '\'' +
                ", licid='" + licid + '\'' +
                ", credit_code='" + credit_code + '\'' +
                ", tax_code='" + tax_code + '\'' +
                ", zspid='" + zspid + '\'' +
                ", empnum='" + empnum + '\'' +
                ", cerno='" + cerno + '\'' +
                ", oriregno='" + oriregno + '\'' +
                ", entitytype='" + entitytype + '\'' +
                ", exactentname='" + exactentname + '\'' +
                ", pinyinname='" + pinyinname + '\'' +
                ", shortname='" + shortname + '\'' +
                ", encode_v1='" + encode_v1 + '\'' +
                ", inv=" + inv +
                ", person=" + person +
                '}';
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
    private String empnum;
    private String cerno;
    private String oriregno;
    private String entitytype;
    private String exactentname;
    private String pinyinname;
//    private String engname;
//    private String exactdom;
    private String shortname;
    private String docid;

    private String data_date;

    private String write_date;

//    private String entname_smart;

    private List<JSONObject> inv;

    private List<JSONObject> person;

    private String  cbuitem;

    public String getCbuitem() {
        return cbuitem;
    }

    public void setCbuitem(String cbuitem) {
        this.cbuitem = cbuitem;
    }

    private String s_ext_sequence;

    public String getS_ext_sequence() {
        return s_ext_sequence;
    }

    public void setS_ext_sequence(String s_ext_sequence) {
        this.s_ext_sequence = s_ext_sequence;
    }

    public String getPinyinname() {
        return pinyinname;
    }

    public void setPinyinname(String pinyinname) {
        this.pinyinname = pinyinname;
    }

    public String getData_date() {
        return data_date;
    }

    public void setData_date(String data_date) {
        this.data_date = data_date;
    }

    public String getWrite_date() {
        return write_date;
    }

    public void setWrite_date(String write_date) {
        this.write_date = write_date;
    }

/*    public String getEntname_smart() {
        return entname_smart;
    }

    public void setEntname_smart(String entname_smart) {
        this.entname_smart = entname_smart;
    }*/

    public String getDocid() {
        return docid;
    }

    public void setDocid(String docid) {
        this.docid = docid;
    }

    public String getShortname() {
        return shortname;
    }

    public void setShortname(String shortname) {
        this.shortname = shortname;
    }

    public String getExactentname() {
        return exactentname;
    }

    public void setExactentname(String exactentname) {
        this.exactentname = exactentname;
    }

    private String encode_v1;

    public String getEncode_v1() {
        return encode_v1;
    }


    public void setEncode_v1(String encode_v1) {
        this.encode_v1 = encode_v1;
    }

/*    public String getPinyinname() {
        return pinyinname;
    }

    public void setPinyinname(String pinyinname) {
        this.pinyinname = pinyinname;
    }*/

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public String getEmpnum() {
        return empnum;
    }

    public void setEmpnum(String empnum) {
        this.empnum = empnum;
    }

    public String getCerno() {
        return cerno;
    }

    public void setCerno(String cerno) {
        this.cerno = cerno;
    }

    public String getOriregno() {
        return oriregno;
    }

    public void setOriregno(String oriregno) {
        this.oriregno = oriregno;
    }

    public String getEntitytype() {
        return entitytype;
    }

    public void setEntitytype(String entitytype) {
        this.entitytype = entitytype;
    }

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
