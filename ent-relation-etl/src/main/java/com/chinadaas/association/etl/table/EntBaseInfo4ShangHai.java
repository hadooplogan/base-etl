package com.chinadaas.association.etl.table;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.chinadaas.association.etl.sparksql.Hdfs2EsETL4ShangHai;
import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by hejianning on 2017/9/20.
 *
 * 上海电信企业信息实体类
 */
public class EntBaseInfo4ShangHai implements Serializable{

    private static final long serialVersionUID = -191313031740635751L;

    private String s_ext_nodenum;
    private String pripid;
    private String entname;
    private String regno;
    private String enttype;
    private String industryphy;
    private String industryco;
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
    private String dom;
    private String regcapcur;
    private String entname_old;
    private String name;
    private String candate;
    private String revdate;
    private String licid;
    private String credit_code;
    private String tax_code;
    private String oriregno;
//    private String entname_smart;
    private String exactentname;

    private String ancheyear_tel;
    private String ancheyear_email;

    private String s_ext_sequence;

    private String entitytype;


    public String getS_ext_sequence() {
        return s_ext_sequence;
    }

    public void setS_ext_sequence(String s_ext_sequence) {
        this.s_ext_sequence = s_ext_sequence;
    }

    public String getAncheyear_tel() {
        return ancheyear_tel;
    }

    public void setAncheyear_tel(String ancheyear_tel) {
        this.ancheyear_tel = ancheyear_tel;
    }

    public String getAncheyear_email() {
        return ancheyear_email;
    }

    public void setAncheyear_email(String ancheyear_email) {
        this.ancheyear_email = ancheyear_email;
    }

    private String regorgcode;

    public String getRegorgcode() {
        return regorgcode;
    }

    public void setRegorgcode(String regorgcode) {
        this.regorgcode = regorgcode;
    }

    private String addr;

    private String ancheyear;

    public String getAddr() {
        return addr;
    }

    public void setAddr(String addr) {
        this.addr = addr;
    }

    public String getAncheyear() {
        return ancheyear;
    }

    public void setAncheyear(String ancheyear) {
        this.ancheyear = ancheyear;
    }

    private List<JSONObject> inv;
    public EntBaseInfo4ShangHai() {

    }

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public List<JSONObject> getInv() {
        return inv;
    }

    public void setInv(List<JSONObject> inv) {
        this.inv = inv;
    }

    public EntBaseInfo4ShangHai(String s_ext_nodenum, String pripid, String entname, String regno, String enttype,
                                String industryphy, String industryco, String opfrom, String opto, String postalcode,
                                String tel, String email, String esdate, String apprdate, String regorg, String entstatus,
                                String regcap, String opscope, String dom, String regcapcur, String entname_old,
                                String name, String candate, String revdate, String licid, String credit_code,
                                String tax_code, String oriregno,String addr,String ancheyear,String regorgcode,
                                String ancheyear_tel,String ancheyear_email,String s_ext_sequence,String entitytype) {
        this.s_ext_nodenum = s_ext_nodenum;
        this.pripid = pripid;
        this.entname = entname;
        this.regno = regno;
        this.enttype = enttype;
        this.industryphy = industryphy;
        this.industryco = industryco;
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
        this.dom = dom;
        this.regcapcur = regcapcur;
        this.entname_old = entname_old;
        this.name = name;
        this.candate = candate;
        this.revdate = revdate;
        this.licid = licid;
        this.credit_code = credit_code;
        this.tax_code = tax_code;
        this.oriregno = oriregno;
        this.exactentname = entname;
//        this.entname_smart = entname;
        this.addr=addr;
        this.ancheyear=ancheyear;
        this.regorgcode = regorgcode;
        this.ancheyear_tel = ancheyear_tel;
        this.ancheyear_email = ancheyear_email;
        this.s_ext_sequence=s_ext_sequence;
        this.entitytype=entitytype;
    }

//    public String getEntname_smart() {
//        return entname_smart;
//    }

//    public void setEntname_smart(String entname_smart) {
//        this.entname_smart = entname_smart;
//    }


    public String getEntitytype() {
        return entitytype;
    }

    public void setEntitytype(String entitytype) {
        this.entitytype = entitytype;
    }

    public String getExactentname() {
        return exactentname;
    }

    public void setExactentname(String exactentname) {
        this.exactentname = exactentname;
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

    public String getDom() {
        return dom;
    }

    public void setDom(String dom) {
        this.dom = dom;
    }

    public String getRegcapcur() {
        return regcapcur;
    }

    public void setRegcapcur(String regcapcur) {
        this.regcapcur = regcapcur;
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

    public String getOriregno() {
        return oriregno;
    }

    public void setOriregno(String oriregno) {
        this.oriregno = oriregno;
    }

    public static JavaRDD<EntBaseInfo4ShangHai> convertEntData(SparkSession sqlContext, Hdfs2EsETL4ShangHai hdfs) {


        //主体企业
        JavaPairRDD<String,EntBaseInfo4ShangHai> entda = getJavaRddEnt(hdfs.getEntDataFrame(sqlContext));
        JavaPairRDD<String, Iterable<JSONObject>> inv = getJavaRddEntInv(hdfs.getEntInvDf(sqlContext));


        JavaRDD<EntBaseInfo4ShangHai> entInv = entda.leftOuterJoin(inv).mapToPair(new PairFunction<Tuple2<String,
                Tuple2<EntBaseInfo4ShangHai, Optional<Iterable<JSONObject>>>>, String, EntBaseInfo4ShangHai>() {
            @Override
            public Tuple2<String, EntBaseInfo4ShangHai> call(Tuple2<String, Tuple2<EntBaseInfo4ShangHai,
                    Optional<Iterable<JSONObject>>>> stringTuple2Tuple2) throws Exception {
                EntBaseInfo4ShangHai entEO = stringTuple2Tuple2._2()._1();

                if(stringTuple2Tuple2._2()._2().isPresent()){
                    entEO.setInv(IteratorUtils.toList(stringTuple2Tuple2._2()._2().get().iterator()));
                }
                return new Tuple2(stringTuple2Tuple2._1,entEO);
            }
        }).map(new Function<Tuple2<String, EntBaseInfo4ShangHai>, EntBaseInfo4ShangHai>() {
            @Override
            public EntBaseInfo4ShangHai call(Tuple2<String, EntBaseInfo4ShangHai> stringEntBaseInfo4ShangHaiTuple2) throws Exception {
                return stringEntBaseInfo4ShangHaiTuple2._2();
            }
        });

        return entInv;
    }


    public static JavaPairRDD<String, EntBaseInfo4ShangHai> getJavaRddEnt(Dataset df){
        return df.toJavaRDD().mapToPair(new PairFunction<Row, String, EntBaseInfo4ShangHai>() {
            @Override
            public Tuple2<String, EntBaseInfo4ShangHai> call(Row row) throws Exception {
                return new Tuple2(row.getAs("pripid").toString(), new EntBaseInfo4ShangHai(row.getString(0),
                        row.getString(1), row.getString(2), row.getString(3), row.getString(4),
                        row.getString(5), row.getString(6), row.getString(7), row.getString(8),
                        row.getString(9), row.getString(10), row.getString(11), row.getString(12),
                        row.getString(13), row.getString(14), row.getString(15), row.getString(16),
                        row.getString(17), row.getString(18), row.getString(19), row.getString(20),
                        row.getString(21), row.getString(22), row.getString(23), row.getString(24),
                        row.getString(25), row.getString(26), row.getString(27),row.getString(28),
                        row.getString(29),row.getString(30),row.getString(31),row.getString(32),
                        row.getString(33),row.getString(34)));
            }
        });
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
    public static Map<String,String> converInvMap(Row row){

        Map<String,String> invMap = new HashMap<String,String>(10);
        invMap.put("s_ext_nodenum",row.getString(0));
        invMap.put("pripid",row.getString(1));
        invMap.put("inv",row.getString(2));
        invMap.put("invtype",row.getString(3));
        invMap.put("subconam",row.getString(4));
        invMap.put("condate",row.getString(5));
        invMap.put("currency",row.getString(6));
        invMap.put("s_ext_sequence",row.getString(7));
        return invMap;
    }




}
