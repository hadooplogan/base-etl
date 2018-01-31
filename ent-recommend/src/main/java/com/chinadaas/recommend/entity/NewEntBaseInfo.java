package com.chinadaas.recommend.entity;

import java.io.Serializable;

/**
 * Created by hejianning on 2017/10/16.
 * 新企(NEW_ENT_BASEINFO表)实体类
 */
public class NewEntBaseInfo implements Serializable{
    private static final long serialVersionUID = 1025026912977591750L;

    private String pripid;
    private String s_ext_nodenum;
    private String entname;
    private String regno;
    private String enttype;
    private String industryphy;
    private String industryco;
    private String abuitem;
    private String cbuitem;
    private String opfrom;
    private String opto;
    private String postalcode;
    private String tel;
    private String email;
    private String credlevel;
    private String esdate;
    private String apprdate;
    private String regorg;
    private String entstatus;
    private String regcap;
    private String opscope;
    private String domdistrict;
    private String dom;
    private String empnum;
    private String regcapcur;
    private String country;
    private String s_ext_timestamp;
    private String s_ext_sequence;
    private String person_id;
    private String name;
    private String certype;
    private String cerno;
    private String candate;
    private String revdate;
    private String entname_old;
    private String credit_code;
    private String jobid;
    private String is_new;
    private String countrydisplay;
    private String statusdisplay;
    private String typedisplay;
    private String regorgdisplay;
    private String regcapcurdisplay;
    private String tax_code;
    private String licid;
    private String zspid;
    private String data_date;
    private String verify_flag;
    private String altdate;
    private String inv_nums;
    private String person_nums;
    private String hk_value;

    public NewEntBaseInfo() {
    }

    public NewEntBaseInfo(String pripid) {
        this.pripid = pripid;
    }

    public NewEntBaseInfo(String pripid, String s_ext_nodenum, String entname, String regno, String enttype, String industryphy, String industryco, String abuitem, String cbuitem, String opfrom, String opto, String postalcode, String tel, String email, String credlevel, String esdate, String apprdate, String regorg, String entstatus, String regcap, String opscope, String domdistrict, String dom, String empnum, String regcapcur, String country, String s_ext_timestamp, String s_ext_sequence, String person_id, String name, String certype, String cerno, String candate, String revdate, String entname_old, String credit_code, String jobid, String is_new, String countrydisplay, String statusdisplay, String typedisplay, String regorgdisplay, String regcapcurdisplay, String tax_code, String licid, String zspid, String data_date, String verify_flag, String altdate, String inv_nums, String person_nums, String hk_value) {
        this.pripid = pripid;
        this.s_ext_nodenum = s_ext_nodenum;
        this.entname = entname;
        this.regno = regno;
        this.enttype = enttype;
        this.industryphy = industryphy;
        this.industryco = industryco;
        this.abuitem = abuitem;
        this.cbuitem = cbuitem;
        this.opfrom = opfrom;
        this.opto = opto;
        this.postalcode = postalcode;
        this.tel = tel;
        this.email = email;
        this.credlevel = credlevel;
        this.esdate = esdate;
        this.apprdate = apprdate;
        this.regorg = regorg;
        this.entstatus = entstatus;
        this.regcap = regcap;
        this.opscope = opscope;
        this.domdistrict = domdistrict;
        this.dom = dom;
        this.empnum = empnum;
        this.regcapcur = regcapcur;
        this.country = country;
        this.s_ext_timestamp = s_ext_timestamp;
        this.s_ext_sequence = s_ext_sequence;
        this.person_id = person_id;
        this.name = name;
        this.certype = certype;
        this.cerno = cerno;
        this.candate = candate;
        this.revdate = revdate;
        this.entname_old = entname_old;
        this.credit_code = credit_code;
        this.jobid = jobid;
        this.is_new = is_new;
        this.countrydisplay = countrydisplay;
        this.statusdisplay = statusdisplay;
        this.typedisplay = typedisplay;
        this.regorgdisplay = regorgdisplay;
        this.regcapcurdisplay = regcapcurdisplay;
        this.tax_code = tax_code;
        this.licid = licid;
        this.zspid = zspid;
        this.data_date = data_date;
        this.verify_flag = verify_flag;
        this.altdate = altdate;
        this.inv_nums = inv_nums;
        this.person_nums = person_nums;
        this.hk_value = hk_value;
    }

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public String getPripid() {
        return pripid;
    }

    public void setPripid(String pripid) {
        this.pripid = pripid;
    }

    public String getS_ext_nodenum() {
        return s_ext_nodenum;
    }

    public void setS_ext_nodenum(String s_ext_nodenum) {
        this.s_ext_nodenum = s_ext_nodenum;
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

    public String getCbuitem() {
        return cbuitem;
    }

    public void setCbuitem(String cbuitem) {
        this.cbuitem = cbuitem;
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

    public String getCredlevel() {
        return credlevel;
    }

    public void setCredlevel(String credlevel) {
        this.credlevel = credlevel;
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

    public String getDomdistrict() {
        return domdistrict;
    }

    public void setDomdistrict(String domdistrict) {
        this.domdistrict = domdistrict;
    }

    public String getDom() {
        return dom;
    }

    public void setDom(String dom) {
        this.dom = dom;
    }

    public String getEmpnum() {
        return empnum;
    }

    public void setEmpnum(String empnum) {
        this.empnum = empnum;
    }

    public String getRegcapcur() {
        return regcapcur;
    }

    public void setRegcapcur(String regcapcur) {
        this.regcapcur = regcapcur;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getS_ext_timestamp() {
        return s_ext_timestamp;
    }

    public void setS_ext_timestamp(String s_ext_timestamp) {
        this.s_ext_timestamp = s_ext_timestamp;
    }

    public String getS_ext_sequence() {
        return s_ext_sequence;
    }

    public void setS_ext_sequence(String s_ext_sequence) {
        this.s_ext_sequence = s_ext_sequence;
    }

    public String getPerson_id() {
        return person_id;
    }

    public void setPerson_id(String person_id) {
        this.person_id = person_id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCertype() {
        return certype;
    }

    public void setCertype(String certype) {
        this.certype = certype;
    }

    public String getCerno() {
        return cerno;
    }

    public void setCerno(String cerno) {
        this.cerno = cerno;
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

    public String getEntname_old() {
        return entname_old;
    }

    public void setEntname_old(String entname_old) {
        this.entname_old = entname_old;
    }

    public String getCredit_code() {
        return credit_code;
    }

    public void setCredit_code(String credit_code) {
        this.credit_code = credit_code;
    }

    public String getJobid() {
        return jobid;
    }

    public void setJobid(String jobid) {
        this.jobid = jobid;
    }

    public String getIs_new() {
        return is_new;
    }

    public void setIs_new(String is_new) {
        this.is_new = is_new;
    }

    public String getCountrydisplay() {
        return countrydisplay;
    }

    public void setCountrydisplay(String countrydisplay) {
        this.countrydisplay = countrydisplay;
    }

    public String getStatusdisplay() {
        return statusdisplay;
    }

    public void setStatusdisplay(String statusdisplay) {
        this.statusdisplay = statusdisplay;
    }

    public String getTypedisplay() {
        return typedisplay;
    }

    public void setTypedisplay(String typedisplay) {
        this.typedisplay = typedisplay;
    }

    public String getRegorgdisplay() {
        return regorgdisplay;
    }

    public void setRegorgdisplay(String regorgdisplay) {
        this.regorgdisplay = regorgdisplay;
    }

    public String getRegcapcurdisplay() {
        return regcapcurdisplay;
    }

    public void setRegcapcurdisplay(String regcapcurdisplay) {
        this.regcapcurdisplay = regcapcurdisplay;
    }

    public String getTax_code() {
        return tax_code;
    }

    public void setTax_code(String tax_code) {
        this.tax_code = tax_code;
    }

    public String getLicid() {
        return licid;
    }

    public void setLicid(String licid) {
        this.licid = licid;
    }

    public String getZspid() {
        return zspid;
    }

    public void setZspid(String zspid) {
        this.zspid = zspid;
    }

    public String getData_date() {
        return data_date;
    }

    public void setData_date(String data_date) {
        this.data_date = data_date;
    }

    public String getVerify_flag() {
        return verify_flag;
    }

    public void setVerify_flag(String verify_flag) {
        this.verify_flag = verify_flag;
    }

    public String getAltdate() {
        return altdate;
    }

    public void setAltdate(String altdate) {
        this.altdate = altdate;
    }

    public String getInv_nums() {
        return inv_nums;
    }

    public void setInv_nums(String inv_nums) {
        this.inv_nums = inv_nums;
    }

    public String getPerson_nums() {
        return person_nums;
    }

    public void setPerson_nums(String person_nums) {
        this.person_nums = person_nums;
    }

    public String getHk_value() {
        return hk_value;
    }

    public void setHk_value(String hk_value) {
        this.hk_value = hk_value;
    }

    @Override
    public String toString() {
        return "NewEntBaseInfo{" +
                "pripid='" + pripid + '\'' +
                ", s_ext_nodenum='" + s_ext_nodenum + '\'' +
                ", entname='" + entname + '\'' +
                ", regno='" + regno + '\'' +
                ", enttype='" + enttype + '\'' +
                ", industryphy='" + industryphy + '\'' +
                ", industryco='" + industryco + '\'' +
                ", abuitem='" + abuitem + '\'' +
                ", cbuitem='" + cbuitem + '\'' +
                ", opfrom='" + opfrom + '\'' +
                ", opto='" + opto + '\'' +
                ", postalcode='" + postalcode + '\'' +
                ", tel='" + tel + '\'' +
                ", email='" + email + '\'' +
                ", credlevel='" + credlevel + '\'' +
                ", esdate='" + esdate + '\'' +
                ", apprdate='" + apprdate + '\'' +
                ", regorg='" + regorg + '\'' +
                ", entstatus='" + entstatus + '\'' +
                ", regcap='" + regcap + '\'' +
                ", opscope='" + opscope + '\'' +
                ", domdistrict='" + domdistrict + '\'' +
                ", dom='" + dom + '\'' +
                ", empnum='" + empnum + '\'' +
                ", regcapcur='" + regcapcur + '\'' +
                ", country='" + country + '\'' +
                ", s_ext_timestamp='" + s_ext_timestamp + '\'' +
                ", s_ext_sequence='" + s_ext_sequence + '\'' +
                ", person_id='" + person_id + '\'' +
                ", name='" + name + '\'' +
                ", certype='" + certype + '\'' +
                ", cerno='" + cerno + '\'' +
                ", candate='" + candate + '\'' +
                ", revdate='" + revdate + '\'' +
                ", entname_old='" + entname_old + '\'' +
                ", credit_code='" + credit_code + '\'' +
                ", jobid='" + jobid + '\'' +
                ", is_new='" + is_new + '\'' +
                ", countrydisplay='" + countrydisplay + '\'' +
                ", statusdisplay='" + statusdisplay + '\'' +
                ", typedisplay='" + typedisplay + '\'' +
                ", regorgdisplay='" + regorgdisplay + '\'' +
                ", regcapcurdisplay='" + regcapcurdisplay + '\'' +
                ", tax_code='" + tax_code + '\'' +
                ", licid='" + licid + '\'' +
                ", zspid='" + zspid + '\'' +
                ", data_date='" + data_date + '\'' +
                ", verify_flag='" + verify_flag + '\'' +
                ", altdate='" + altdate + '\'' +
                ", inv_nums='" + inv_nums + '\'' +
                ", person_nums='" + person_nums + '\'' +
                ", hk_value='" + hk_value + '\'' +
                '}';
    }
}
