package com.chinadaas.common.table;

import java.io.Serializable;

/**
 * @author haoxing
 * 年报表
 */
public class E_ANNREPORT_BASEINFO implements Serializable{
  private String  ancheid       ;
    private String  pripid        ;
    private String  anchedate     ;
    private String  ancheyear     ;
    private String  regno         ;
    private String  creditno      ;
    private String  entname       ;
    private String  tel           ;
    private String  addr          ;
    private String  postalcode    ;
    private String  email         ;
    private String  busst         ;
    private String  empnum        ;
    private String  s_ext_nodenum ;
    private String  udt           ;
    private String  idt           ;
    private String  id            ;

    public E_ANNREPORT_BASEINFO(String ancheid, String pripid, String anchedate, String ancheyear, String regno, String creditno, String entname, String tel, String addr, String postalcode, String email, String busst, String empnum, String s_ext_nodenum, String udt, String idt, String id) {
        this.ancheid = ancheid;
        this.pripid = pripid;
        this.anchedate = anchedate;
        this.ancheyear = ancheyear;
        this.regno = regno;
        this.creditno = creditno;
        this.entname = entname;
        this.tel = tel;
        this.addr = addr;
        this.postalcode = postalcode;
        this.email = email;
        this.busst = busst;
        this.empnum = empnum;
        this.s_ext_nodenum = s_ext_nodenum;
        this.udt = udt;
        this.idt = idt;
        this.id = id;
    }


    public String getAncheid() {
        return ancheid;
    }

    public void setAncheid(String ancheid) {
        this.ancheid = ancheid;
    }

    public String getPripid() {
        return pripid;
    }

    public void setPripid(String pripid) {
        this.pripid = pripid;
    }

    public String getAnchedate() {
        return anchedate;
    }

    public void setAnchedate(String anchedate) {
        this.anchedate = anchedate;
    }

    public String getAncheyear() {
        return ancheyear;
    }

    public void setAncheyear(String ancheyear) {
        this.ancheyear = ancheyear;
    }

    public String getRegno() {
        return regno;
    }

    public void setRegno(String regno) {
        this.regno = regno;
    }

    public String getCreditno() {
        return creditno;
    }

    public void setCreditno(String creditno) {
        this.creditno = creditno;
    }

    public String getEntname() {
        return entname;
    }

    public void setEntname(String entname) {
        this.entname = entname;
    }

    public String getTel() {
        return tel;
    }

    public void setTel(String tel) {
        this.tel = tel;
    }

    public String getAddr() {
        return addr;
    }

    public void setAddr(String addr) {
        this.addr = addr;
    }

    public String getPostalcode() {
        return postalcode;
    }

    public void setPostalcode(String postalcode) {
        this.postalcode = postalcode;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getBusst() {
        return busst;
    }

    public void setBusst(String busst) {
        this.busst = busst;
    }

    public String getEmpnum() {
        return empnum;
    }

    public void setEmpnum(String empnum) {
        this.empnum = empnum;
    }

    public String getS_ext_nodenum() {
        return s_ext_nodenum;
    }

    public void setS_ext_nodenum(String s_ext_nodenum) {
        this.s_ext_nodenum = s_ext_nodenum;
    }

    public String getUdt() {
        return udt;
    }

    public void setUdt(String udt) {
        this.udt = udt;
    }

    public String getIdt() {
        return idt;
    }

    public void setIdt(String idt) {
        this.idt = idt;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}
