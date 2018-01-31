package com.chinadaas.common.table;

import java.io.Serializable;

/**
 * @author haoxing
 *企业行政处罚案件基本信息表
 */


public class E_CASEBASEINFO implements Serializable {


   private String caseid          ;
   private String pendecno        ;
   private String illegacttype    ;
   private String pentype         ;
   private String pentype_cn      ;
   private String pencontent      ;
   private String penauth         ;
   private String penauth_cn      ;
   private String pendecissdate   ;
   private String publicdate      ;
   private String date_udt        ;
   private String date_idt        ;
   private String jobid           ;
   private String date_seq        ;



    public E_CASEBASEINFO(String caseid, String pendecno, String illegacttype, String pentype, String pentype_cn, String pencontent, String penauth, String penauth_cn, String pendecissdate, String publicdate, String date_udt, String date_idt, String jobid, String date_seq) {
        this.caseid = caseid;
        this.pendecno = pendecno;
        this.illegacttype = illegacttype;
        this.pentype = pentype;
        this.pentype_cn = pentype_cn;
        this.pencontent = pencontent;
        this.penauth = penauth;
        this.penauth_cn = penauth_cn;
        this.pendecissdate = pendecissdate;
        this.publicdate = publicdate;
        this.date_udt = date_udt;
        this.date_idt = date_idt;
        this.jobid = jobid;
        this.date_seq = date_seq;

    }



    public String getCaseid() {
        return caseid;
    }

    public void setCaseid(String caseid) {
        this.caseid = caseid;
    }

    public String getPendecno() {
        return pendecno;
    }

    public void setPendecno(String pendecno) {
        this.pendecno = pendecno;
    }

    public String getIllegacttype() {
        return illegacttype;
    }

    public void setIllegacttype(String illegacttype) {
        this.illegacttype = illegacttype;
    }

    public String getPentype() {
        return pentype;
    }

    public void setPentype(String pentype) {
        this.pentype = pentype;
    }

    public String getPentype_cn() {
        return pentype_cn;
    }

    public void setPentype_cn(String pentype_cn) {
        this.pentype_cn = pentype_cn;
    }

    public String getPencontent() {
        return pencontent;
    }

    public void setPencontent(String pencontent) {
        this.pencontent = pencontent;
    }

    public String getPenauth() {
        return penauth;
    }

    public void setPenauth(String penauth) {
        this.penauth = penauth;
    }

    public String getPenauth_cn() {
        return penauth_cn;
    }

    public void setPenauth_cn(String penauth_cn) {
        this.penauth_cn = penauth_cn;
    }

    public String getPendecissdate() {
        return pendecissdate;
    }

    public void setPendecissdate(String pendecissdate) {
        this.pendecissdate = pendecissdate;
    }

    public String getPublicdate() {
        return publicdate;
    }

    public void setPublicdate(String publicdate) {
        this.publicdate = publicdate;
    }

    public String getDate_udt() {
        return date_udt;
    }

    public void setDate_udt(String date_udt) {
        this.date_udt = date_udt;
    }

    public String getDate_idt() {
        return date_idt;
    }

    public void setDate_idt(String date_idt) {
        this.date_idt = date_idt;
    }

    public String getJobid() {
        return jobid;
    }

    public void setJobid(String jobid) {
        this.jobid = jobid;
    }

    public String getDate_seq() {
        return date_seq;
    }

    public void setDate_seq(String date_seq) {
        this.date_seq = date_seq;
    }


}
