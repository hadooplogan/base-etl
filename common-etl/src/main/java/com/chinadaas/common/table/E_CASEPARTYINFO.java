package com.chinadaas.common.table;

import java.io.Serializable;

/**
 * @author haoxing
 */ //企业行政处罚案件基本信息表
public class E_CASEPARTYINFO implements Serializable{
  private String caseid     ;         	
  private String unitname   ;         	
  private String credit_code;         	
  private String lerep      ;         	
  private String pripid     ;         	
  private String date_udt   ;         	
  private String date_idt   ;         	
  private String jobid      ;         	
  private String date_seq   ;



    public E_CASEPARTYINFO(String caseid, String unitname, String credit_code, String lerep, String pripid, String date_udt, String date_idt, String jobid, String date_seq) {
        this.caseid = caseid;
        this.unitname = unitname;
        this.credit_code = credit_code;
        this.lerep = lerep;
        this.pripid = pripid;
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

    public String getUnitname() {
        return unitname;
    }

    public void setUnitname(String unitname) {
        this.unitname = unitname;
    }

    public String getCredit_code() {
        return credit_code;
    }

    public void setCredit_code(String credit_code) {
        this.credit_code = credit_code;
    }

    public String getLerep() {
        return lerep;
    }

    public void setLerep(String lerep) {
        this.lerep = lerep;
    }

    public String getPripid() {
        return pripid;
    }

    public void setPripid(String pripid) {
        this.pripid = pripid;
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
