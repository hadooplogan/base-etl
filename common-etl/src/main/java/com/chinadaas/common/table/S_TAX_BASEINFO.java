package com.chinadaas.common.table;

import java.io.Serializable;

public class S_TAX_BASEINFO implements Serializable{

   private String pripid          ;
   private String tax_name        ;
   private String tax_archives_no ;
   private String tax_regno       ;
   private String tax_tel         ;
   private String tax_addr        ;
   private String jobid           ;
   private String idt             ;
   private String udt             ;

    public S_TAX_BASEINFO(String pripid, String tax_name, String tax_archives_no, String tax_regno, String tax_tel, String tax_addr, String jobid, String idt, String udt) {
        this.pripid = pripid;
        this.tax_name = tax_name;
        this.tax_archives_no = tax_archives_no;
        this.tax_regno = tax_regno;
        this.tax_tel = tax_tel;
        this.tax_addr = tax_addr;
        this.jobid = jobid;
        this.idt = idt;
        this.udt = udt;
    }

    public String getPripid() {
        return pripid;
    }

    public void setPripid(String pripid) {
        this.pripid = pripid;
    }

    public String getTax_name() {
        return tax_name;
    }

    public void setTax_name(String tax_name) {
        this.tax_name = tax_name;
    }

    public String getTax_archives_no() {
        return tax_archives_no;
    }

    public void setTax_archives_no(String tax_archives_no) {
        this.tax_archives_no = tax_archives_no;
    }

    public String getTax_regno() {
        return tax_regno;
    }

    public void setTax_regno(String tax_regno) {
        this.tax_regno = tax_regno;
    }

    public String getTax_tel() {
        return tax_tel;
    }

    public void setTax_tel(String tax_tel) {
        this.tax_tel = tax_tel;
    }

    public String getTax_addr() {
        return tax_addr;
    }

    public void setTax_addr(String tax_addr) {
        this.tax_addr = tax_addr;
    }

    public String getJobid() {
        return jobid;
    }

    public void setJobid(String jobid) {
        this.jobid = jobid;
    }

    public String getIdt() {
        return idt;
    }

    public void setIdt(String idt) {
        this.idt = idt;
    }

    public String getUdt() {
        return udt;
    }

    public void setUdt(String udt) {
        this.udt = udt;
    }
}
