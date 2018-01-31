package com.chinadaas.common.table;

import java.io.Serializable;

/**
 * @author haoxing
 * 软件著作权登记人信息表
 */
public class E_COPYRIGHTORG implements Serializable{
   private String frj_djh;
   private String frj_zzqr;
   private String frj_gj;
   private String frj_authortype;
   private String frj_order;
   private String frj_idt;
   private String frj_udt;
   private String frj_status;
   private String frj_id;
   private String date_udt;
   private String date_idt;
   private String jobid;
   private String date_seq;
   private String pripid;
   private String person_id;


    public E_COPYRIGHTORG(String frj_djh, String frj_zzqr, String frj_gj, String frj_authortype, String frj_order, String frj_idt, String frj_udt, String frj_status, String frj_id, String date_udt, String date_idt, String jobid, String date_seq, String pripid, String person_id) {
        this.frj_djh = frj_djh;
        this.frj_zzqr = frj_zzqr;
        this.frj_gj = frj_gj;
        this.frj_authortype = frj_authortype;
        this.frj_order = frj_order;
        this.frj_idt = frj_idt;
        this.frj_udt = frj_udt;
        this.frj_status = frj_status;
        this.frj_id = frj_id;
        this.date_udt = date_udt;
        this.date_idt = date_idt;
        this.jobid = jobid;
        this.date_seq = date_seq;
        this.pripid = pripid;
        this.person_id = person_id;

    }

    public String getFrj_djh() {
        return frj_djh;
    }

    public void setFrj_djh(String frj_djh) {
        this.frj_djh = frj_djh;
    }

    public String getFrj_zzqr() {
        return frj_zzqr;
    }

    public void setFrj_zzqr(String frj_zzqr) {
        this.frj_zzqr = frj_zzqr;
    }

    public String getFrj_gj() {
        return frj_gj;
    }

    public void setFrj_gj(String frj_gj) {
        this.frj_gj = frj_gj;
    }

    public String getFrj_authortype() {
        return frj_authortype;
    }

    public void setFrj_authortype(String frj_authortype) {
        this.frj_authortype = frj_authortype;
    }

    public String getFrj_order() {
        return frj_order;
    }

    public void setFrj_order(String frj_order) {
        this.frj_order = frj_order;
    }

    public String getFrj_idt() {
        return frj_idt;
    }

    public void setFrj_idt(String frj_idt) {
        this.frj_idt = frj_idt;
    }

    public String getFrj_udt() {
        return frj_udt;
    }

    public void setFrj_udt(String frj_udt) {
        this.frj_udt = frj_udt;
    }

    public String getFrj_status() {
        return frj_status;
    }

    public void setFrj_status(String frj_status) {
        this.frj_status = frj_status;
    }

    public String getFrj_id() {
        return frj_id;
    }

    public void setFrj_id(String frj_id) {
        this.frj_id = frj_id;
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

    public String getPripid() {
        return pripid;
    }

    public void setPripid(String pripid) {
        this.pripid = pripid;
    }

    public String getPerson_id() {
        return person_id;
    }

    public void setPerson_id(String person_id) {
        this.person_id = person_id;
    }


}
