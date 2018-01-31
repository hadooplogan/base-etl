package com.chinadaas.common.table;

import java.io.Serializable;

/**
 * @author haoxing
 *
 * 软件著作权信息登记表
 */
public class E_COPYRIGHTINFO implements Serializable{

    private String frj_djh;
    private String frj_rjflh;
    private String frj_hyflh;
    private String frj_rjqc;
    private String frj_rjjc;
    private String frj_bbh;
    private String frj_scfbdate;
    private String frj_djdate;
    private String frj_zzqr_gj;
    private String frj_idt;
    private String frj_udt;
    private String frj_status;
    private String frj_id;
    private String date_udt;
    private String date_idt;
    private String jobid;
    private String date_seq;


    public E_COPYRIGHTINFO(String person, String frj_djh, String frj_rjflh, String frj_hyflh, String frj_rjqc, String frj_rjjc, String frj_bbh, String frj_scfbdate, String frj_djdate, String frj_zzqr_gj, String frj_idt, String frj_udt, String frj_status, String frj_id, String date_udt, String date_idt, String jobid) {
        this.frj_djh = frj_djh;
        this.frj_rjflh = frj_rjflh;
        this.frj_hyflh = frj_hyflh;
        this.frj_rjqc = frj_rjqc;
        this.frj_rjjc = frj_rjjc;
        this.frj_bbh = frj_bbh;
        this.frj_scfbdate = frj_scfbdate;
        this.frj_djdate = frj_djdate;
        this.frj_zzqr_gj = frj_zzqr_gj;
        this.frj_idt = frj_idt;
        this.frj_udt = frj_udt;
        this.frj_status = frj_status;
        this.frj_id = frj_id;
        this.date_udt = date_udt;
        this.date_idt = date_idt;
        this.jobid = jobid;
        this.date_seq = date_seq;
    }

    public String getFrj_djh() {
        return frj_djh;
    }

    public void setFrj_djh(String frj_djh) {
        this.frj_djh = frj_djh;
    }

    public String getFrj_rjflh() {
        return frj_rjflh;
    }

    public void setFrj_rjflh(String frj_rjflh) {
        this.frj_rjflh = frj_rjflh;
    }

    public String getFrj_hyflh() {
        return frj_hyflh;
    }

    public void setFrj_hyflh(String frj_hyflh) {
        this.frj_hyflh = frj_hyflh;
    }

    public String getFrj_rjqc() {
        return frj_rjqc;
    }

    public void setFrj_rjqc(String frj_rjqc) {
        this.frj_rjqc = frj_rjqc;
    }

    public String getFrj_rjjc() {
        return frj_rjjc;
    }

    public void setFrj_rjjc(String frj_rjjc) {
        this.frj_rjjc = frj_rjjc;
    }

    public String getFrj_bbh() {
        return frj_bbh;
    }

    public void setFrj_bbh(String frj_bbh) {
        this.frj_bbh = frj_bbh;
    }

    public String getFrj_scfbdate() {
        return frj_scfbdate;
    }

    public void setFrj_scfbdate(String frj_scfbdate) {
        this.frj_scfbdate = frj_scfbdate;
    }

    public String getFrj_djdate() {
        return frj_djdate;
    }

    public void setFrj_djdate(String frj_djdate) {
        this.frj_djdate = frj_djdate;
    }

    public String getFrj_zzqr_gj() {
        return frj_zzqr_gj;
    }

    public void setFrj_zzqr_gj(String frj_zzqr_gj) {
        this.frj_zzqr_gj = frj_zzqr_gj;
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


}
