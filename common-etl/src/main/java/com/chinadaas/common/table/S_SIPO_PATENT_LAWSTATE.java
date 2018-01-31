package com.chinadaas.common.table;

import java.io.Serializable;

/**
 * @author haoxing
 * 专利法律信息表
 */
public class S_SIPO_PATENT_LAWSTATE implements Serializable {
   private String pt_lawstate_id ;
   private String addiche_no     ;
   private String pt_reg_no      ;
   private String pt_issue_date  ;
   private String lawstate_date  ;
   private String lawstate       ;
   private String check_no       ;
   private String lawstate_msg   ;
   private String data_status    ;
   private String jobid          ;
   private String idt            ;
   private String udt            ;
   private String date_seq       ;


    public S_SIPO_PATENT_LAWSTATE(String pt_lawstate_id, String addiche_no, String pt_reg_no, String pt_issue_date, String lawstate_date, String lawstate, String check_no, String lawstate_msg, String data_status, String jobid, String idt, String udt, String date_seq) {
        this.pt_lawstate_id = pt_lawstate_id;
        this.addiche_no = addiche_no;
        this.pt_reg_no = pt_reg_no;
        this.pt_issue_date = pt_issue_date;
        this.lawstate_date = lawstate_date;
        this.lawstate = lawstate;
        this.check_no = check_no;
        this.lawstate_msg = lawstate_msg;
        this.data_status = data_status;
        this.jobid = jobid;
        this.idt = idt;
        this.udt = udt;
        this.date_seq = date_seq;
    }

    public String getPt_lawstate_id() {
        return pt_lawstate_id;
    }

    public void setPt_lawstate_id(String pt_lawstate_id) {
        this.pt_lawstate_id = pt_lawstate_id;
    }

    public String getAddiche_no() {
        return addiche_no;
    }

    public void setAddiche_no(String addiche_no) {
        this.addiche_no = addiche_no;
    }

    public String getPt_reg_no() {
        return pt_reg_no;
    }

    public void setPt_reg_no(String pt_reg_no) {
        this.pt_reg_no = pt_reg_no;
    }

    public String getPt_issue_date() {
        return pt_issue_date;
    }

    public void setPt_issue_date(String pt_issue_date) {
        this.pt_issue_date = pt_issue_date;
    }

    public String getLawstate_date() {
        return lawstate_date;
    }

    public void setLawstate_date(String lawstate_date) {
        this.lawstate_date = lawstate_date;
    }

    public String getLawstate() {
        return lawstate;
    }

    public void setLawstate(String lawstate) {
        this.lawstate = lawstate;
    }

    public String getCheck_no() {
        return check_no;
    }

    public void setCheck_no(String check_no) {
        this.check_no = check_no;
    }

    public String getLawstate_msg() {
        return lawstate_msg;
    }

    public void setLawstate_msg(String lawstate_msg) {
        this.lawstate_msg = lawstate_msg;
    }

    public String getData_status() {
        return data_status;
    }

    public void setData_status(String data_status) {
        this.data_status = data_status;
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

    public String getDate_seq() {
        return date_seq;
    }

    public void setDate_seq(String date_seq) {
        this.date_seq = date_seq;
    }


}
