package com.chinadaas.common.table;

import java.io.Serializable;

/**
 * @author haoxing
 */
public class S_SIPO_PATENT_COPYRIGHT implements Serializable{
   private String pt_patentright_id   ;
   private String pt_reg_no           ;
   private String pt_typeno           ;
   private String pt_reg_per          ;
   private String pt_reg_pripid       ;
   private String pt_reg_group        ;
   private String pt_reg_group_order  ;
   private String pt_reg_num          ;
   private String pt_reg_pertype      ;
   private String rc1                 ;
   private String rc2                 ;
   private String pt_reg_gl_pertype   ;
   private String data_status         ;
   private String jobid               ;
   private String idt                 ;
   private String udt                 ;
   private String date_seq            ;


    public S_SIPO_PATENT_COPYRIGHT(String pt_patentright_id, String pt_reg_no, String pt_typeno, String pt_reg_per, String pt_reg_pripid, String pt_reg_group, String pt_reg_group_order, String pt_reg_num, String pt_reg_pertype, String rc1, String rc2, String pt_reg_gl_pertype, String data_status, String jobid, String idt, String udt, String date_seq) {
        this.pt_patentright_id = pt_patentright_id;
        this.pt_reg_no = pt_reg_no;
        this.pt_typeno = pt_typeno;
        this.pt_reg_per = pt_reg_per;
        this.pt_reg_pripid = pt_reg_pripid;
        this.pt_reg_group = pt_reg_group;
        this.pt_reg_group_order = pt_reg_group_order;
        this.pt_reg_num = pt_reg_num;
        this.pt_reg_pertype = pt_reg_pertype;
        this.rc1 = rc1;
        this.rc2 = rc2;
        this.pt_reg_gl_pertype = pt_reg_gl_pertype;
        this.data_status = data_status;
        this.jobid = jobid;
        this.idt = idt;
        this.udt = udt;
        this.date_seq = date_seq;

    }




    public String getPt_patentright_id() {
        return pt_patentright_id;
    }

    public void setPt_patentright_id(String pt_patentright_id) {
        this.pt_patentright_id = pt_patentright_id;
    }

    public String getPt_reg_no() {
        return pt_reg_no;
    }

    public void setPt_reg_no(String pt_reg_no) {
        this.pt_reg_no = pt_reg_no;
    }

    public String getPt_typeno() {
        return pt_typeno;
    }

    public void setPt_typeno(String pt_typeno) {
        this.pt_typeno = pt_typeno;
    }

    public String getPt_reg_per() {
        return pt_reg_per;
    }

    public void setPt_reg_per(String pt_reg_per) {
        this.pt_reg_per = pt_reg_per;
    }

    public String getPt_reg_pripid() {
        return pt_reg_pripid;
    }

    public void setPt_reg_pripid(String pt_reg_pripid) {
        this.pt_reg_pripid = pt_reg_pripid;
    }

    public String getPt_reg_group() {
        return pt_reg_group;
    }

    public void setPt_reg_group(String pt_reg_group) {
        this.pt_reg_group = pt_reg_group;
    }

    public String getPt_reg_group_order() {
        return pt_reg_group_order;
    }

    public void setPt_reg_group_order(String pt_reg_group_order) {
        this.pt_reg_group_order = pt_reg_group_order;
    }

    public String getPt_reg_num() {
        return pt_reg_num;
    }

    public void setPt_reg_num(String pt_reg_num) {
        this.pt_reg_num = pt_reg_num;
    }

    public String getPt_reg_pertype() {
        return pt_reg_pertype;
    }

    public void setPt_reg_pertype(String pt_reg_pertype) {
        this.pt_reg_pertype = pt_reg_pertype;
    }

    public String getRc1() {
        return rc1;
    }

    public void setRc1(String rc1) {
        this.rc1 = rc1;
    }

    public String getRc2() {
        return rc2;
    }

    public void setRc2(String rc2) {
        this.rc2 = rc2;
    }

    public String getPt_reg_gl_pertype() {
        return pt_reg_gl_pertype;
    }

    public void setPt_reg_gl_pertype(String pt_reg_gl_pertype) {
        this.pt_reg_gl_pertype = pt_reg_gl_pertype;
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
