package com.chinadaas.common.table;

import java.io.Serializable;

/**
 * @author haoxing
 * 专利信息主表
 */
public class S_SIPO_PATENT_INFO implements Serializable {
   private String pt_pub_code         	;
   private String pt_date             	;
   private String pt_reg_no           	;
   private String pt_reg_date         	;
   private String pt_code             	;
   private String pt_name             	;
   private String pt_sub_code         	;
   private String pt_type_code        	;
   private String pt_reg_per          	;
   private String pt_invent_per       	;
   private String pt_summary          	;
   private String pt_sov_itm          	;
   private String pt_por_itm          	;
   private String pt_sml_itm          	;
   private String pt_code_num         	;
   private String pt_country          	;
   private String division_regno      	;
   private String pt_addr             	;
   private String pt_zipcode          	;
   private String pt_agent_org        	;
   private String pt_agent_pripid     	;
   private String pt_agent            	;
   private String pt_checkup_per      	;
   private String pt_issue_date       	;
   private String pt_nal_reg          	;
   private String pt_nal_pub          	;
   private String pt_innation_date    	;
   private String pt_summary_url      	;
   private String pt_europe_sub_typeno	;
   private String pt_europe_typeno    	;
   private String pt_nat_sub_typeno   	;
   private String pt_nat_typeno       	;
   private String pt_pub_url          	;
   private String pt_pagenum          	;
   private String pt_reg_natcode      	;
   private String pt_typeno_old       	;
   private String pt_typeno           	;
   private String pt_reg_source       	;
   private String pt_ref_doc          	;
   private String pt_scope_type       	;
   private String pt_cp_doc           	;
   private String pt_spe              	;
   private String pt_spe_img          	;
   private String pt_cp_doc_pn        	;
   private String pt_spe_pn           	;
   private String pt_spe_img_pn       	;
   private String data_status         	;
   private String jobid               	;
   private String idt                 	;
   private String udt                 	;
   private String date_seq            	;


    public S_SIPO_PATENT_INFO(String pt_pub_code, String pt_date, String pt_reg_no, String pt_reg_date, String pt_code, String pt_name, String pt_sub_code, String pt_type_code, String pt_reg_per, String pt_invent_per, String pt_summary, String pt_sov_itm, String pt_por_itm, String pt_sml_itm, String pt_code_num, String pt_country, String division_regno, String pt_addr, String pt_zipcode, String pt_agent_org, String pt_agent_pripid, String pt_agent, String pt_checkup_per, String pt_issue_date, String pt_nal_reg, String pt_nal_pub, String pt_innation_date, String pt_summary_url, String pt_europe_sub_typeno, String pt_europe_typeno, String pt_nat_sub_typeno, String pt_nat_typeno, String pt_pub_url, String pt_pagenum, String pt_reg_natcode, String pt_typeno_old, String pt_typeno, String pt_reg_source, String pt_ref_doc, String pt_scope_type, String pt_cp_doc, String pt_spe, String pt_spe_img, String pt_cp_doc_pn, String pt_spe_pn, String pt_spe_img_pn, String data_status, String jobid, String idt, String udt, String date_seq) {
        this.pt_pub_code = pt_pub_code;
        this.pt_date = pt_date;
        this.pt_reg_no = pt_reg_no;
        this.pt_reg_date = pt_reg_date;
        this.pt_code = pt_code;
        this.pt_name = pt_name;
        this.pt_sub_code = pt_sub_code;
        this.pt_type_code = pt_type_code;
        this.pt_reg_per = pt_reg_per;
        this.pt_invent_per = pt_invent_per;
        this.pt_summary = pt_summary;
        this.pt_sov_itm = pt_sov_itm;
        this.pt_por_itm = pt_por_itm;
        this.pt_sml_itm = pt_sml_itm;
        this.pt_code_num = pt_code_num;
        this.pt_country = pt_country;
        this.division_regno = division_regno;
        this.pt_addr = pt_addr;
        this.pt_zipcode = pt_zipcode;
        this.pt_agent_org = pt_agent_org;
        this.pt_agent_pripid = pt_agent_pripid;
        this.pt_agent = pt_agent;
        this.pt_checkup_per = pt_checkup_per;
        this.pt_issue_date = pt_issue_date;
        this.pt_nal_reg = pt_nal_reg;
        this.pt_nal_pub = pt_nal_pub;
        this.pt_innation_date = pt_innation_date;
        this.pt_summary_url = pt_summary_url;
        this.pt_europe_sub_typeno = pt_europe_sub_typeno;
        this.pt_europe_typeno = pt_europe_typeno;
        this.pt_nat_sub_typeno = pt_nat_sub_typeno;
        this.pt_nat_typeno = pt_nat_typeno;
        this.pt_pub_url = pt_pub_url;
        this.pt_pagenum = pt_pagenum;
        this.pt_reg_natcode = pt_reg_natcode;
        this.pt_typeno_old = pt_typeno_old;
        this.pt_typeno = pt_typeno;
        this.pt_reg_source = pt_reg_source;
        this.pt_ref_doc = pt_ref_doc;
        this.pt_scope_type = pt_scope_type;
        this.pt_cp_doc = pt_cp_doc;
        this.pt_spe = pt_spe;
        this.pt_spe_img = pt_spe_img;
        this.pt_cp_doc_pn = pt_cp_doc_pn;
        this.pt_spe_pn = pt_spe_pn;
        this.pt_spe_img_pn = pt_spe_img_pn;
        this.data_status = data_status;
        this.jobid = jobid;
        this.idt = idt;
        this.udt = udt;
        this.date_seq = date_seq;

    }

    public String getPt_pub_code() {
        return pt_pub_code;
    }

    public void setPt_pub_code(String pt_pub_code) {
        this.pt_pub_code = pt_pub_code;
    }

    public String getPt_date() {
        return pt_date;
    }

    public void setPt_date(String pt_date) {
        this.pt_date = pt_date;
    }

    public String getPt_reg_no() {
        return pt_reg_no;
    }

    public void setPt_reg_no(String pt_reg_no) {
        this.pt_reg_no = pt_reg_no;
    }

    public String getPt_reg_date() {
        return pt_reg_date;
    }

    public void setPt_reg_date(String pt_reg_date) {
        this.pt_reg_date = pt_reg_date;
    }

    public String getPt_code() {
        return pt_code;
    }

    public void setPt_code(String pt_code) {
        this.pt_code = pt_code;
    }

    public String getPt_name() {
        return pt_name;
    }

    public void setPt_name(String pt_name) {
        this.pt_name = pt_name;
    }

    public String getPt_sub_code() {
        return pt_sub_code;
    }

    public void setPt_sub_code(String pt_sub_code) {
        this.pt_sub_code = pt_sub_code;
    }

    public String getPt_type_code() {
        return pt_type_code;
    }

    public void setPt_type_code(String pt_type_code) {
        this.pt_type_code = pt_type_code;
    }

    public String getPt_reg_per() {
        return pt_reg_per;
    }

    public void setPt_reg_per(String pt_reg_per) {
        this.pt_reg_per = pt_reg_per;
    }

    public String getPt_invent_per() {
        return pt_invent_per;
    }

    public void setPt_invent_per(String pt_invent_per) {
        this.pt_invent_per = pt_invent_per;
    }

    public String getPt_summary() {
        return pt_summary;
    }

    public void setPt_summary(String pt_summary) {
        this.pt_summary = pt_summary;
    }

    public String getPt_sov_itm() {
        return pt_sov_itm;
    }

    public void setPt_sov_itm(String pt_sov_itm) {
        this.pt_sov_itm = pt_sov_itm;
    }

    public String getPt_por_itm() {
        return pt_por_itm;
    }

    public void setPt_por_itm(String pt_por_itm) {
        this.pt_por_itm = pt_por_itm;
    }

    public String getPt_sml_itm() {
        return pt_sml_itm;
    }

    public void setPt_sml_itm(String pt_sml_itm) {
        this.pt_sml_itm = pt_sml_itm;
    }

    public String getPt_code_num() {
        return pt_code_num;
    }

    public void setPt_code_num(String pt_code_num) {
        this.pt_code_num = pt_code_num;
    }

    public String getPt_country() {
        return pt_country;
    }

    public void setPt_country(String pt_country) {
        this.pt_country = pt_country;
    }

    public String getDivision_regno() {
        return division_regno;
    }

    public void setDivision_regno(String division_regno) {
        this.division_regno = division_regno;
    }

    public String getPt_addr() {
        return pt_addr;
    }

    public void setPt_addr(String pt_addr) {
        this.pt_addr = pt_addr;
    }

    public String getPt_zipcode() {
        return pt_zipcode;
    }

    public void setPt_zipcode(String pt_zipcode) {
        this.pt_zipcode = pt_zipcode;
    }

    public String getPt_agent_org() {
        return pt_agent_org;
    }

    public void setPt_agent_org(String pt_agent_org) {
        this.pt_agent_org = pt_agent_org;
    }

    public String getPt_agent_pripid() {
        return pt_agent_pripid;
    }

    public void setPt_agent_pripid(String pt_agent_pripid) {
        this.pt_agent_pripid = pt_agent_pripid;
    }

    public String getPt_agent() {
        return pt_agent;
    }

    public void setPt_agent(String pt_agent) {
        this.pt_agent = pt_agent;
    }

    public String getPt_checkup_per() {
        return pt_checkup_per;
    }

    public void setPt_checkup_per(String pt_checkup_per) {
        this.pt_checkup_per = pt_checkup_per;
    }

    public String getPt_issue_date() {
        return pt_issue_date;
    }

    public void setPt_issue_date(String pt_issue_date) {
        this.pt_issue_date = pt_issue_date;
    }

    public String getPt_nal_reg() {
        return pt_nal_reg;
    }

    public void setPt_nal_reg(String pt_nal_reg) {
        this.pt_nal_reg = pt_nal_reg;
    }

    public String getPt_nal_pub() {
        return pt_nal_pub;
    }

    public void setPt_nal_pub(String pt_nal_pub) {
        this.pt_nal_pub = pt_nal_pub;
    }

    public String getPt_innation_date() {
        return pt_innation_date;
    }

    public void setPt_innation_date(String pt_innation_date) {
        this.pt_innation_date = pt_innation_date;
    }

    public String getPt_summary_url() {
        return pt_summary_url;
    }

    public void setPt_summary_url(String pt_summary_url) {
        this.pt_summary_url = pt_summary_url;
    }

    public String getPt_europe_sub_typeno() {
        return pt_europe_sub_typeno;
    }

    public void setPt_europe_sub_typeno(String pt_europe_sub_typeno) {
        this.pt_europe_sub_typeno = pt_europe_sub_typeno;
    }

    public String getPt_europe_typeno() {
        return pt_europe_typeno;
    }

    public void setPt_europe_typeno(String pt_europe_typeno) {
        this.pt_europe_typeno = pt_europe_typeno;
    }

    public String getPt_nat_sub_typeno() {
        return pt_nat_sub_typeno;
    }

    public void setPt_nat_sub_typeno(String pt_nat_sub_typeno) {
        this.pt_nat_sub_typeno = pt_nat_sub_typeno;
    }

    public String getPt_nat_typeno() {
        return pt_nat_typeno;
    }

    public void setPt_nat_typeno(String pt_nat_typeno) {
        this.pt_nat_typeno = pt_nat_typeno;
    }

    public String getPt_pub_url() {
        return pt_pub_url;
    }

    public void setPt_pub_url(String pt_pub_url) {
        this.pt_pub_url = pt_pub_url;
    }

    public String getPt_pagenum() {
        return pt_pagenum;
    }

    public void setPt_pagenum(String pt_pagenum) {
        this.pt_pagenum = pt_pagenum;
    }

    public String getPt_reg_natcode() {
        return pt_reg_natcode;
    }

    public void setPt_reg_natcode(String pt_reg_natcode) {
        this.pt_reg_natcode = pt_reg_natcode;
    }

    public String getPt_typeno_old() {
        return pt_typeno_old;
    }

    public void setPt_typeno_old(String pt_typeno_old) {
        this.pt_typeno_old = pt_typeno_old;
    }

    public String getPt_typeno() {
        return pt_typeno;
    }

    public void setPt_typeno(String pt_typeno) {
        this.pt_typeno = pt_typeno;
    }

    public String getPt_reg_source() {
        return pt_reg_source;
    }

    public void setPt_reg_source(String pt_reg_source) {
        this.pt_reg_source = pt_reg_source;
    }

    public String getPt_ref_doc() {
        return pt_ref_doc;
    }

    public void setPt_ref_doc(String pt_ref_doc) {
        this.pt_ref_doc = pt_ref_doc;
    }

    public String getPt_scope_type() {
        return pt_scope_type;
    }

    public void setPt_scope_type(String pt_scope_type) {
        this.pt_scope_type = pt_scope_type;
    }

    public String getPt_cp_doc() {
        return pt_cp_doc;
    }

    public void setPt_cp_doc(String pt_cp_doc) {
        this.pt_cp_doc = pt_cp_doc;
    }

    public String getPt_spe() {
        return pt_spe;
    }

    public void setPt_spe(String pt_spe) {
        this.pt_spe = pt_spe;
    }

    public String getPt_spe_img() {
        return pt_spe_img;
    }

    public void setPt_spe_img(String pt_spe_img) {
        this.pt_spe_img = pt_spe_img;
    }

    public String getPt_cp_doc_pn() {
        return pt_cp_doc_pn;
    }

    public void setPt_cp_doc_pn(String pt_cp_doc_pn) {
        this.pt_cp_doc_pn = pt_cp_doc_pn;
    }

    public String getPt_spe_pn() {
        return pt_spe_pn;
    }

    public void setPt_spe_pn(String pt_spe_pn) {
        this.pt_spe_pn = pt_spe_pn;
    }

    public String getPt_spe_img_pn() {
        return pt_spe_img_pn;
    }

    public void setPt_spe_img_pn(String pt_spe_img_pn) {
        this.pt_spe_img_pn = pt_spe_img_pn;
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
