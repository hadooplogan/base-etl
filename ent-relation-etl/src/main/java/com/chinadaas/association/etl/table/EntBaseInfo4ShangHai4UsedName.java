package com.chinadaas.association.etl.table;

import java.io.Serializable;

/**
 * Created by hejianning on 2017/10/13.
 */
public class EntBaseInfo4ShangHai4UsedName implements Serializable{

    private static final long serialVersionUID = -191313031740635753L;

    private String pripid ;
    private String data_date;
    private String nodenum ;
    private String usedname ;
    private String jobid ;
    private String usedname_index;

    public EntBaseInfo4ShangHai4UsedName() {
    }

    public EntBaseInfo4ShangHai4UsedName(String pripid, String data_date, String nodenum, String usedname, String jobid) {
        this.pripid = pripid;
        this.data_date = data_date;
        this.nodenum = nodenum;
        this.usedname = usedname;
        this.usedname_index = usedname;
        this.jobid = jobid;
    }

    public String getPripid() {
        return pripid;
    }

    public void setPripid(String pripid) {
        this.pripid = pripid;
    }

    public String getData_date() {
        return data_date;
    }

    public void setData_date(String data_date) {
        this.data_date = data_date;
    }

    public String getNodenum() {
        return nodenum;
    }

    public void setNodenum(String nodenum) {
        this.nodenum = nodenum;
    }

    public String getUsedname() {
        return usedname;
    }

    public void setUsedname(String usedname) {
        this.usedname = usedname;
    }

    public String getJobid() {
        return jobid;
    }

    public void setJobid(String jobid) {
        this.jobid = jobid;
    }

    public String getUsedname_index() {
        return usedname_index;
    }

    public void setUsedname_index(String usedname_index) {
        this.usedname_index = usedname_index;
    }

    @Override
    public String toString() {
        return "EntBaseInfo4ShangHai4UsedName{" +
                "pripid='" + pripid + '\'' +
                ", data_date='" + data_date + '\'' +
                ", nodenum='" + nodenum + '\'' +
                ", usedname='" + usedname + '\'' +
                ", jobid='" + jobid + '\'' +
                '}';
    }
}
