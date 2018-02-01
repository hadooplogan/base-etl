package com.chinadaas.common.table;

import java.io.Serializable;

public class S_EN_USEDNAME implements Serializable{

  private String nodenum    ;
  private String pripid     ;
  private String usedname   ;
  private String jobid      ;



    public S_EN_USEDNAME(String nodenum, String pripid, String usedname, String jobid) {
        this.nodenum = nodenum;
        this.pripid = pripid;
        this.usedname = usedname;
        this.jobid = jobid;

    }

    public String getNodenum() {
        return nodenum;
    }

    public void setNodenum(String nodenum) {
        this.nodenum = nodenum;
    }

    public String getPripid() {
        return pripid;
    }

    public void setPripid(String pripid) {
        this.pripid = pripid;
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


}
