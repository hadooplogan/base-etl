package com.chinadaas.association.etl.table;

import java.io.Serializable;

/**
 * Created by gongxs01 on 2017/12/14.
 */
public class Hold implements Serializable{

    private String pripid;

    private String topripid;


    private String tophold;

    public Hold(String pripid, String topripid) {
        this.pripid = pripid;
        this.topripid = topripid;
    }

    public Hold() {
    }

    public String getTophold() {
        return tophold;
    }

    public void setTophold(String tophold) {
        this.tophold = tophold;
    }

    public String getPripid() {
        return pripid;
    }

    public void setPripid(String pripid) {
        this.pripid = pripid;
    }

    public String getTopripid() {
        return topripid;
    }

    public void setTopripid(String topripid) {
        this.topripid = topripid;
    }
}
