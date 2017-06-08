package com.chinadaas.association.etl.table;

import java.io.Serializable;

/**
 * Created by gongxs01 on 2017/6/6.
 */
public class PersonEO implements Serializable{

    private static final long serialVersionUID = -5439011425468607614L;

    public PersonEO(){}
        public PersonEO(String s_ext_nodenum,String pripid,String name,String certype,String cerno,
                      String sex,String natdate,String lerepsign,String country,String position,
                      String offhfrom,String offhto,String zspid){

            this.s_ext_nodenum=s_ext_nodenum ;
            this.pripid=pripid        ;
            this.name=name          ;
            this.certype=certype       ;
            this.cerno=cerno         ;
            this.sex =sex          ;
            this.natdate=natdate       ;
            this.lerepsign=lerepsign     ;
            this.country=country       ;
            this.position=position      ;
            this.offhfrom=offhfrom      ;
            this.offhto=offhto        ;
            this.zspid=zspid         ;
        }
        private  String s_ext_nodenum ;
        private  String pripid        ;
        private  String name          ;
        private  String certype       ;
        private  String cerno         ;
        private  String sex           ;
        private  String natdate       ;
        private  String lerepsign     ;
        private  String country       ;
        private  String position      ;
        private  String offhfrom      ;
        private  String offhto        ;
        private  String zspid         ;

        public String getS_ext_nodenum() {
            return s_ext_nodenum;
        }

        public void setS_ext_nodenum(String s_ext_nodenum) {
            this.s_ext_nodenum = s_ext_nodenum;
        }

        public String getPripid() {
            return pripid;
        }

        public void setPripid(String pripid) {
            this.pripid = pripid;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getCertype() {
            return certype;
        }

        public void setCertype(String certype) {
            this.certype = certype;
        }

        public String getCerno() {
            return cerno;
        }

        public void setCerno(String cerno) {
            this.cerno = cerno;
        }

        public String getSex() {
            return sex;
        }

        public void setSex(String sex) {
            this.sex = sex;
        }

        public String getNatdate() {
            return natdate;
        }

        public void setNatdate(String natdate) {
            this.natdate = natdate;
        }

        public String getLerepsign() {
            return lerepsign;
        }

        public void setLerepsign(String lerepsign) {
            this.lerepsign = lerepsign;
        }

        public String getCountry() {
            return country;
        }

        public void setCountry(String country) {
            this.country = country;
        }

        public String getPosition() {
            return position;
        }

        public void setPosition(String position) {
            this.position = position;
        }

        public String getOffhfrom() {
            return offhfrom;
        }

        public void setOffhfrom(String offhfrom) {
            this.offhfrom = offhfrom;
        }

        public String getOffhto() {
            return offhto;
        }

        public void setOffhto(String offhto) {
            this.offhto = offhto;
        }

        public String getZspid() {
            return zspid;
        }

        public void setZspid(String zspid) {
            this.zspid = zspid;
        }
}
