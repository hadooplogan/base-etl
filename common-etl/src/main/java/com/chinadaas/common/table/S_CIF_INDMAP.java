package com.chinadaas.common.table;

/**
 * Created by gongxs01 on 2017/10/23.
 */
public class S_CIF_INDMAP {

    private String  zspid ;
    private String  cerno ;
    private String  encode_v1 ;
    private String  encode_v2;

    public S_CIF_INDMAP(String zspid, String cerno, String encode_v1, String encode_v2) {
        this.zspid = zspid;
        this.cerno = cerno;
        this.encode_v1 = encode_v1;
        this.encode_v2 = encode_v2;
    }



    public String getZspid() {
        return zspid;
    }

    public void setZspid(String zspid) {
        this.zspid = zspid;
    }

    public String getCerno() {
        return cerno;
    }

    public void setCerno(String cerno) {
        this.cerno = cerno;
    }

    public String getEncode_v1() {
        return encode_v1;
    }

    public void setEncode_v1(String encode_v1) {
        this.encode_v1 = encode_v1;
    }

    public String getEncode_v2() {
        return encode_v2;
    }

    public void setEncode_v2(String encode_v2) {
        this.encode_v2 = encode_v2;
    }
}
