package com.chinadaas.newEnt.etl.general;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by 74061 on 2017/6/19.
 */
public class GeneralUtils {

    public static int getColumnsInt(String field) {
        if(field == null || field.equals("")){
            return 0;
        }else{
            return Integer.parseInt(field);
        }
    }

    public static String getColumns(String field) {
        if(field == null || field.equals("")){
            return "";
        }else{
            return field;
        }
    }

    public static Timestamp getColumnsTimeStamp(String field) {

        String format = "yyyyMMddHHmmss";
        Timestamp tstamp=null;
        //if (null != field && !"".equals(field)) {
        if(field == null || field.equals("")){
            return null;
        }else{
            SimpleDateFormat sdf = new SimpleDateFormat(format);
            Date date1 = null;
            try {
                date1 = sdf.parse(field);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            return tstamp= new Timestamp(date1.getTime());
        }
    }

    public static java.sql.Date getColumnsDate(String field) {

        if(field == null || field.equals("")){
            return null;
        }else{
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
            Date date = null;
            try {
                date = dateFormat.parse(field);
                System.out.println( date.toString());//切割掉不要的时分秒数据
            } catch (ParseException e) {
                e.printStackTrace();
            }

            java.sql.Date sqlDate=new java.sql.Date(date.getTime());
            return sqlDate;
        }

    }
}
