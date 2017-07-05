package com.chinadaas.newEnt.etl.general;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    /**
     * 根据当前日期，得到一年 的 日期范围边界
     */
    public static String getYearRangeDate() {
        Calendar calendar = Calendar.getInstance();
        Date date = new Date(System.currentTimeMillis());
        String currYearDate = getString(date);

        calendar.setTime(date);   //        calendar.add(Calendar.WEEK_OF_YEAR, -1);
        calendar.add(Calendar.YEAR, -1);
        date = calendar.getTime();
        String lastYearDate =getString(date);
        return currYearDate+"_"+lastYearDate;
    }

    public static String getString(Date date) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        return sdf.format(date);
    }
    public static Boolean regx(String str) {

        String rege1 = "^(13|15|18|17)[0-9]{9}$";
        String rege2 = "^[(|（]?(13|15|18|17)[0-9]{9}[-|(|)|）|\\|*| |,|，|/]{1,6}+(13|15|18|17)[0-9]{9}";
       /* String rege3 = "^[(|（]?(13|15|18|17)[0-9]{9}[-|(|)}）|\\|*| |,|，|/]{1,6}+(0[0-9]{2,3}[\-|\(\\|\*|\-|-|\ ]?)?([2-9][0-9]{6,7})(\-[0-9]{1,4})?";
        String rege4 = "^[(|（]?(0[0-9]{2,3}[-|)|)|）|\\|*|\-|-|\ }/]?)?([2-9]{1}[0-9]{1}{6,7})(\-[0-9]{1,4})?";
        String rege5 = "^[(|（]?(0[0-9]{2,3}[-|)|)|）|\\|*|\-|-|\ ]?)?([2-9]{1}[0-9]{1}{6,7})(\-[0-9]{1,4})?+(13|15|18|17)[0-9]{9}";
        String rege5 = "^[(|（]?(0[0-9]{2,3}[-|)|)|）|\\|*|\-|-|\ ]?)?([2-9]{1}[0-9]{1}{6,7})(\-[0-9]{1,4})?[-|(|)}）|\\|*| |,|，|/]+[(|（]?(0[0-9]{2,3}[-|)|)|）|\\|*|\-|-|\ ]?)?([2-9]{1}[0-9]{1}{5,7})(\-[0-9]{1,4})?";
        */


        // 邮箱验证规则            String regEx = "[a-zA-Z_]{1,}[0-9]{0,}@(([a-zA-z0-9]-*){1,}\\.){1,3}[a-zA-z\\-]{1,}";
        // 编译正则表达式
        Pattern pattern = Pattern.compile(rege1);
        Matcher matcher = pattern.matcher(str);
        // 字符串是否与正则表达式相匹配
        boolean rs = matcher.matches();


        return rs;

    }

}
