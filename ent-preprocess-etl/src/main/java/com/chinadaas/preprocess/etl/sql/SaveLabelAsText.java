package com.chinadaas.preprocess.etl.sql;

import com.chinadaas.common.util.DataFrameUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * @author haoxing
 *
 *查询出工行所需要的标签。
 * 存储成text文件格式，因为spark存储text文件只能存储成一个字段，就把数据拼成一个串。
 */
public class SaveLabelAsText {

    public static Dataset getentlabel(SparkSession spark){

        String hql = "select \n" +
                "concat_ws('\\u001B',a.pripid,\n" +//pripid
                "case when a.ee0008 = '-9' then '未知' when a.ee0008 is null then '未知' else a.ee0008 end,\n" +//企业分类码
                "case when a.ee0009 = '-9' then '未知' when a.ee0009 is null then '未知' else a.ee0009 end,\n" +//企业性质
                "nvl(i.eb0043,0),\n" +//企业股东数目
                "case when nvl(i.eb0043,0)-nvl(i.eb0044,0) > 0 then '是' else '否' end,\n" +//是否有法人股东
                "case when a.eb0002 = '-9' then '未知' when a.eb0002 is null then '未知' when a.eb0002 = '01' then '直辖市' when a.eb0002 = '02'then '省会城市' when a.eb0002 = '03' then '一般城市' else a.eb0002 end,\n" +//地区级别
                "case when a.ee0017 = '-9' then '未知' when a.ee0017 is null then '未知' when a.ee0017 = '1' then '以个人财产出资' when a.ee0017 = '2' then '以家庭共有财产作为个人出资' else a.ee0017 end,\n" +//出资方式(个独)或组成形式（个体
                "nvl(a.ee0005,'未知'),\n" +//注册资本币种
                "nvl(a.ee0011,'未知'),\n" +//行业门类
                "nvl(c.eb0097,0),\n" +//最近一年列入严重违法次数
                "nvl(i.eb0043,0)-nvl(i.eb0044,0),\n" +//企业法人股东数
                "nvl(a.ee0001,'未知'),\n" +//省份
                "nvl(b.ee0023,'未知'),\n" +//目前是否存在经营异常
                "nvl(e.eb0041,0),\n" +//法定代表人曾对外任职企业数
                "nvl(g.eb0027,0),\n" +//法定代表人对外投资有效企业数
                "case when a.ee0018 = '-9' then '未知' when a.ee0018 is null then '未知' else a.ee0018 end,\n" +//设立方式
                "nvl(c.eb0095,'未知'),\n" +//目前是否存在严重违法
                "nvl(f.eb0033,0),\n" +//历史对外投资公司有效企业数
                "nvl(a.entname,'未知'),\n" +//企业名称
                "nvl(a.ee0012,'未知'),\n" +//行业大类
                "nvl(b.eb0092,0),\n" +//最近一年列入经营异常名录次数
                "case when a.ee0015 = '-9' then '未知' when a.ee0015 is null then '未知' else a.ee0015 end,\n" +//合伙方式
                "nvl(i.eb0044,0),\n" +//企业自然人股东数
                "nvl(a.ee0004,'未知'),\n" +//注册资本（原币）
                "case when a.ee0014 = '-9' then '未知' when a.ee0014 is null then '未知' else a.ee0014 end,\n" +//信用等级
                "nvl(d.eb0100,0),\n" +//近三年行政处罚数
                "nvl(a.ee0007,'未知'),\n" +//企业类型
                "nvl(a.ee0013,'未知'),\n" +//行业类别
                "case when a.ee0003 <> '1' then '是' else '否' end,\n" +//关停标志
                "case when a.ee0016 = '-9' then '未知' when a.ee0016 is null then '未知' else a.ee0016 end,\n" +//隶属分类
                "nvl(a.credit_code,'未知'),\n" +//统一社会信用代码
                "nvl(a.ee0002,'未知')) as lable\n"+//城市
                "from base a\n" +
                "left join abnormity b on a.pripid = b.pripid\n" +
                "left join breaklaw c on a.pripid = c.pripid\n" +
                "left join caseinfo d on a.pripid = d.pripid\n" +
                "left join legaloffice e on a.pripid = e.pripid\n" +
                "left join entinvestment f on a.pripid = f.pripid\n" +
                "left join legalinvestment g on a.pripid = g.pripid\n" +
                "left join stockofcompany i on a.pripid = i.pripid";

        return DataFrameUtil.getDataFrame(spark,hql,"tmpguangda");

    }
}
