package com.chinadaas.next.etl.sparksql.main;

import com.chinadaas.common.common.CommonConfig;
import com.chinadaas.common.common.Constants;
import com.chinadaas.common.register.RegisterTable;
import com.chinadaas.common.util.DataFrameUtil;
import com.chinadaas.common.util.LogUtil;
import com.chinadaas.common.util.MyFileUtil;
import com.chinadaas.next.etl.sparksql.sql.*;
import org.apache.spark.sql.SparkSession;

import java.util.Map;


/**
 * @author haoxing
 *
 *通过读取配置文件，来读取数据。
 */
public class BaseBiKpiApp {

    protected static LogUtil logger = LogUtil.getLogger(BaseBiKpiApp.class);

    public static void main(String[] args) {

        /**
         *  参数1  表后缀
         *  参数2  储存路径
         */
        SparkSession spark = SparkSession
                .builder()
                .appName("Chinadaas DEAL_ENT_INDEX_APP")
                .enableHiveSupport()
                .getOrCreate();

        //提高任务并行度
        spark.sqlContext().conf().setConfString("spark.sql.shuffle.partitions", "400");

        //读取生产环境全量表的配置文件 table.list
        //model  model = inc增量更新 model = all 全量更新  默认您全量更新 m
        String date = args[0];
        String AllCfgPath = args[1];


        if (args != null && args.length == 2) {

        } else {
            System.out.println("args erro");
            return;
        }

        //全量更新 注册需要使用的hdfs表
        Map<String, String> allcfg = MyFileUtil.getFileCfg(AllCfgPath);
        //  Map<String,String> cfg = MyFileUtil.getDateCfg(AllCfgPath);

        //把table.list里面的文档读取过来注册临时表
        registerTable(spark, date, allcfg);

        //拿出需要日期拼串的表(读取分区表的方式)
        //   String trademarkdate = cfg.get("S_EN_TRADEMARKINFO");
        //   String productcopyrightinfodate = cfg.get("S_EN_PRODUCTCOPYRIGHTINFO");
        //   String copyrightorgdate = cfg.get("S_EN_COPYRIGHTORG");
        //   String copyrightinfodate = cfg.get("S_EN_COPYRIGHTINFO");



        //execBase(spark);
        //execAbnormity(spark);
        //execBreakLaw(spark);
        //execCaseInfo(spark);
        //execPublishSoftwork(spark);
        //execPublishCopyright(spark);
        //execSubcompany(spark);
        execTradeMrkInfo(spark);
        //execEnterpriseChange(spark);
        //execTopExperienceKpi(spark);
        //execEnterpriseInvestment(spark);
        //execLegalInvestment(spark);
        //execStockOfCompany(spark);
        //execLegalOffice(spark);
        //execPatent(spark);

        /*暂停处理的司法标签(暂时待定)*/
        //exectopLessCredit(spark);
        //exectopLessCreditCase(spark);
        //exeNpLessCredit(spark);
        //exeNpLessCreditCase(spark);
        //execLgLessCredit(spark);
        //execlgLessCreditCase(spark);

        /*法人对外任职，与主题企业拥有合伙企业标签(修改中)*/
        //法人对外任职,非法人。完成法人对外任职。


        spark.stop();
    }


    private static void execBase(SparkSession spark) {
        logger.info("------开始处理base指标和部分年报指标----------");
        DataFrameUtil.saveAsParquetOverwrite(BaseBiKpiETL.getBaseBiKpi(spark, CommonConfig.getValue(Constants.ENT_INDEX_BASE_DATADATE)), CommonConfig.getValue(Constants.ENT_INDEX_BASE_DIR));
        logger.info("------结束处理base指标和部分年报指标 处理完成----------");
    }


    private static void execAbnormity(SparkSession spark) {
        logger.info("------开始处理经营异常指标----------");
        DataFrameUtil.saveAsParquetOverwrite(AbnormityKpiETL.getAbnormityKpi(spark, CommonConfig.getValue(Constants.ENT_INDEX_ABNORMITY_DATADATE)), CommonConfig.getValue(Constants.ENT_INDEX_ABNORMITY_DIR));
        logger.info("------结束处理经营异常,处理完成----------");
    }


    private static void execBreakLaw(SparkSession spark) {
        logger.info("------开始处理严重违法指标----------");
        DataFrameUtil.saveAsParquetOverwrite(BreakLawKpiETL.getBreakLawKpi(spark, CommonConfig.getValue(Constants.ENT_INDEX_BREAKLAW_DATADATE)), CommonConfig.getValue(Constants.ENT_INDEX_BREAKLAW_DIR));
        logger.info("------结束处理严重违法,处理完成----------");
    }

    private static void execCaseInfo(SparkSession spark) {
        logger.info("------开始处理行政处罚指标----------");
        DataFrameUtil.saveAsParquetOverwrite(CaseInfoKpiETL.getCaseInfoKpi(spark, CommonConfig.getValue(Constants.ENT_INDEX_CASEINFO_DATADATE)), CommonConfig.getValue(Constants.ENT_INDEX_CASEINFO_DIR));
        logger.info("------结束处理行政处罚,处理完成----------");
    }


    /* 司法类标签口径暂停开发
     * private static  void exectopLessCredit(SparkSession spark){
     * logger.info("-------开始是处理高管失信次数指标------");
     * DataFrameUtil.saveAsParquetOverwrite(topLessCredit.getTopLessCredit(spark,CommonConfig.getValue(Constants.ENT_INDEX_LESSCREDIT_DATADATE)),CommonConfig.getValue(Constants.ENT_INDEX_LESSCREDIT_DIR));
     * logger.info("------结束处理高管失信次数指标------");
     * }
     * private static  void exectopLessCreditCase(SparkSession spark){
     * logger.info("------开始处理高管失信案件次数指标------");
     * DataFrameUtil.saveAsParquetOverwrite(topLessCreditCase.getTopLessCreditCases(spark,CommonConfig.getValue(Constants.ENT_INDEX_TOPLESSCREDITCASE_DATADATE)),CommonConfig.getValue(Constants.ENT_INDEX_TOPLESSCREDITCASE_DIR));
     * logger.info("------结束处理高管失信案件次数指标------");
     * }
     * private static void exeNpLessCredit(SparkSession spark){
     * logger.info("------开始处理自然人失信指标------");
     * DataFrameUtil.saveAsParquetOverwrite(natrualPersonLessCredit.getNatrualPersonLessCredit(spark,CommonConfig.getValue(Constants.ENT_INDEX_NPLESSCREDIT_DATADATE)),CommonConfig.getValue(Constants.ENT_INDEX_NPLESSCREDIT_DIR));
     * logger.info("------结束处理自然人失信指标------");
     * }
     * private static void exeNpLessCreditCase(SparkSession spark){
     * logger.info("------开始处理自然人失信案件数------");
     * DataFrameUtil.saveAsParquetOverwrite(naturalPersonLessCreditCase.getNatrualPersonLessCreditCase(spark,CommonConfig.getValue(Constants.ENT_INDEX_NPLESSCREDITCASE_DATADATE)),CommonConfig.getValue(Constants.ENT_INDEX_NPLESSCREDITCASE_DIR));
     * logger.info("------结束处理自然人失信案件数------");
     * }
     * private static void execLgLessCredit(SparkSession spark){
     * logger.info("------开始处理法人失信次数指标------");
     * DataFrameUtil.saveAsParquetOverwrite(legalPersonLessCredit.getlgPersonLessCredit(spark,CommonConfig.getValue(Constants.ENT_INDEX_LEGALLESSCREDIT_DATADATE)),CommonConfig.getValue(Constants.ENT_INDEX_LEGALLESSCREDIT_DIR));
     * logger.info("------结束处理法人失信次数指标------");
     * }
     * private static void execlgLessCreditCase(SparkSession spark){
     * logger.info("----开始处理法人失信案件数指标------");
     * DataFrameUtil.saveAsParquetOverwrite(legalPersonLessCreditCase.getLegalPersonLessCreditCase(spark,CommonConfig.getValue(Constants.ENT_INDEX_LEGALLESSCREDITCASE_DATADATE)),CommonConfig.getValue(Constants.ENT_INDEX_LEGALLESSCREDITCASE_DIR));
     * logger.info("----结束处理法人失信案件数指标------");
     * }
     */


    public static void execTopExperienceKpi(SparkSession spark) {
        logger.info("------开始计算高管经验指标------");
        DataFrameUtil.saveAsParquetOverwrite(TopExperience.getTopExperienceKpi(spark, CommonConfig.getValue(Constants.ENT_INDEX_TOPEXPERIENCE_DATADATE)), CommonConfig.getValue(Constants.ENT_INDEX_TOPEXPERIENCE_DIR));
        logger.info("------结束计算高管经验指标------");
    }

    public static void execPublishSoftwork(SparkSession spark) {
        logger.info("------开始计算公司软件发布数量指标------");
        DataFrameUtil.saveAsParquetOverwrite(PublishSoftWork.getPublishSoftWork(spark, CommonConfig.getValue(Constants.ENT_INDEX_PUBLISHSOFTWORK_DATADATE)), CommonConfig.getValue(Constants.ENT_INDEX_PUBLISHSOFTWORK_DIR));
        logger.info("------结束计算公司软件发布数量指标------");
    }


    public static void execPublishCopyright(SparkSession spark) {
        logger.info("------开始计算软件著作权发布数量指标------");
        DataFrameUtil.saveAsParquetOverwrite(PublishCopyright.getPublishCopyright(spark, CommonConfig.getValue(Constants.ENT_INDEX_PUBLISHCOPYRIGHT_DATADATE)), CommonConfig.getValue(Constants.ENT_INDEX_PUBLISHCOPYRIGHT_DIR));
        logger.info("------结束计算软件著作权发布数量指标------");
    }

    public static void execSubcompany(SparkSession spark) {
        logger.info("------开始计算子公司信息指标------");
        DataFrameUtil.saveAsParquetOverwrite(Subcompany.getSubcompany(spark, CommonConfig.getValue(Constants.ENT_INDEX_SUBCOMPANY_DATADATE)), CommonConfig.getValue(Constants.ENT_INDEX_SUBCOMPANY_DIR));
        logger.info("------结束计算子公司信息指标------");
    }


    public static void execTradeMrkInfo(SparkSession spark) {
        logger.info("------开始计算商标信息指标------");
        DataFrameUtil.saveAsParquetOverwrite(TradeMarkInfo.getTradeMarkInfo(spark, CommonConfig.getValue(Constants.ENT_INDEX_TRADEMARKINFO_DATADATE)), CommonConfig.getValue(Constants.ENT_INDEX_TRADEMARKINFO_DIR));
        logger.info("------结束计算商标信息指标------");
    }

    public static void execEnterpriseChange(SparkSession spark) {
        logger.info("------开始计算企业变更信息------");
        DataFrameUtil.saveAsParquetOverwrite(EnterpriseChange.getEnterpriseChange(spark, CommonConfig.getValue(Constants.ENT_INDEX_ENTERPRISE_EXCHANGE_DATADATE)), CommonConfig.getValue(Constants.ENT_INDEX_ENTERPRISE_EXCHANGE_DIR));
        logger.info("------结束计算企业变更信息------");
    }

    public static void execStockOfCompany(SparkSession spark) {
        logger.info("------开始计算企业股东数------");
        DataFrameUtil.saveAsParquetOverwrite(StockOfCompanyETL.getStockOfCompany(spark, CommonConfig.getValue(Constants.ENT_INDEX_STOCKOFCOMPANY_DATADATE)), CommonConfig.getValue(Constants.ENT_INDEX_STOCKOFCOMPANY_DIR));
        logger.info("------结束计算企业股东数------");
    }

    public static void execLegalOffice(SparkSession spark) {
        logger.info("------开始计算法人对外任职指标------");
        DataFrameUtil.saveAsParquetOverwrite(LegalOffice.getLegalOffice(spark, CommonConfig.getValue(Constants.ENT_INDEX_LEGALOFFICE_DATADATE)), CommonConfig.getValue(Constants.ENT_INDEX_LEGALOFFICE_DIR));
        logger.info("------结束计算法人对外任职指标------");
    }


    public static void execLegalInvestment(SparkSession spark) {
        logger.info("-----开始计算法人对外投资指标------");
        DataFrameUtil.saveAsParquetOverwrite(LegalInvestment.getLegalInvestment(spark, CommonConfig.getValue(Constants.ENT_INDEX_LEGALINVESTMENT_DATADATE)), CommonConfig.getValue(Constants.ENT_INDEX_LEGALINVESTMENT_DIR));
        logger.info("-----结束计算法人对外投资指标------");
    }

    public static void execEnterpriseInvestment(SparkSession spark) {
        logger.info("------开始计算企业对外投资指标------");
        DataFrameUtil.saveAsParquetOverwrite(EnterpriseInvestment.getEnterpriseInvestment(spark, CommonConfig.getValue(Constants.ENT_INDEX_ENTERPRISEINVESTMENT_DATADATE)), CommonConfig.getValue(Constants.ENT_INDEX_ENTERPRISEINVESTMENT_DIR));
        logger.info("------结束计算企业对外投资指标------");

    }

    public static void execPatent(SparkSession spark){
        logger.info("------开始计算企业专利指标------");
        DataFrameUtil.saveAsParquetOverwrite(PatentKpiETL.getPatentKpi(spark),CommonConfig.getValue(Constants.ENT_INDEX_PATENT_DIR));
        logger.info("------结束计算企业专利指标------");
    }


    //注册表，数据来自于我hive的hdfs文件


    public static void registerTable(SparkSession spark, String date, Map<String, String> allcfg) {


        String entPath = null;
        String invPath = null;
        String personPath = null;
        String alterPath = null;
        String casePartyPath = null;
        String caseBasePath = null;
        String productcpinfoPath = null;
        String copyrightorgPath = null;
        String copyrightinfoPath = null;
        String trademarkinfopath = null;
        String abnormityPath = null;
        String breaklaw = null;

        String patentinfo = null;
        String patentcopyright = null;
        String patentlawstate = null;

        entPath = allcfg.get("ENTERPRISEBASEINFOCOLLECT");
        personPath = allcfg.get("E_PRI_PERSON");
        invPath = allcfg.get("E_INV_INVESTMENT");
        alterPath = allcfg.get("E_ALTER_RECODER");
        abnormityPath = allcfg.get("S_EN_ABNORMITY");
        breaklaw = allcfg.get("S_EN_BREAK_LAW");
        //待添加的5个表
        caseBasePath = allcfg.get("S_EN_CASEBASEINFO");
        casePartyPath = allcfg.get("S_EN_CASEPARTYINFO");
        productcpinfoPath = allcfg.get("S_EN_PRODUCTCOPYRIGHTINFO");
        copyrightorgPath = allcfg.get("S_EN_COPYRIGHTORG");
        copyrightinfoPath = allcfg.get("S_EN_COPYRIGHTINFO");
        trademarkinfopath = allcfg.get("S_EN_TRADEMARKINFO");
        //专利数据使用的表 2018-01-08
        patentinfo = allcfg.get("S_SIPO_PATENT_INFO");
        patentcopyright = allcfg.get("S_SIPO_PATENT_COPYRIGHT");
        patentlawstate = allcfg.get("S_SIPO_PATENT_LAWSTATE");

        //注册临时表

        RegisterTable.registerBaseInfoTable(spark, "enterprisebaseinfocollect", date, entPath);
        RegisterTable.registerEpriPersonTable(spark, "e_pri_person", date, personPath);
        RegisterTable.registerInvTable(spark, "e_inv_investment", date, invPath);
        RegisterTable.registerAlterRecoderTable(spark, "e_alter_recoder", date, alterPath);
        RegisterTable.regiserAbnormityTable(spark, "s_en_abnormity", abnormityPath);
        RegisterTable.regiserBreakLawTable(spark, "s_en_break_law", breaklaw);
        RegisterTable.registerCaseBaseInfo(spark, "s_en_casebaseinfo", caseBasePath);
        RegisterTable.registerCaseParty(spark, "s_en_casepartyinfo", casePartyPath);
        RegisterTable.registerTradeMarkInfo(spark, "s_en_trademarkinfo", trademarkinfopath);
        RegisterTable.registerProductCopyRightInfo(spark, "s_en_productcopyrightinfo", productcpinfoPath);
        RegisterTable.registerCopyRightOrg(spark, "s_en_copyrightorg", copyrightorgPath);
        RegisterTable.registerCopyRightInfo(spark, "s_en_copyrightinfo", copyrightinfoPath);
        //patent表
       // RegisterTable.registerS_SIPO_PATENT_INFO(spark,"s_sipo_patent_info",patentinfo);
       // RegisterTable.registerS_SIPO_PATENT_COPYRIGHT(spark,"s_sipo_patent_copyright",patentcopyright);
       // RegisterTable.registerS_SIPO_PATENT_LAWSTATE(spark,"s_sipo_patent_lawstate",patentlawstate);
    }
}







