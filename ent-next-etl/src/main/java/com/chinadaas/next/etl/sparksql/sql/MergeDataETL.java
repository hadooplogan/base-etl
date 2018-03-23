package com.chinadaas.next.etl.sparksql.sql;

import com.chinadaas.common.util.DataFrameUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;


/**
 * @author haoxing
 * @date 20180208
 */
public class MergeDataETL {

    public static Dataset getMergeData(SparkSession spark) {

        String hql = "select a.*,\n" +
                "nvl(b.eb0091,0) as eb0091,\n" +
                "nvl(b.ee0023,'n') as ee0023,\n" +
                "nvl(b.eb0092,0) as eb0092,\n" +
                "nvl(b.eb0093,0) as eb0093,\n" +
                "nvl(b.eb0094,0) as eb0094,\n" +
                "nvl(c.eb0095,'-9') as eb0095,\n" +
                "nvl(c.eb0096,0) as eb0096,\n" +
                "nvl(c.eb0097,0) as eb0097,\n" +
                "nvl(c.eb0098,0) as eb0098,\n" +
                "nvl(c.eb0099,0) as eb0099,\n" +
                "nvl(d.eb0101,0) as eb0101,\n" +
                "nvl(d.eb0100,0) as eb0100," +
                "nvl(d.eb0107,'n')as eb0107,\n" +
                "nvl(f.eb0070,0) as eb0070,\n" +
                "nvl(f.eb0071,0) as eb0071,\n" +
                "nvl(f.eb0072,0) as eb0072,\n" +
                "nvl(f.eb0073,0) as eb0073,\n" +
                "nvl(f.eb0074,0) as eb0074,\n" +
                "nvl(f.eb0075,0) as eb0075,\n" +
                "nvl(f.eb0076,0) as eb0076,\n" +
                "nvl(f.eb0077,0) as eb0077,\n" +
                "nvl(f.eb0078,0) as eb0078,\n" +
                "nvl(f.eb0111,0) as eb0111,\n" +
                "nvl(g.eb0022,0) as eb0022,\n" +
                "nvl(g.eb0023,0) as eb0023,\n" +
                "nvl(h.eb0049,0) as eb0049,\n" +
                "nvl(h.eb0047,0) as eb0047,\n" +
                "nvl(h.eb0048,0) as eb0048,\n" +
                "nvl(h.eb0050,0) as eb0050,\n" +
                "nvl(i.eb0055,0) as eb0055,\n" +
                "nvl(i.eb0056,0) as eb0056,\n" +
                "nvl(i.eb0057,0) as eb0057,\n" +
                "nvl(i.eb0058,0) as eb0058,\n" +
                "nvl(j.eb0051,0) as eb0051,\n" +
                "nvl(j.eb0052,0) as eb0052,\n" +
                "nvl(j.eb0053,0) as eb0053,\n" +
                "nvl(j.eb0054,0) as eb0054,\n" +
                "nvl(j.eb0113,0.0) as eb0113,\n" +
                "nvl(m.eb0026,0) as eb0026,\n" +
                "nvl(m.eb0027,0) as eb0027,\n" +
                "nvl(m.eb0028,0) as eb0028,\n" +
                "nvl(k.eb0029,0) as eb0029,\n" +
                "nvl(k.eb0030,0) as eb0030,\n" +
                "nvl(k.eb0031,0) as eb0031,\n" +
                "nvl(k.eb0032,0) as eb0032,\n" +
                "nvl(k.eb0033,0) as eb0033,\n" +
                "nvl(k.eb0034,0) as eb0034,\n" +
                "nvl(k.eb0035,0) as eb0035,\n" +
                "nvl(k.eb0036,0.0) as eb0036,\n" +
                "nvl(k.eb0037,0.0) as eb0037,\n" +
                "nvl(k.eb0038,0.0) as eb0038,\n" +
                "nvl(k.eb0039,0.0) as eb0039,\n" +
                "nvl(k.eb0040,0.0) as eb0040,\n" +
                "nvl(k.eb0109,0.0) as eb0109,\n" +
                "nvl(k.eb0108,0) as eb0108,\n" +
                "nvl(k.eb0110,0) as eb0110,\n" +
                "nvl(l.eb0041,0) as eb0041,\n" +
                "nvl(l.eb0042,0) as eb0042,\n" +
                "nvl(t.eb0043,0) as eb0043,\n" +
                "nvl(t.eb0044,0) as eb0044,\n" +
                "nvl(t.eb0043,0)-nvl(t.eb0044,0) as eb0045,\n" +
                "nvl(t.eb0021,'-9') as eb0021,\n" +
                "nvl(n.eb0066,'n') as eb0066,\n" +
                "nvl(n.eb0067,0) as eb0067,\n" +
                "nvl(n.eb0068,0) as eb0068,\n" +
                "nvl(o.eb0059,0) as eb0059,\n" +
                "nvl(o.eb0060,0) as eb0060,\n" +
                "nvl(o.eb0061,0) as eb0061,\n" +
                "nvl(o.eb0062,0) as eb0062,\n" +
                "nvl(o.eb0112,0) as eb0112,\n" +
                "nvl(p.ee0066,'未知') as ee0066,\n" +
                "nvl(p.ee0067,'未知') as ee0067,\n" +
                "nvl(p.ee0068,'未知') as ee0068,\n" +
                "nvl(p.ee0069,'未知') as ee0069,\n" +
                "nvl(p.ee0070,'未知') as ee0070,\n" +
                "nvl(p.ee0071,'未知') as ee0071,\n" +
                "nvl(p.ee0072,'未知') as ee0072,\n" +
                "nvl(p.ee0073,'未知') as ee0073,\n" +
                "nvl(p.ee0074,'未知') as ee0074,\n" +
                "nvl(p.ee0075,'未知') as ee0075,\n" +
                "nvl(p.ee0076,'未知') as ee0076,\n" +
                "nvl(p.ee0077,'未知') as ee0077,\n" +
                "nvl(p.ee0078,'未知') as ee0078,\n" +
                "nvl(p.ee0080,'未知') as ee0080,\n" +
                "nvl(p.ee0081,'未知') as ee0081,\n" +
                "nvl(p.ee0082,'未知') as ee0082,\n" +
                "nvl(p.ee0083,'未知') as ee0083,\n" +
                "nvl(q.ee0033,'n') as ee0033,\n" +
                "nvl(q.ee0034,'-9') as ee0034,\n" +
                "nvl(q.ee0036,'-9') as ee0036,\n" +
                "nvl(q.ee0037,'-9') as ee0037,\n" +
                "case when nvl(t.eb0043,0)-nvl(t.eb0044,0) > 0 then 'y' else 'n' end as eb0106\n" +
                "from basebikpitemp a \n" +
                "left join tmppublishsoftwork j on a.pripid = j.pripid\n" +
                "left join abnormitykpitemp b on a.pripid = b.pripid\n" +
                "left join breaklawkpitemp c on a.pripid = c.pripid\n" +
                "left join caseinfokpitemp d on a.pripid = d.pripid\n" +
                "left join tmpenterpriseChange f on a.pripid = f.pripid\n" +
                "left join tmpsubconpany g on a.pripid = g.pripid\n" +
                "left join tmptrademarkinfo h on a.pripid = h.pripid\n" +
                "left join tmppublishcopyright i on a.pripid = i.pripid\n" +
                "left join stockcompanttmp t on a.pripid = t.pripid\n" +
                "left join enterpriseinvestment k on a.pripid = k.pripid\n" +
                "left join legalofficetmp l on a.pripid = l.pripid\n" +
                "left join topexperiencekpi n on a.pripid = n.pripid\n" +
                "left join legalinvestmenttmp m on a.pripid = m.pripid\n" +
                "left join patenttmp o on a.pripid = o.pripid\n" +
                "left join financial p on a.pripid = p.pripid\n" +
                "left join listed q on a.pripid = q.pripid";
        return DataFrameUtil.getDataFrame(spark, hql, "mergeDataTmp");
    }
}
