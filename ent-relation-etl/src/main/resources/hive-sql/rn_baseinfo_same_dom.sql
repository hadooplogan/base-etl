--��ҵͬ��ַ����
create table rn_baseinfo_same_dom_20170508 as
select dom
  from (select dom, count(distinct pripid) dom_count
          from (select *
                  from enterprisebaseinfocollect_hdfs_ext_20170508 a
                 where (length(a.DOM) > 4 AND
                       (a.dom LIKE '%��%' OR a.dom LIKE '%��%��%' OR
                       a.dom LIKE '%¥%'))
                   and ((length(a.entname) > 3) OR a.entname LIKE '%��˾%' OR
                       a.entname LIKE '%��%' OR a.entname LIKE '%��ҵ%'))
         where entstatus = '1'
           and dom is not null
           and dom <> ''
         group by dom
        having count(distinct pripid) > 1 and count(distinct pripid) < 5);
--��ҵͬ��ַ����
create table rn_baseinfo_same_dom_rela_20170508 as
select me.pripid, ai.dom
  from (select dom, count(distinct pripid) dom_count
          from (select *
                  from enterprisebaseinfocollect_hdfs_ext_20170508 a
                 where (length(a.DOM) > 4 AND
                       (a.dom LIKE '%��%' OR a.dom LIKE '%��%��%' OR
                       a.dom LIKE '%¥%'))
                   and ((length(a.entname) > 3) OR a.entname LIKE '%��˾%' OR
                       a.entname LIKE '%��%' OR a.entname LIKE '%��ҵ%')
                   and a.entstatus = '1')
         where dom is not null
           and dom <> ''
         group by dom
        having count(distinct pripid) > 1 and count(distinct pripid) <= 5) ai
 inner join (select distinct dom, pripid
               from enterprisebaseinfocollect_hdfs_ext_20170508 a
              where (length(a.DOM) > 4 AND
                    (a.dom LIKE '%��%' OR a.dom LIKE '%��%��%' OR
                    a.dom LIKE '%¥%'))
                and ((length(a.entname) > 3) OR a.entname LIKE '%��˾%' OR
                    a.entname LIKE '%��%' OR a.entname LIKE '%��ҵ%')
                and a.entstatus = '1') me
    on ai.dom = me.dom
 where me.pripid is not null
   and me.pripid <> '';
