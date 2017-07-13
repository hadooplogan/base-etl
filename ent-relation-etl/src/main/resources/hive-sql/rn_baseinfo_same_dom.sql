--企业同地址关联
create table rn_baseinfo_same_dom_20170620 as
select dom
  from (select dom, count(distinct pripid) dom_count
          from (select *
                  from enterprisebaseinfocollect_hdfs_ext_20170620 a
                 where (length(a.DOM) > 4 AND
                       (a.dom LIKE '%号%' OR a.dom LIKE '%村%组%' OR
                       a.dom LIKE '%楼%'))
                   and ((length(a.entname) > 3) OR a.entname LIKE '%公司%' OR
                       a.entname LIKE '%室%' OR a.entname LIKE '%企业%'))
         where entstatus = '1'
           and dom is not null
           and dom <> ''
         group by dom
        having count(distinct pripid) > 1 and count(distinct pripid) <= 5);
--企业同地址关联
create table rn_baseinfo_same_dom_rela_20170620 as
select me.pripid, ai.dom
  from (select dom, count(distinct pripid) dom_count
          from (select *
                  from enterprisebaseinfocollect_hdfs_ext_20170620 a
                 where (length(a.DOM) > 4 AND
                       (a.dom LIKE '%号%' OR a.dom LIKE '%村%组%' OR
                       a.dom LIKE '%楼%'))
                   and ((length(a.entname) > 3) OR a.entname LIKE '%公司%' OR
                       a.entname LIKE '%室%' OR a.entname LIKE '%企业%')
                   and a.entstatus = '1')
         where dom is not null
           and dom <> ''
         group by dom
        having count(distinct pripid) > 1 and count(distinct pripid) <= 5) ai
 inner join (select distinct dom, pripid
               from enterprisebaseinfocollect_hdfs_ext_20170620 a
              where (length(a.DOM) > 4 AND
                    (a.dom LIKE '%号%' OR a.dom LIKE '%村%组%' OR
                    a.dom LIKE '%楼%'))
                and ((length(a.entname) > 3) OR a.entname LIKE '%公司%' OR
                    a.entname LIKE '%室%' OR a.entname LIKE '%企业%')
                and a.entstatus = '1') me
    on ai.dom = me.dom
 where me.pripid is not null
   and me.pripid <> '';
