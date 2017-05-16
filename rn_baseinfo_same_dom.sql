--企业同地址关联
create table rn_baseinfo_same_dom_20170424 as
select dom
  from (select dom, count(distinct pripid) dom_count
          from (select *
                  from enterprisebaseinfocollect_merge_20170417 a
                 where (length(a.DOM) > 4 AND
                       (a.dom LIKE '%号%' OR a.dom LIKE '%村%组%' OR
                       a.dom LIKE '%楼%'))
                   and ((a.entTYPE in ('10',
                                       '11',
                                       '12',
                                       '13',
                                       '14',
                                       '15',
                                       '31',
                                       '32',
                                       '33',
                                       '34') AND length(a.entname) > 3) OR
                       a.entname LIKE '%公司%' OR a.entname LIKE '%室%' OR
                       a.entname LIKE '%企业%'))
         where entstatus = '1'
           and dom is not null
           and dom <> ''
         group by dom
        having count(distinct pripid) > 1 and count(distinct pripid) < 5);
--企业同地址关联
create table rn_baseinfo_same_dom_rela_20170424 as
select me.pripid,
       ai.dom,
       concat_ws('#', me.pripid, '2') as spid,
       ai.dom as epid
  from (select dom, count(distinct pripid) dom_count
          from (select *
                  from enterprisebaseinfocollect_merge_20170417 a
                 where (length(a.DOM) > 4 AND
                       (a.dom LIKE '%号%' OR a.dom LIKE '%村%组%' OR
                       a.dom LIKE '%楼%'))
                   and ((a.entTYPE in ('10',
                                       '11',
                                       '12',
                                       '13',
                                       '14',
                                       '15',
                                       '31',
                                       '32',
                                       '33',
                                       '34') AND length(a.entname) > 3) OR
                       a.entname LIKE '%公司%' OR a.entname LIKE '%室%' OR
                       a.entname LIKE '%企业%'))
         where entstatus = '1'
           and dom is not null
           and dom <> ''
         group by dom
        having count(distinct pripid) > 1 and count(distinct pripid) < 5) ai
 inner join (select distinct dom, pripid
               from enterprisebaseinfocollect_merge_20170417) me
    on ai.dom = me.dom
 where me.pripid is not null
   and me.pripid <> '';
