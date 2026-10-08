-- hive catalog 与 USING hive 相同：Spark 集群已整合 Hive，直接查询 database.table。
-- 行级变更要求 Paimon 表有主键。单表 UPDATE、DELETE 保持原语句，关联表改写成 MERGE。
CREATE TABLE test1
USING paimon
OPTIONS (
    'metastore' = 'hive',
    'table_name' = 'default.t_user',
    'type' = 'source'
);

CREATE TABLE test2
USING paimon
OPTIONS (
    'metastore' = 'hive',
    'table_name' = 'default.t_user4',
    'type' = 'sink,source'
);

truncate table test2;
insert into test2 select id, name, c_time from test1;
update test2 set name = 'updated' where id > 0;
delete from test2 where id < 0;
update test2 a, test1 b set a.name = b.name where a.id = b.id;
delete from test2 a, test1 b where a.id = b.id and a.id < 0;
merge into test2 t
using test1 s
on t.id = s.id
when matched then update set t.name = s.name
when not matched then insert (t.id, t.name, t.c_time) values (s.id, s.name, s.c_time);
select * from test2;
