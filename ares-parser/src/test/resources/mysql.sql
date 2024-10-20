set spark.master=local[*];

CREATE TABLE test11
    WITH (
--     'datasource' = 'mytest',
        'connector'='jdbc',
        'url'='jdbc:mysql://127.0.0.1:3306/mytest',
        'driver'='com.mysql.cj.jdbc.Driver',
        'user'='root',
        'password'='121212',
--     'table-name'='t_user',
--     'table_path'='t_user',
        'query'='select * from t_user',
--     'partition-column'='id',
--     'num-partitions'='5',
--     'exception-table'='t_user_e',
--     'sql'='select id,name,c_time from t_user where name<>''xxxx'' ',
        'type' = 'source'
        );

CREATE TABLE test13
    WITH (
--     'datasource' = 'mytest',
        'connector'='jdbc',
        'url'='jdbc:mysql://127.0.0.1:3306/mytest',
        'driver'='com.mysql.cj.jdbc.Driver',
        'user'='root',
        'password'='121212',
        'table_path'='t_user',
--         'query'='select * from t_user3',
        'type' = 'source'
        );

CREATE TABLE test12
    WITH (
--     'datasource' = 'mytest',
        'connector'='jdbc',
        'url'='jdbc:mysql://127.0.0.1:3306/mytest',
        'driver'='com.mysql.cj.jdbc.Driver',
        'user'='root',
        'password'='121212',
        'target_table'='t_user0',
--     'database'='mytest',
--     'table-name'='t_user2',
--     'query'='select * from t_user',
--     'partition-column'='id',
--     'num-partitions'='5',
--     'exception-table'='t_user_e',
--     'sql'='select id,name,c_time from t_user where name<>''xxxx'' ',
        'type' = 'sink'
        );

-- select * from test11;

-- (id,name,c_time)
-- insert into test12  select id,name||'_',c_time from test11;


-- insert into test12 (id,name)  select * from (select id,name from test11);
declare
  a INT := 3;
  b VARCHAR;
begin
-- insert into test12 (id,name,c_time) values (:a,:b,cast('2024-05-02 12:23:34' as timestamp)),(2,:b,cast('2024-05-12 10:23:34' as timestamp));

-- b := 'xvv_'||cast(power(a,2) as int);

-- update test12 a, test11 b set a.name=b.name where a.id=b.id;

-- delete from test12 a, test11 b where a.id=b.id;
  select id,name from test11 where name=:b order by id desc;
-- select id from test11;
end;

-- rule on column test1.name is 'not null';
-- rule on column test1.name is 'regex(^3x*)';
-- rule on column test1.balance is 'scale(2)';
-- rule on column test1.name is 'len(6,10)';
-- rule on column test1.id is 'range(1,10)';
-- rule on column test1.name is 'len(5,10)';

-- CREATE TABLE t_group
-- WITH (
--     'datasource' = 'mytest',
--     'table-name'='t_group',
--     'type' = 'source,sink'
-- );
-- select * from test1;
--
-- select * from test1_exp;

-- CREATE TABLE test2
-- WITH (
--     'datasource' = 'mytest',
--     'batch-size' = '2',
--     'table-name'='t_user2',
--     'type' = 'source,sink'
-- );



-- CREATE TABLE test4
-- (
--     id BIGINT,
--     name VARCHAR,
--     c_time TIMESTAMP
-- )
--     WITH (
--         'connector'='jdbc',
--         'url'='jdbc:oracle:thin:@localhost:1521:XE',
--         'username'='mytest',
--         'password'='m121212',
--         'table-name'='t_user',
--         'type' = 'source,sink'
--         );
--
-- -- select * from test1;
--
-- delete from test4;
--
-- insert into test4 (id,name,c_time) select id,name,c_time from test1;


-- select * from test1 where id=1;

-- select  a.*, coalesce(b.groups, '[]') as groups from test1 a left join
-- (
-- select b.user_id,to_json(collect_list(struct(b.group_name))) as groups
-- from  t_group b  group by b.user_id
-- ) b on a.id=b.user_id order by a.id;

-- insert into test2 select id,name,11 as age ,c_time from test1;


-- create procedure  ttt(p1 in int, p2 in varchar, p3 out varchar) as
--   d1 INT:=1;
--   d2 VARCHAR:='Eric';
-- begin
--   d1 := d1+p1;
--   d2 := d2||'_'||p2;
--   p3 := d2;
--   select :d1 as id ,:d2 as name;
-- end;
-- /
-- create procedure  ttt2(p1 in int, p2 in varchar) as
--   d1 INT:=2;
--   d2 VARCHAR:='Andy';
-- begin
-- --   d1 := d1+p1;
-- --   d2 := d2||'_'||p2;
-- --   select :d1 as id ,:d2 as name;
--   ttt(p1, p2, d2);
--   select :d2 as name;
-- --   select * from test1 where id=:d1 and name!=:p2;
-- end;
-- /
-- -- call ttt(2,'xxx');
-- call ttt2(3,'fff');

-- truncate table  test2;

-- declare
--   i int:=1;
--   aa number:=6;
--   bb varchar:='_x';
--   cc timestamp:=to_date('2024-01-01 12:23:34', 'yyyy-MM-dd HH:mm:ss');
-- begin
--   create table test1 as select * from test11;
--   repartition(5);

--   WHILE i <= 5 LOOP
--     select :i as idx;
--     i := i + 1;
--   END LOOP;

--   FOR idx IN 1..aa LOOP
--     select :idx as idx;
--   END LOOP;

--   println('xxxvvv'||1);
--   delete from test2;
--
--     WHILE i <= 11 LOOP
--       println('xxx ' || i);
--       i := i + 1;
--       if i=6 then
--          exit;
--       end if;
--     END LOOP;

--   FOR cursor_1 IN (select * from test1 where id>0 limit 100) LOOP
--     select :cursor_1.id as id, :cursor_1.name as name;
--     DBMS_OUTPUT.PUT_LINE(cursor_1.name||'__');
--     println(cursor_1.name||'__');
--      if cursor_1.id<=3 then
--        dbms_output.put_line('ID less than 3 ' || cursor_1.id);
--      elsif cursor_1.id=4 then
--       println('ID equals 4 ' || cursor_1.id);
--      else
--        println('ID great than 4 ' || cursor_1.id);
--      end if;

--     i := 1;
--     WHILE i <= cursor_1.id LOOP
--       println('xxx ' || cursor_1.id);
--       i := i + 1;
--     END LOOP;

--     FOR j IN 1..cursor_1.id LOOP
-- --       println('xxx ' || cursor_1.id || ' ' || j);
-- --       if cursor_1.id=j then
--          println('current idx: '||j);
--          if j=5 then
--             exit;
--          end if;
-- --       end if;
--     END LOOP;

--     insert into test2 values (:cursor_1.id, :cursor_1.name, :cursor_1.id + 20, :cursor_1.c_time);
--   END LOOP;

--   delete from test2;
--   select   into :cc;
--   select :cc as ct;
--   select name,c_time into :bb,:cc from test1 where id=:aa;
--   select :bb as name, :cc as c_time;
--    bb := 'xxx' || bb;
--   insert into test2 (id,name,c_time) values(16, :bb, :cc);
--   insert into test2 (id,name,c_time) select id,name||:bb,c_time from test1;

--   select * from test2;
--     cc := to_date('2024-01-11 11:23:34', 'yyyy-MM-dd HH:mm:ss');
--       update test2 set name='vvv_', c_time=:cc where id=:aa;
--     update test2 a, (select id,name, c_time from test1) b set a.name=b.name,a.age=:aa, a.c_time=b.c_time where a.id=b.id;
--     delete from test2 where id=11 and name<>:bb;
--     delete from test2  where exists (select 1 from test1 b where b.id=id) and age=abs(:aa);

-- merge into test2 tu2
-- using (select * from test1) tu
-- on (tu2.id=tu.id)
-- when not matched then
--     insert (tu2.id, tu2.name, tu2.age, tu2.c_time)
--         values (tu.id, tu.name, :aa, tu.c_time)
-- when matched then
--     update set tu2.name = tu.name, tu2.age = :aa, tu2.c_time = tu.c_time;

--   if aa<3 then
--     select * from test1 where id=:aa;
--   elsif aa=3 then
--     select * from test1 where id=3;
--   elsif aa>3 and aa<10 then
--     select * from test1 where id=5;
--   else
--     select * from test1 where id=11;
--   end if;
--   aa := power(aa, 2);·
--   bb := 'Eri';
--   select * from test1 where id=:aa and name=:bb;
--   select * from test1 where id=:aa-8 and name=:bb||'c';
-- end;



-- -- delete from test2;
--
-- -- CREATE TABLE tt_user2
-- -- (
-- --     id BIGINT,
-- --     name VARCHAR,
-- --     age INT,
-- --     c_time TIMESTAMP
-- -- )
-- -- WITH (
-- --     'connector'='jdbc',
-- --     'url'='jdbc:postgresql://localhost:5432/postgres',
-- --     'driver'='org.postgresql.Driver',
-- --     'username'='postgres',
-- --     'password'='password',
-- --     'table-name'='t_user2',
-- --     'type' = 'sink'
-- --     );
--
-- -- truncate table tt_user2;
--
-- -- insert into test2 select id, name, 1 as age, c_time from test1 where id>1000;
-- -- UPDATE test2 a, test1 b SET a.name=b.name, a.c_time=b.c_time WHERE a.id=b.id and b.id>1000;
-- create or replace procedure mytest2 AS
-- BEGIN
-- -- DELETE FROM test2 a WHERE EXISTS (SELECT 1 FROM test1 b WHERE a.id=b.id and b.id>1) and a.name='xxx';
-- update test2 a, (select  * from test1 where name!='fxx') b set a.name=b.name,a.c_time=b.c_time where a.id=b.id and b.name<>'sss';
-- END;
-- call mytest2();

-- select '$abc' as t2;
--
-- delete from test2 where name='$xxx';

-- CREATE TABLE tt_user2
-- (
--     id BIGINT,
--     name VARCHAR,
--     age INT,
--     c_time TIMESTAMP
-- )
-- WITH (
--     'connector'='jdbc',
--     'url'='jdbc:oracle:thin:@localhost:1521:XE',
--     'username'='mytest',
--     'password'='m121212',
--     'table-name'='t_user2',
--     'type' = 'sink'
-- );

-- select * from tt_user;

-- CREATE TABLE tt_user6
-- (
--     id INT,
--     name VARCHAR,
--     age INT,
--     c_time TIMESTAMP
-- )
--     WITH (
--     'connector'='jdbc',
--     'url'='jdbc:mysql://127.0.0.1:3306/mytest',
--     'driver'='com.mysql.jdbc.Driver',
--     'username'='root',
--     'password'='121212',
-- --     'datasource' = 'mytest',
-- --     'table-name'='t_user',
--     'sql'='select id,name,age,c_time from t_user6',
-- --     'fetch-size'='-2147483648',
--     'type' = 'source'
-- );



-- create table test1 as select leaf_seg('test1') as seq, id, name, c_time from tt_user;
-- repartition(1);

-- select * from test1 order by seq asc;
-- truncate table tt_user2;
-- insert into tt_user2 select leaf_seg('test1') as id, name,age, c_time from tt_user;
-- repartition(5,'id');


-- select name,count(1) cnt from tt_user6 group by name ;

-- select * from tt_user;

-- select a.id,a.name,b.id as id2,b.name as name2 from
-- (SELECT id, name, ROW_NUMBER() OVER (ORDER BY id desc) as rownum
-- FROM tt_user) a full join
-- (SELECT id, name, ROW_NUMBER() OVER (ORDER BY id asc) as rownum
--  FROM tt_user where id>1)  b on a.rownum=b.rownum
-- ;

-- select  name,count(1) as cnt from tt_user group by name;

-- create or replace function add_numbers(a number, b number) return number
-- is
--   user_id number;
--   result number;
-- begin
--   select id into :user_id from tt_user where name='Eric' limit 1;
--   result := a + b + user_id;
-- return result;
-- end;
-- /

-- create procedure  ttt(p1 in int, p2 in varchar ) as
--   d1 INT:=1;
--   d2 VARCHAR;
-- begin
--   println(p1 || '_' || p2);
-- end;
--
-- CREATE OR REPLACE PROCEDURE test (p1 IN INT, xx OUT INT, yy OUT VARCHAR) AS
--     aa NUMBER :=2;
--     test VARCHAR:='"xxxx"';
--     i INT;
-- BEGIN
-- --   test := '"xsss"';
--   test := db_type('tt_user');
--
--   select * from tt_user where name=:test;
--   select id,name into :aa,:test from tt_user where name <> :test limit 1;
--   truncate table tt_user2;
--   insert into tt_user2 (id,name,c_time) values (4, '"Test"', now());
--   insert into tt_user2 (id,name,c_time) select 1,'"xxx"',current_timestamp ;
--   insert into tt_user2 (id,name,c_time) select /*+ mapjoin(tt_user)*/ id,name,c_time from tt_user;
--   update tt_user2 set name='"TTT"',c_time=now() where id=1;
--   update tt_user2 a, tt_user b set a.name=b.name,a.c_time=b.c_time where a.id=b.id and a.name<>'"sss"';
--   delete from tt_user2 where name = '"Eric"';
--   merge into tt_user2 tu2
--     using (select * from tt_user where name<>'"XXX"') tu
--     on (tu2.id=tu.id)
--     when not matched then
--         insert (tu2.id, tu2.name, tu2.age, tu2.c_time)
--             values (tu.id, tu.name, tu.id+20, tu.c_time)
--     when matched then
--         update set tu2.name = tu.name, tu2.age = tu.id+20, tu2.c_time = tu.c_time;

-- insert into tt_user2 (id,name,c_time) values (1,:test.name,null);

-- select * from tt_user;

--  update tt_user2 a, (select id,name,c_time from tt_user) b set a.name=b.name, a.c_time=b.c_time where a.id=b.id ;

-- update tt_user2 a, (select 1 as id,'XXXX' as name,null as c_time) b set a.name=b.name, a.c_time=b.c_time where a.id=b.id ;

--     insert into tt_user2 (id,name,c_time) select id,name,c_time from tt_user where id>1;
--   delete from tt_user2 a where exists (select 1 from tt_user b where a.id=b.id and b.name<>'sssfff') ;
--   delete from tt_user2 where id=:aa;

--   FOR cursor_1 IN (select * from tt_user where id>0) LOOP
--     delete from tt_user2 a where exists (select 1 from tt_user b where a.id=:cursor_1.id) and a.name=:cursor_1.name;
--       DBMS_OUTPUT.PUT_LINE('xxxx ' || cursor_1.name);
--     insert into tt_user2 (id,name,c_time) values (:cursor_1.id,:cursor_1.name,:cursor_1.c_time);
--   select id,name from tt_user where id=:cursor_1.id;
--     select id,name into :aa,:test from tt_user where id=:cursor_1.id;
--     println(aa||'_'||test);
--     insert into tt_user2 (id,name,c_time) select id,name,c_time from tt_user where id=:cursor_1.id;
--     update tt_user2 set name=:cursor_1.name,c_time=:cursor_1.c_time where id=:cursor_1.id;
--     update tt_user2 a, (select id,name,c_time from tt_user where id=:cursor_1.id) b set a.name=b.name,a.c_time=b.c_time where a.id=b.id ;
--     delete from tt_user2 where id=:cursor_1.id;
--   merge into tt_user2 tu2
--     using (select * from tt_user where id=:cursor_1.id) tu
--     on (tu2.id=tu.id)
--     when not matched then
--         insert (tu2.id, tu2.name, tu2.age, tu2.c_time)
--             values (tu.id, :cursor_1.name, tu.id+20, tu.c_time)
--     when matched then
--         update set tu2.name = :cursor_1.name, tu2.age = tu.id+20, tu2.c_time = tu.c_time;

--     aa := cursor_1.id;
--     test := cursor_1.name;
--     ttt(aa, test);
--   END LOOP;

--   FOR i IN 1..p1 LOOP
--     if mod(i, 3) = 0 then
--       DBMS_OUTPUT.PUT_LINE('Iteration3 ' || i);
--       EXIT;
--     else
--       DBMS_OUTPUT.PUT_LINE('Iteration ' || i);
--     end if;
--   END LOOP;
--
--   select * from tt_user;
--   i := 1;
--   WHILE i <= p1 LOOP
--     if mod(i, 3) = 0 then
--       DBMS_OUTPUT.PUT_LINE('Iteration3 ' || i);
--       EXIT;
--     else
--       DBMS_OUTPUT.PUT_LINE('Iteration ' || i);
--     end if;
--     i := i + 1;
--   END LOOP;

-- select * from tt_user where id<=10;

-- aa := add_numbers(aa, 3);
--
-- println(aa);
-- aa := aa + 1;
--

-- delete from tt_user2;
-- insert into tt_user2 (id,name,c_time) select * from tt_user;
--
-- execute('mytest','delete from t_user2');

-- insert into tt_user2 value (1,'aa',null,null),(2,'b',null,null);

-- update tt_user2 a, tt_user b set a.age=b.age, a.c_time=b.c_time where id=1;
-- delete from tt_user2 0where id=1;

-- create table tt_user1 as
-- select * from tt_user ;
-- withRepartition(10,'id');
-- withCache();

-- select * from tt_user1 where id=:aa;

-- if aa>2 and p1 is not null then
--   println('println if');
--   create table abc as select id,name from tt_user where id>1;
-- elsif p1='abc' then
--   println('println elsif');
-- else
--   dbms_output.put_line('println else');
-- end if;

-- select * from tt_user;

-- select id into :aa from tt_user where name='Eric';
--
-- delete from tt_user2 where id=:aa;
--
-- insert into tt_user2 values (1, 'Alex', :aa, STR_TO_DATE('2023-08-20 15:30:00', '%Y-%m-%d %H:%i:%s'));
-- --
-- -- truncate table tt_user2;
-- --
-- insert into tt_user2 (id,name,age,c_time) select id,name,id+23 as age,c_time from tt_user where id>0;
--
-- select * from tt_user2;
--
-- update tt_user2 set name='Eric' where id=1;
--
-- update tt_user2 a, tt_user b
-- set a.name=b.name, a.age=b.id+20,a.c_time=b.c_time,a.id=a.id+1;
-- where a.id=b.id;


--
-- create table tuser as select id,name, id+30 as age, c_time from tt_user where id<3;
--
-- repartition(1, 'id, name');
--
--
-- --
-- truncate table tt_user2;
--
-- insert into tt_user2 select * from tuser;
--
-- merge into tt_user2 tu2
-- using (select * from tt_user) tu
-- on (tu2.id=tu.id)
-- when not matched then
--     insert (tu2.id, tu2.name, tu2.age, tu2.c_time)
--         values (tu.id, tu.name, tu.id+20, tu.c_time)
-- when matched then
--     update set tu2.name = tu.name, tu2.age = tu.id+20, tu2.c_time = tu.c_time;


-- execute('mytest', 'insert into "t_user2" (id,name,c_time) values (10,''Eric'',null)');

-- println(p1);
--
-- -- xx := p1;
--
-- select 3,'ss' into :xx,:yy;
--
-- -- println(xx);
--
-- END;
-- /

-- -- declare
-- --  x1 int;
-- --  x2 varchar;
-- create procedure test2 IS
--   x1 int;
--   x2 varchar;
-- begin
--   test(x1, x1, x2);
--   println(x1 || '_' || x2);
-- end;
-- /
--
-- declare
--   xx1 int:=1;
--   xx2 varchar:='asdf';
-- begin
--   xx1 := (xx1 + 4) * 3;
--   if mod(xx1, 3) = 0 then
--     println(xx1||'__'||xx2);
--   else
--     println(xx1||'--'||xx2);
--   end if;
--
-- --   test2();
-- --   test(4, xx1, xx2);
-- --   println(xx1||'__'||xx2);
-- end;

