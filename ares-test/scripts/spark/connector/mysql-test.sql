SET datasource.mytest.connector=jdbc;
SET datasource.mytest.url=jdbc:mysql://127.0.0.1:3306/mytest?useSSL=false;
SET datasource.mytest.driver=com.mysql.cj.jdbc.Driver;
SET datasource.mytest.user=root;
SET datasource.mytest.password=121212;

CREATE TABLE test1
WITH (
    'datasource' = 'mytest',
    -- 'query'='select * from t_user',
    'table_name'='t_user',
    'type' = 'source,sink'
);

CREATE TABLE test2
WITH (
    'datasource' = 'mytest',
    'table_name'='t_user1',
    'type' = 'source,sink'
);

-- SELECT * FROM test1 LIMIT 20;
--
-- TRUNCATE TABLE test2;

INSERT INTO test2 (id, name, c_time) VALUES (100, 'Eric', to_timestamp('2024-01-01 12:23:34'));

UPDATE test2 SET name = 'Eric2', c_time = to_timestamp('2024-02-02 12:23:34') WHERE id = 100;

DELETE FROM test2 WHERE id = 100;
--
-- TRUNCATE TABLE test2;
--
INSERT INTO test2 (id, name, c_time) SELECT id, name, c_time FROM test1 WHERE id > 0 LIMIT 100;

UPDATE test2 a, test1 b SET a.name = b.name||'_',
  a.c_time = to_timestamp(date_add(b.c_time, 1)||' '||date_format(b.c_time, 'HH:mm:ss')) WHERE a.id = b.id;

DELETE FROM test2 a, (SELECT * FROM test1 WHERE id>3) b WHERE a.id = b.id;

-- DECLARE
--     a INT:=101;
--     b VARCHAR := 'Hello World';
--     c NUMBER := 3.1415926;
--     d DATE := '2022-01-01';
--     e TIMESTAMP := '2022-01-01 12:23:34';
--     f INT := -1;
-- BEGIN
--     INSERT INTO test2 (id, name, c_time) VALUES (:a, :b, :e);
--
--     SELECT count(1) INTO :f FROM test2 where id = :a;
--
--     IF f = 1 THEN
--        PUT_LINE('Insert successful.');
--     ELSE
--        PUT_LINE('Insert failed.');
--     END IF;
--
--     UPDATE test2 SET name = :b||'_', c_time = to_timestamp(date_add(:e, 3)||' 01:03:04') WHERE id = :a;
--
--     TRUNCATE TABLE test2;
--
--     INSERT INTO test2 (id, name, c_time) SELECT id, name, c_time FROM test1 WHERE id > 0 LIMIT 100;
--
--     FOR cur IN (SELECT * FROM test2 WHERE id > 0 LIMIT 10) LOOP
--         PUT_LINE('id='||cur.id||', name='||cur.name||', c_time='||date_format(cur.c_time, 'yyyy-MM-dd HH:mm:ss'));
--         UPDATE test2 SET name = :cur.name||'_', c_time = :cur.c_time WHERE id = :cur.id;
--     END LOOP;
--
--     DELETE FROM test2 a, (SELECT * FROM test1 WHERE id>0) b WHERE a.id = b.id;
--
-- END;