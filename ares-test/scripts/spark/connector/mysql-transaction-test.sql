SET datasource.mytest.connector=mysql;
SET datasource.mytest.url=jdbc:mysql://127.0.0.1:3306/mytest?useSSL=false;
SET datasource.mytest.driver=com.mysql.cj.jdbc.Driver;
SET datasource.mytest.user=root;
SET datasource.mytest.password=121212;

CREATE TABLE test1
WITH (
    'datasource' = 'mytest',
    'table_name'='t_user',
    'type' = 'source,sink'
);

CREATE TABLE test2
WITH (
    'datasource' = 'mytest',
    'table_name'='t_user1',
    'type' = 'source,sink'
);

TRUNCATE TABLE test2;
INSERT INTO test2 (id, name, c_time) SELECT id, name, c_time FROM test1 WHERE id > 0 LIMIT 100;

DECLARE
    cnt INT := 0;
BEGIN
    START TRANSACTION;
    FOR cur IN (SELECT * FROM test2 WHERE id > 0 LIMIT 10) LOOP
        UPDATE test2 SET name = :cur.name||'_', c_time = :cur.c_time WHERE id = :cur.id;
        cnt := cnt + 1;
        IF cnt >= 3 THEN
            COMMIT;
            cnt := 0;
        END IF;
    END LOOP;
    COMMIT;
EXCEPTION
    WHEN ex THEN
        ROLLBACK;
        PUT_LINE(ex.message);
        RAISE;
END;
