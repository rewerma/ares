SET datasource.mytest.connector=mysql;
SET datasource.mytest.url='jdbc:mysql://127.0.0.1:3306/mytest?useSSL=false';
SET datasource.mytest.driver=com.mysql.cj.jdbc.Driver;
SET datasource.mytest.user=root;
SET datasource.mytest.password=121212;

CREATE TABLE test1
USING mysql
OPTIONS (
    'datasource' = 'mytest',
    'table_name'='t_user',
    'type' = 'source,sink'
);

CREATE TABLE test2
USING mysql
OPTIONS (
    'datasource' = 'mytest',
    'table_name'='t_user1',
    'type' = 'source,sink'
);

TRUNCATE TABLE test2;
INSERT INTO test2 (id, name, c_time) SELECT id, name, c_time FROM test1 WHERE id > 0 LIMIT 100;

def cnt = 0;
try
    START TRANSACTION;
    for cur in (SELECT * FROM test2 WHERE id > 0 LIMIT 10)
        UPDATE test2 SET name = :cur.name||'_', c_time = :cur.c_time WHERE id = :cur.id;
        cnt = cnt + 1;
        if cnt >= 3
            COMMIT;
            cnt = 0;
        end
    end
    if cnt > 0
        COMMIT;
    end
    END TRANSACTION;
catch
    ROLLBACK;
    END TRANSACTION;
    PUT_LINE(ex.message);
    raise;
end
