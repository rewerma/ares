# Ares-PL/SQL语法-SELECT SQL语法

## SELECT语法

在定义好[数据源](datasource.md)后，我们就可以使用SELECT语句（跨源）查询、统计、分析数据源的数据:

```sql
SELECT column1, column2,...
  FROM table_name
  WHERE condition
  GROUP BY column1, column2,...
```

示例：

```sql
SET datasource.mytest.connector=jdbc;
SET datasource.mytest.url=jdbc:mysql://127.0.0.1:3306/mytest;
SET datasource.mytest.driver=com.mysql.cj.jdbc.Driver;
SET datasource.mytest.user=root;
SET datasource.mytest.password=123456;
   
SET datasource.pg_test.connector=jdbc;
SET datasource.pg_test.url=jdbc:postgresql://127.0.0.1:5432/postgres;
SET datasource.pg_test.driver=org.postgresql.Driver;
SET datasource.pg_test.user=postgres;
SET datasource.pg_test.password=password;

CREATE TABLE t_user_v
WITH (
    'datasource'='mytest',
    'table_name'='t_user',
    'type' = 'source'
);

CREATE TABLE t_group_v
WITH (
    'datasource'='pg_test',
    'table_name'='t_group',
    'type' = 'source'
);

SELECT id, name, age FROM (
    SELECT id, name, age, ROW_NUMBER() OVER (PARTITION BY name ORDER BY id DESC) AS rn
    FROM t_user_v
) WHERE rn = 1;

SELECT b.group_name, count(b.group_name) as cnt FROM t_user_v a
    LEFT JOIN t_group_v b ON a.group_id = b.id
    WHERE a.age > 35 GROUP BY b.group_name;
```

查询结果将会在控制台打印输出前`100`行数据。

## SELECT变量赋值语法

在[过程语句]()中，可以通过SELECT语句将查询结果赋值给变量：

```sql
SET datasource.mytest.connector=jdbc
SET datasource.mytest.url=jdbc:mysql://127.0.0.1:3306/mytest
SET datasource.mytest.driver=com.mysql.cj.jdbc.Driver
SET datasource.mytest.user=root
SET datasource.mytest.password=123456


CREATE TABLE t_user_v
WITH (
    'datasource'='mytest',
    'table_name'='t_user',
    'type' = 'source'
);

DECLARE 
  v_name VARCHAR;
  v_age INT;
BEGIN
  SELECT name, age INTO :v_name, :v_age FROM t_user_v WHERE id = 1;
  PUT_LINE('Name: '||v_name||', Age: '||v_age);
END;
```
**注意事项**：在SQL中如果使用变量，必须使用冒号`:`作为前缀，例如`:v_name`。