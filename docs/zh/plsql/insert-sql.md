# Ares-PL/SQL语法-INSERT SQL语法

## INSERT语法

在定义好[数据源](datasource.md)后，我们就可以使用INSERT语句向数据源中插入数据:

```sql
INSERT INTO table_name (column1, column2,...)
VALUES (value1, value2,...)
```

示例：

```sql
SET datasource.mytest.connector=jdbc;
SET datasource.mytest.url=jdbc:mysql://127.0.0.1:3306/mytest;
SET datasource.mytest.driver=com.mysql.cj.jdbc.Driver;
SET datasource.mytest.user=root;
SET datasource.mytest.password=123456;

CREATE TABLE t_user2_v
WITH (
    'datasource'='mytest',
    'table_name'='t_user2',
    'type' = 'sink'
);

INSERT INTO t_user2_v (id, name, age) VALUES (1, 'Alice', 20);
```

## 联表INSERT语法

在定义好[数据源](datasource.md)后，我们也可以使用INSERT语句向数据源中的目标表插入源表的或SQL语句的结果的数据：

```sql
INSERT INTO table_name 
SELECT column1, column2,...
  FROM source_table_name
  WHERE condition
```

示例：

```sql
SET datasource.mytest.connector=jdbc;
SET datasource.mytest.url=jdbc:mysql://127.0.0.1:3306/mytest;
SET datasource.mytest.driver=com.mysql.cj.jdbc.Driver;
SET datasource.mytest.user=root;
SET datasource.mytest.password=123456;

CREATE TABLE t_user_v
WITH (
    'datasource'='mytest',
    'table_name'='t_user',
    'type' = 'source'
);

CREATE TABLE t_role_v
WITH (
    'datasource'='mytest',
    'table_name'='t_role',
    'type' = 'source'
);

CREATE TABLE t_user2_v
WITH (
    'datasource'='mytest',
    'table_name'='t_user2',
    'type' = 'sink'
);

INSERT INTO t_user2_v (id, name, age) SELECT id, name, age FROM t_user_v WHERE id > 10;

INSERT INTO t_user2_v (id, name, age, role_name) SELECT a.id, a.name, a.age, b.name AS role_name FROM t_user_v a 
    LEFT JOIN t_role_v b ON a.role_id = b.id WHERE a.id > 10;
```