# Ares-PL/SQL语法-UPDATE SQL语法

## UPDATE语法

在定义好[数据源](datasource.md)后，我们就可以使用UPDATE语句更新数据源中的数据:

```sql
UPDATE table_name SET column1 = value1, column2 = value2,... WHERE condition;
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

UPDATE t_user2_v SET name = 'Tom', age = 25 WHERE id = 1;
```

## 联表UPDATE语法

在定义好[数据源](datasource.md)后，我们也可以使用INSERT语句向数据源中的目标表插入源表的或SQL语句的结果的数据：

```sql
UPDATE table_name a, table_name2 b SET a.column1 = b.column1, a.column2 = b.column2,... WHERE a.column3 = b.column3;
```

```sql
UPDATE table_name a, (SELECT * FROM table_name2) b SET a.column1 = b.column1, a.column2 = b.column2,... WHERE a.column3 = b.column3;
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

CREATE TABLE t_user2_v
WITH (
    'datasource'='mytest',
    'table_name'='t_user2',
    'type' = 'sink'
);

UPDATE t_user2_v a, t_user_v b SET a.name = b.name, a.age = b.age WHERE a.id = b.id;

UPDATE t_user2_v a, (SELECT * FROM t_user_v wher id > 10) b SET a.name = b.name, a.age = b.age WHERE a.id = b.id;
```

## 注意事项

- 联表UPDATE语法中不能将目标表的字段作为输入字段，如：
```sql
UPDATE t_user2_v a, t_user_v b SET a.name = a.name || b.name, a.age = b.age WHERE a.id = a.id + b.id;
```

- 部分sink端connectors插件不支持UPDATE语法或只支持通过主键进行UPDATE，如：`file` connector，`hbase` connector等。（`jdbc` connector支持通过任意字段进行UPDATE）。