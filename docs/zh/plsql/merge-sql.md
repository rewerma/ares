# Ares-PL/SQL语法-MERGE SQL语法

## MERGE语法

在定义好[数据源](datasource.md)后，我们就可以使用MERGE语句插入或更新数据源中的数据:

```sql
MERGE INTO table_name
USING source_table
ON (search_condition)
WHEN MATCHED THEN
    UPDATE SET column1 = value1, column2 = value2,...
WHEN NOT MATCHED THEN
    INSERT (column1, column2,...)
    VALUES (value1, value2,...)
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
    'type' = 'source,sink'
);

MERGE INTO t_user2_v a
USING t_user_v b
ON (a.id = b.id)
WHEN MATCHED THEN
    UPDATE SET a.name = b.name, a.age = b.age
WHEN NOT MATCHED THEN
    INSERT (id, name, age)
    VALUES (b.id, b.name, b.age);

MERGE INTO t_user2_v a
USING (SELECT id, name||'_', age FROM t_user_v WHERE id > 10) b
ON (a.id = b.id)
WHEN MATCHED THEN
    UPDATE SET a.name = b.name||'update', a.age = b.age
WHEN NOT MATCHED THEN
    INSERT (id, name, age)
    VALUES (b.id, b.name||'insert', b.age);
```

## 注意事项

- MERGE语法中不能将目标表的字段作为输入字段

- 部分sink端connectors插件不支持MERGE语法中的更新操作或只支持通过主键进行更新，如：`file` connector，`hbase` connector等。（`jdbc` connector支持通过任意字段作为插入更新的条件）。

- 使用MERGE语法时，目标表的类型必须同时配置为`source` 