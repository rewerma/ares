# Ares-PL/SQL语法-数据源

## 数据源定义

通过`CREATE TABLE ... WITH ..`语法定义数据源连接：

```sql
CREATE TABLE t_user2_v
WITH (
    'connector'='jdbc',
    'url'='jdbc:postgresql://127.0.0.1:5432/postgres',
    'driver'='org.postgresql.Driver',
    'user'='root',
    'password'='123456',
    'table_name'='t_user',
    'type' = 'source,sink'
);
```

其中，`connector`参数指定数据源类型需要与connectors中的连接器名称对应，`type`参数指定数据源角色：`source`表示数据源，`sink`表示数据目标（可同时作为数据源和目标），这些均为必填参数。

`url`,`·driver·`, `user`, `password`等参数则对应数据源的连接信息，这些参数在connectors中定义需要与之匹配。

## 数据源公共参数

数据源公共部分可以提取配置在执行参数中：

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
```

数据源的 connector 配置项对应connectors中的连接器名称，connectors的相关配置项参考[连接器配置]()。
