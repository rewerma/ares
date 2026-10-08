# Ares-PL/SQL语法-数据源

## 数据源定义

通过`CREATE TABLE ... USING ... OPTIONS (...)`定义数据源连接。`USING`后面是连接器名称，需要与 connectors 中的插件名对应。

```sql
CREATE TABLE t_user2_v
USING mysql
OPTIONS (
    'url'='jdbc:mysql://127.0.0.1:3306/mytest',
    'driver'='com.mysql.cj.jdbc.Driver',
    'user'='root',
    'password'='123456',
    'table_name'='t_user',
    'type' = 'source,sink'
);
```

`type`指定这张表的角色：`source`表示数据源，`sink`表示数据目标，`source,sink`表示两者都是。不写`type`时，同时作为数据源和数据目标。

`url`、`driver`、`user`、`password`等参数对应数据源的连接信息，需要与 connectors 中的定义匹配。`CREATE TABLE ... USING`只能写在脚本顶层。

## 数据源公共参数

数据源公共部分可以提取配置在执行参数中：

```sql
SET datasource.mytest.connector=mysql;
SET datasource.mytest.url='jdbc:mysql://127.0.0.1:3306/mytest';
SET datasource.mytest.driver=com.mysql.cj.jdbc.Driver;
SET datasource.mytest.user=root;
SET datasource.mytest.password=123456;

CREATE TABLE t_user_v
USING mysql
OPTIONS (
    'datasource'='mytest',
    'table_name'='t_user',
    'type' = 'source'
);

CREATE TABLE t_user2_v
USING mysql
OPTIONS (
    'datasource'='mytest',
    'table_name'='t_user2',
    'type' = 'sink'
);
```

数据源的 connector 配置项对应connectors中的连接器名称，connectors的相关配置项参考[连接器配置]()。

## JDBC

`USING` 后面的名字与连接器名称一致，不区分大小写。驱动在发行包的 `lib` 目录，连接器在 `connectors` 目录。

| 连接器 | driver | url |
| --- | --- | --- |
| `mysql` | `com.mysql.cj.jdbc.Driver` | `jdbc:mysql://主机:端口/库名` |
| `oracle` | `oracle.jdbc.OracleDriver` | `jdbc:oracle:thin:@主机:端口:SID` |
| `postgres` | `org.postgresql.Driver` | `jdbc:postgresql://主机:端口/库名` |
| `sqlserver` | `com.microsoft.sqlserver.jdbc.SQLServerDriver` | `jdbc:sqlserver://主机:端口;databaseName=库名` |
| `dameng` | `dm.jdbc.driver.DmDriver` | `jdbc:dm://主机:端口` |
| `kingbase` | `com.kingbase8.Driver` | `jdbc:kingbase8://主机:端口/库名` |
| `oceanbase` | `com.oceanbase.jdbc.Driver` | `jdbc:oceanbase://主机:端口/库名` |
| `opengauss` | `org.opengauss.Driver` | `jdbc:opengauss://主机:端口/库名` |
| `starrocks` | `com.mysql.cj.jdbc.Driver` | `jdbc:mysql://FE主机:9030/库名` |
| `doris` | `com.mysql.cj.jdbc.Driver` | `jdbc:mysql://FE主机:9030/库名` |

`table_name` 在 `mysql`、`starrocks`、`doris`，以及 `oceanbase` 的 MySQL 模式里，写成表名或 `库名.表名`。在 `oracle`、`sqlserver`、`postgres`、`dameng`、`kingbase`、`opengauss`，以及 `oceanbase` 的 Oracle 模式里，写成 `schema.表名`。

Kingbase 使用 KingbaseES V8 的 `kingbase8` 驱动，地址也可以写成 `jdbc:kingbase://`。

```sql
SET datasource.kb.connector=kingbase;
SET datasource.kb.url='jdbc:kingbase8://127.0.0.1:54321/test';
SET datasource.kb.driver=com.kingbase8.Driver;
SET datasource.kb.user=system;
SET datasource.kb.password=123456;

CREATE TABLE t_user_v
USING kingbase
OPTIONS (
    'datasource'='kb',
    'table_name'='public.t_user',
    'type' = 'source,sink'
);
```

OceanBase 用 `compatible_mode` 区分模式，取值是 `mysql` 或 `oracle`。不写时按 MySQL 模式连接。

```sql
SET datasource.ob.connector=oceanbase;
SET datasource.ob.url='jdbc:oceanbase://127.0.0.1:2881/test';
SET datasource.ob.driver=com.oceanbase.jdbc.Driver;
SET datasource.ob.user=root;
SET datasource.ob.password=123456;
SET datasource.ob.compatible_mode=mysql;

CREATE TABLE t_user_v
USING oceanbase
OPTIONS (
    'datasource'='ob',
    'table_name'='t_user',
    'type' = 'source,sink'
);
```

Oracle 模式把 `compatible_mode` 写成 `oracle`，`table_name` 写成 `SCHEMA.T_USER`。

OpenGauss 的类型和标识符按 PostgreSQL 处理。

```sql
SET datasource.og.connector=opengauss;
SET datasource.og.url='jdbc:opengauss://127.0.0.1:5432/postgres';
SET datasource.og.driver=org.opengauss.Driver;
SET datasource.og.user=gaussdb;
SET datasource.og.password=123456;

CREATE TABLE t_user_v
USING opengauss
OPTIONS (
    'datasource'='og',
    'table_name'='public.t_user',
    'type' = 'source,sink'
);
```

## StarRocks

`USING starrocks` 用 `mode` 选择读写方式，取值是 `jdbc` 或 `febe`。不写 `mode` 时走 JDBC。驱动使用发行包 `lib` 里的 MySQL 驱动 `com.mysql.cj.jdbc.Driver`。`table_name` 写成 `库名.表名`。

### jdbc

查询端口一般是 `9030`。`SELECT` 走 MySQL 协议，`INSERT`、`UPDATE`、`DELETE`、`TRUNCATE` 也走 JDBC。主键模型的 `INSERT` 按 StarRocks 的语义覆盖相同主键。

```sql
SET datasource.sr.connector=starrocks;
SET datasource.sr.mode=jdbc;
SET datasource.sr.url='jdbc:mysql://127.0.0.1:9030/demo';
SET datasource.sr.driver=com.mysql.cj.jdbc.Driver;
SET datasource.sr.user=root;
SET datasource.sr.password='';
SET datasource.sr.query_timeout_sec=3600;

CREATE TABLE t_user_v
USING starrocks
OPTIONS (
    'datasource'='sr',
    'table_name'='demo.t_user',
    'type' = 'source,sink'
);
```

全表扫描如果碰到默认 `query_timeout`，把 `query_timeout_sec` 调大。`-1` 表示不改会话超时。

### febe

`febe` 读数据时先向 FE 要查询计划，再从 BE 拉 Arrow 批次。`INSERT` 走 FE 的 Stream Load，由 FE 转到 BE。`fe_nodes` 写 FE 的 HTTP 地址，多个地址用逗号分隔，例如 `fe1:8030,fe2:8030`。也可以写成 `http://fe1:8030`。BE 地址由查询计划返回，不用单独配置。

`UPDATE`、`DELETE`、`TRUNCATE` 仍然走 JDBC，所以这三种语句还要配置 `url`。自定义 `query` 也要配置 `url`，用来读取结果列类型。只做 Stream Load、并且表结构可以从 FE 的 `/_schema` 读到时，可以不写 `url`。

下面是一份读写都走 febe 的完整配置。连接信息放在 `SET` 里，扫描参数写在 source 表上，Stream Load 参数写在 sink 表上。

```sql
SET datasource.sr.connector=starrocks;
SET datasource.sr.mode=febe;
SET datasource.sr.fe_nodes='fe1:8030,fe2:8030,fe3:8030';
SET datasource.sr.url='jdbc:mysql://fe1:9030/demo';
SET datasource.sr.driver=com.mysql.cj.jdbc.Driver;
SET datasource.sr.user=root;
SET datasource.sr.password='';
SET datasource.sr.query_timeout_sec=3600;
SET datasource.sr.max_retries=3;

CREATE TABLE t_user_v
USING starrocks
OPTIONS (
    'datasource'='sr',
    'table_name'='demo.t_user',
    'scan_filter'='id > 0',
    'request_tablet_size'='100',
    'scan_batch_rows'='1024',
    'scan_connect_timeout_ms'='30000',
    'scan_keep_alive_min'='10',
    'scan_mem_limit'='1073741824',
    'http_socket_timeout_ms'='180000',
    'type' = 'source'
);

CREATE TABLE t_user_sink
USING starrocks
OPTIONS (
    'datasource'='sr',
    'table_name'='demo.t_user_sink',
    'batch_size'='1000',
    'batch_max_bytes'='10485760',
    'partial_update'='false',
    'stream_load_props'='{"max_filter_ratio":"0.1"}',
    'type' = 'sink'
);

SELECT id, name FROM t_user_v;

INSERT INTO t_user_sink
SELECT id, name FROM t_user_v;
```

`request_tablet_size` 控制每个扫描分片包含的 tablet 数，不写时不拆开。`scan_filter` 是不带 `WHERE` 的过滤条件，作用和 JDBC 的 `where_condition` 相同。`scan_mem_limit` 和 `batch_max_bytes` 的单位是字节，上面分别是 1GB 和 10MB。`query_timeout_sec` 同时用作 BE 扫描和 Stream Load 的超时；febe 下写成 `-1` 时按 3600 秒处理。

主键表只更新部分列时，把 sink 的 `'partial_update'` 写成 `'true'`。`stream_load_props` 是额外的 Stream Load 请求头，写成 JSON 对象。

自定义查询要带上 `url`：

```sql
CREATE TABLE t_user_q
USING starrocks
OPTIONS (
    'datasource'='sr',
    'table_name'='demo.t_user',
    'query'='SELECT id, name FROM demo.t_user WHERE id > 10',
    'type' = 'source'
);
```

`UPDATE`、`DELETE`、`TRUNCATE` 使用同一套 `url`、`user`、`password`，语句本身和 JDBC 模式相同。

## Doris

`USING doris` 同样用 `mode` 选择 `jdbc` 或 `febe`。不写 `mode` 时走 JDBC。驱动是发行包 `lib` 里的 `com.mysql.cj.jdbc.Driver`，查询端口一般是 `9030`。`table_name` 写成 `库名.表名`。

`jdbc` 模式下 `SELECT`、`INSERT`、`UPDATE`、`DELETE`、`TRUNCATE` 都走 MySQL 协议。Unique Key 表的 `INSERT` 按 Doris 的语义覆盖相同主键。

`febe` 读数据时先向 FE 要查询计划，再从 BE 拉 Arrow 批次。`INSERT` 走 FE 的 Stream Load。`fe_nodes` 写 FE 的 HTTP 地址，多个地址用逗号分隔，例如 `fe1:8030,fe2:8030`。BE 地址由查询计划返回。`UPDATE`、`DELETE`、`TRUNCATE` 以及自定义 `query` 仍然走 JDBC，所以还要配置 `url`。只做 Stream Load、并且表结构可以从 FE 读到时，可以不写 `url`。

Unique Key 表的删除行在 Stream Load 里写成隐藏列 `__DORIS_DELETE_SIGN__`。部分列更新把 `'partial_update'` 写成 `'true'`，请求头使用 `partial_columns`。

```sql
SET datasource.doris.connector=doris;
SET datasource.doris.mode=febe;
SET datasource.doris.fe_nodes='fe1:8030,fe2:8030,fe3:8030';
SET datasource.doris.url='jdbc:mysql://fe1:9030/demo';
SET datasource.doris.driver=com.mysql.cj.jdbc.Driver;
SET datasource.doris.user=root;
SET datasource.doris.password='';
SET datasource.doris.query_timeout_sec=3600;
SET datasource.doris.max_retries=3;

CREATE TABLE t_user_v
USING doris
OPTIONS (
    'datasource'='doris',
    'table_name'='demo.t_user',
    'scan_filter'='id > 0',
    'request_tablet_size'='100',
    'scan_batch_rows'='1024',
    'scan_connect_timeout_ms'='30000',
    'scan_keep_alive_min'='10',
    'scan_mem_limit'='1073741824',
    'http_socket_timeout_ms'='180000',
    'type' = 'source'
);

CREATE TABLE t_user_sink
USING doris
OPTIONS (
    'datasource'='doris',
    'table_name'='demo.t_user_sink',
    'batch_size'='1000',
    'batch_max_bytes'='10485760',
    'partial_update'='false',
    'stream_load_props'='{"max_filter_ratio":"0.1"}',
    'type' = 'sink'
);

SELECT id, name FROM t_user_v;

INSERT INTO t_user_sink
SELECT id, name FROM t_user_v;
```

`scan_mem_limit` 和 `batch_max_bytes` 的单位是字节。`query_timeout_sec` 同时用作 BE 扫描和 Stream Load 的超时；febe 下写成 `-1` 时按 3600 秒处理。JDBC 模式下 `-1` 表示不改会话超时。

## Hive

Spark 集群已经整合 Hive，不再使用独立的 Hive connector。`USING hive`（`hive3` 与 `hive` 相同）会创建视图，直接查询 Hive 表。不需要配置 `metastore_uri`。

```sql
CREATE TABLE t_user_v
USING hive
OPTIONS (
    'table_name'='default.t_user',
    'type' = 'source'
);

SELECT * FROM t_user_v;
```

上面的语句等价于创建视图 `SELECT * FROM default.t_user`。`table_name` 写成 `数据库.表名`。

写入同样走 Spark SQL。`type` 包含 `sink` 时，`INSERT` 和 `TRUNCATE` 会作用到 `table_name` 指向的 Hive 表：

```sql
CREATE TABLE t_user_sink
USING hive
OPTIONS (
    'table_name'='default.t_user',
    'type' = 'sink,source'
);

INSERT INTO t_user_sink
SELECT id, name FROM t_user_v;
```

`UPDATE`、`DELETE`、`MERGE` 不通过这张视图别名执行。

也可以直接写 Hive 的 `CREATE TABLE` 建表语句。语句会交给 Spark 执行，在 Hive Catalog 里建表。建完之后，`SELECT`、`INSERT`、`TRUNCATE` 使用这张表的名字。`UPDATE`、`DELETE`、`MERGE` 仍然不支持。

```sql
CREATE TABLE IF NOT EXISTS default.t_user (
    id INT COMMENT 'id',
    name STRING,
    c_time TIMESTAMP
)
COMMENT 'user table'
PARTITIONED BY (dt STRING)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

INSERT INTO default.t_user
SELECT id, name, c_time FROM t_user_v;
```

`CREATE EXTERNAL TABLE`、`ROW FORMAT`、`LOCATION`、`CLUSTERED BY` 和 `CREATE TABLE ... LIKE` 同样按 Hive DDL 执行。`CREATE TABLE t AS SELECT ...` 仍然只创建脚本里的临时视图；带 `STORED AS` 或 `EXTERNAL` 的 `AS SELECT` 会在 Hive 里建表。

## Paimon

`USING paimon` 也创建视图，读写都走 Spark SQL。`metastore` 决定接入方式。`INSERT`、`TRUNCATE`、`UPDATE`、`DELETE`、`MERGE` 都会下发到 Paimon 表。单表更新和删除保持原语句；带关联表的 `UPDATE`、`DELETE` 会改写成 `MERGE`。这些行级变更要求 Paimon 表有主键。

### hive catalog

和 `USING hive` 相同。Spark 集群已经整合 Hive 时，不需要配置 `warehouse` 或 `metastore_uri`。`table_name` 写成 `数据库.表名`。

```sql
CREATE TABLE t_user_v
USING paimon
OPTIONS (
    'metastore' = 'hive',
    'table_name' = 'default.t_user',
    'type' = 'source,sink'
);

INSERT INTO t_user_v
SELECT id, name FROM t_user_v;
```

### filesystem

`warehouse` 指向 Paimon 仓库。可以写成完整的 HDFS 地址，也可以像 HDFS 文件源一样配置 `fs.defaultFS`，再写仓库路径。`hdfs_site_path` 指向 `hdfs-site.xml` 或其目录时，会作为 Paimon 的 Hadoop 配置目录。filesystem 模式需要 Spark 3，并加载 `connectors/connector-paimon.jar`；仓库在 HDFS 上时还会加载 `thirdparty/hadoop`。

```sql
CREATE TABLE t_user_v
USING paimon
OPTIONS (
    'metastore' = 'filesystem',
    'fs.defaultFS' = 'hdfs://localhost:9000',
    'warehouse' = '/paimon',
    'table_name' = 'default.t_user',
    'type' = 'source,sink'
);
```

上面的仓库地址等价于 `hdfs://localhost:9000/paimon`。也可以直接写：

```sql
'warehouse' = 'hdfs://localhost:9000/paimon'
```

同一份脚本里如果有多个 filesystem 仓库，用 `catalog` 给每个仓库单独起名，默认名称是 `paimon`。

### 建表

带列定义的 `CREATE TABLE ... USING paimon` 会在 Catalog 里创建 Paimon 表，并仍然用语句里的表名作为脚本别名。`table_name` 是要创建的 `数据库.表名`。主键写成 `PRIMARY KEY (...) NOT ENFORCED`，也可以写在 `TBLPROPERTIES` 的 `primary-key` 里。`OPTIONS` 里除了连接参数以外的项，例如 `bucket`，会写进表属性。

```sql
CREATE TABLE t_user (
    id BIGINT COMMENT 'id',
    name STRING,
    dt STRING,
    PRIMARY KEY (id, dt) NOT ENFORCED
)
USING paimon
PARTITIONED BY (dt)
OPTIONS (
    'metastore' = 'filesystem',
    'warehouse' = '/paimon',
    'table_name' = 'default.t_user',
    'bucket' = '4',
    'type' = 'source,sink'
);

INSERT INTO t_user
SELECT id, name, dt FROM source_v;
```

Hive metastore 同样建在 `table_name` 上，不需要 `warehouse`：

```sql
CREATE TABLE IF NOT EXISTS t_user (
    id BIGINT,
    name STRING,
    PRIMARY KEY (id) NOT ENFORCED
)
USING paimon
OPTIONS (
    'metastore' = 'hive',
    'table_name' = 'default.t_user'
);
```

没有列定义的 `USING paimon OPTIONS (...)` 仍然只引用已经存在的表。
