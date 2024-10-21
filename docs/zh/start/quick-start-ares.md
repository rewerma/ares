# Ares快速开始

## 步骤 1: 部署Ares及连接器

在开始前，请确保您已经按照[部署](deployment.md)中的描述下载并部署了Ares

## 步骤 2: 部署并配置Spark(可选)

请先[下载Spark](https://spark.apache.org/downloads.html)(**需要版本 >= 2.4.0**)。 更多信息您可以查看[入门: standalone模式](https://spark.apache.org/docs/latest/spark-standalone.html#installing-spark-standalone-to-a-cluster)

**配置SPARK_HOME**: 修改`config/ares-env.sh`中的设置,它是基于你的引擎在[部署](deployment.md)时的安装路径。
将`SPARK_HOME`修改为Spark的部署目录。

## 步骤 3: 添加PL/SQL脚本文件来定义作业

在ares-bin目录下创建一个SQL脚本文件，例如`sample.sql`，并在其中定义作业：

```sql
CREATE TABLE t_user_v
WITH (
    'connector'='jdbc',
    'url'='jdbc:mysql://127.0.0.1:3306/mytest',
    'driver'='com.mysql.cj.jdbc.Driver',
    'user'='root',
    'password'='123456',
    'table_name'='t_user',
    'type' = 'source'
);

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

DECLARE
    v_cnt INT := 0;
BEGIN
    SELECT COUNT(*) INTO :v_cnt FROM t_user2_v;
    IF v_cnt = 0 THEN
        INSERT INTO t_user2_v (id, name, age, c_time) SELECT id+1, UPPER(name), age, c_time FROM t_user_v;
    END IF;
END;
```

**您可以参考[Ares-PL/SQL语法](../plsql/ares-plsql.md)来开发脚本作业。**

## 步骤 4: 通过Ares提交作业

在ares-bin目录下执行以下命令提交作业：

Local:
``` bash
./bin/ares-local-starter.sh --sql /path/to/sample.sql 
``` 

Spark3: 
``` bash
./bin/start-ares-spark3-connector.sh --sql /path/to/sample.sql --master spark://127.0.0.1:7077 
``` 

Spark2:
``` bash
./bin/start-ares-spark2-connector.sh --sql /path/to/sample.sql --master spark://127.0.0.1:7077 
``` 

**命令行参数说明**

1. `--sql` 指定脚本文件绝对路径
2. `--master` 指定Spark Master URL
3. `--name` 指定作业名称
4. `--conf` 指定Spark配置参数，例如`--conf spark.executor.memory=1g`，多个参数用`,`分隔
5. `--deploy-mode` 指定部署模式，默认为client模式，可以设置为cluster模式（与Spark的提交参数相同）

**查看输出**: 当您运行该命令时，您可以在控制台中看到它的输出。您可以认为这是命令运行成功或失败的标志。

Ares控制台将会打印一些如下日志信息:

```shell
INFO  com.github.ares.connector.discovery.AbstractPluginDiscovery - Load Factory Plugin from /Users/rewerma/Develop/git_aliyun/ares/connectors
INFO  com.github.ares.connector.discovery.AbstractPluginDiscovery - Load plugin: PluginIdentifier{engineType='ares_spark', pluginType='source', pluginName='jdbc'} from classpath
INFO  com.github.ares.connector.discovery.AbstractPluginDiscovery - Load plugin: PluginIdentifier{engineType='ares_spark', pluginType='sink', pluginName='jdbc'} from classpath
INFO  [SQLExecution] - Execute SQL: INSERT INTO t_user2 (id, name, age, c_time) SELECT id + 1, UPPER(name), age, c_time FROM t_user; Params: {v_cnt=0}
INFO  [SQLExecution] - Executed SQL: INSERT INTO t_user2 (id, name, age, c_time) SELECT id + 1, UPPER(name), age, c_time FROM t_user; elapsed time: 1.06s
```

## 此外

你可以通过在[连接器]()中找到Ares所支持的所有source和sink插件。