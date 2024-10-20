# Ares-PL/SQL语法-CREATE AS SQL语法

## CREATE AS语法

在定义好[数据源](datasource.md)后，我们就可以使用CREATE AS SELECT 语句为查询结果创建映射表视图：

```sql
CREATE TABLE table_name AS
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

CREATE TABLE t_user_v
WITH (
    'datasource'='mytest',
    'table_name'='t_user',
    'type' = 'source'
);

CREATE TABLE t_user_v1
SELECT id, name, age FROM (
    SELECT id, name, age, ROW_NUMBER() OVER (PARTITION BY name ORDER BY id DESC) AS rn
    FROM t_user_v
) WHERE rn = 1;

SELECT * FROM t_user_v1;
```

通过`CREATE AS SELECT`语句，我们可以将查询结果映射到一个新的视图表上（但并不会真正创建物理表或视图），这样，我们就可以在Ares作业脚本中使用该视图表，就像使用其他表或视图一样。

通过`CREATE AS SELECT`语句，可以更加灵活地代替`CTE`语法。