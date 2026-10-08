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
SET datasource.mytest.connector=mysql;
SET datasource.mytest.url='jdbc:mysql://127.0.0.1:3306/mytest';
SET datasource.mytest.driver=com.mysql.cj.jdbc.Driver;
SET datasource.mytest.user=root;
SET datasource.mytest.password=123456;
   
SET datasource.pg_test.connector=postgres;
SET datasource.pg_test.url='jdbc:postgresql://127.0.0.1:5432/postgres';
SET datasource.pg_test.driver=org.postgresql.Driver;
SET datasource.pg_test.user=postgres;
SET datasource.pg_test.password=password;

CREATE TABLE t_user_v
USING mysql
OPTIONS (
    'datasource'='mytest',
    'table_name'='t_user',
    'type' = 'source'
);

CREATE TABLE t_group_v
USING postgres
OPTIONS (
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

查询结果默认在控制台打印输出前`100`行；若 SELECT 本身带有 `LIMIT`，则按该 `LIMIT` 输出。

## CTE语法

用 `WITH` 给一段查询起名字，后面的语句可以反复引用这个名字。多个表达式用逗号隔开，后面的表达式可以引用前面的。查询需要写在括号里。

```sql
WITH adult AS (
    SELECT id, name, age FROM t_user_v WHERE age >= 18
),
names AS (
    SELECT name, count(1) AS cnt FROM adult GROUP BY name
)
SELECT name, cnt FROM names WHERE cnt > 1 ORDER BY cnt DESC;
```

`WITH` 也可以写在 INSERT、UPDATE、DELETE、MERGE 前面，或写在这些语句内部的查询前面：

```sql
WITH adult AS (
    SELECT id, name, age FROM t_user_v WHERE age >= 18
)
INSERT INTO t_user2_v (id, name, age)
SELECT id, name, age FROM adult;

INSERT INTO t_user2_v (id, name, age)
WITH adult AS (
    SELECT id, name, age FROM t_user_v WHERE age >= 18
)
SELECT id, name, age FROM adult;
```

`CREATE TABLE ... AS` 和游标循环里的查询同样可以使用 `WITH`。

## SELECT变量赋值语法

在脚本中先用`def`定义变量，再通过SELECT语句把查询结果赋给变量：

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

def v_name = '';
def v_age = 0;
SELECT name, age INTO :v_name, :v_age FROM t_user_v WHERE id = 1;
PUT_LINE('Name: ' || v_name || ', Age: ' || v_age);
```
**注意事项**：在SQL中如果使用变量，必须使用冒号`:`作为前缀，例如`:v_name`。
