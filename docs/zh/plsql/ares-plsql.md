# Ares-PL/SQL语法支持

## 介绍

Ares 脚本是一串语句，不需要 `DECLARE`，也不需要 `BEGIN/END` 包起来。支持的过程语法包括变量、条件、循环、异常捕获、游标、事务、数据源定义，以及 SELECT、INSERT、UPDATE、DELETE、MERGE。

普通语句以分号结束。`if`、`while`、`for`、`try` 以 `end` 结束，`end` 后面的分号可写可不写。`SET` 和 `CREATE TABLE ... USING` 只能写在脚本顶层。

## 执行参数定义

在脚本中用 `SET ...=...;` 定义执行参数。值里如果包含 `:`、`?`、`&` 等符号，需要用单引号包起来：

```sql
SET spark.logLevel=info;
SET spark.master='spark://127.0.0.1:7077';
SET spark.driver.memory=1G;
SET spark.executor.memory=2G;
SET spark.executor.cores=1;
SET spark.cores.max=1;
```

# SQL语法

## 数据源定义 SQL语法

参考：[数据源](datasource.md)创建语法

## INSERT SQL语法

参考：[INSERT-SQL](insert-sql.md)语法

## UPDATE SQL语法

参考：[UPDATE-SQL](update-sql.md)语法

## DELETE SQL语法

参考：[DELETE-SQL](delete-sql.md)语法

## MERGE SQL语法

参考：[MERGE-SQL](merge-sql.md)语法

## SELECT SQL语法

参考：[SELECT-SQL](select-sql.md)语法

## CREATE AS SQL语法

参考：[CREATE AS-SQL](create-as-sql.md)语法

# PL语法

## 脚本与变量

参考：[脚本与变量](anonymous-block.md)语法

## 内置函数

参考：[内置函数](pl-function.md)语法

## IF语法

参考：[IF语法](if-block.md)语法

## 循环语法

参考：[循环语法](loop-block.md)语法

## 异常处理语法

参考：[异常处理语法](exception-block.md)语法

## 事务语法

参考：[事务语法](transaction-block.md)语法
