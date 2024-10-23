# Ares-PL/SQL语法支持

## 介绍

Ares-PL/SQL支持丰富的过程语言语法，包括条件语句、循环语句、异常处理、函数、存储过程、游标、变量、数据类型、数据源定义等。

## 执行参数定义

在Ares-PL/SQL脚本中，可以通过`SET ...=...`的方式定义内部执行参数：

```sql
SET spark.logLevel=info
SET spark.master=spark://127.0.0.1:7077
SET spark.driver.memory=1G
SET spark.executor.memory=2G
SET spark.executor.cores=1
SET spark.cores.max=1
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

## 匿名过程块PL语法

参考：[匿名过程块](anonymous-block.md)语法

## 存储过程块PL语法

参考：[存储过程块](procedure-block.md)语法

## 函数块PL语法

参考：[函数块](function-block.md)语法

## 内置PL函数

参考：[内置PL函数](pl-function.md)语法

## IF PL语法

参考：[IF PL语法](if-block.md)语法