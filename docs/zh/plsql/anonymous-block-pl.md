# Ares-PL/SQL语法-匿名过程块 PL语法

## BEGIN-END 语法

在Ares的作业脚本可以定义匿名过程块，以执行内部的过程代码，语法如下：

```sql
BEGIN
  -- 匿名过程块内部的代码
END;
```

过程块内部的代码可以是任何有效的PL/SQL代码，在执行过程中会被直接执行，不需要额外的调用。

## 变量声明使用语法

在匿名过程块内部，可以声明使用变量，语法如下：

```sql
DECLARE
  v_name VARCHAR;
  v_age INT;
  v_email VARCHAR := 'xxx@xxx.xxx'; -- 变量初始化
BEGIN
  v_name := 'John';
  v_age := 35;
  v_email := 'john@example.com';
  PUT_LINE('Name: '||v_name||', Age: '||v_age||', Email: '||v_email);
  
  -- 需要先定义数据源 table_name         
  SELECT * FROM table_name WHERE age > :v_age;
END;
```

**注意事项**：如果在SQL中使用变量，必须使用冒号`:`作为前缀，例如`:v_age`。

## SELECT结果赋值给变量

参考[SELECT SQL语法](select-sql.md)
