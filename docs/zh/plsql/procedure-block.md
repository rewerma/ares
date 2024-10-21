# Ares-PL/SQL语法-存储过程块 PL语法及调用

## CREATE PROCEDURE 语法

在Ares的作业脚本可以定义存储过程块，提供调用执行内部的过程代码，语法示例如下：

```sql
CREATE PROCEDURE sample(p1 IN INT, p2 IN NUMBER) AS
    a NUMBER;
BEGIN
    a := p1 * p2;
    PUT_LINE('The result is: '|| a);
END;
```

## 调用存储过程

在Ares作业脚本中，可以通过`CALL`语句调用存储过程，语法示例如下：

```sql
CALL sample(2, 3.14);
```

也可以直接忽略`CALL`关键字，直接使用存储过程名调用，语法示例如下：

```sql
sample(2, 3.14);
```

## 带出参的存储过程语法

在Ares的作业脚本中，也可以定义带出参的存储过程，语法示例如下：

```sql
CREATE PROCEDURE sample(p1 IN INT, p2 IN NUMBER, p3 OUT NUMBER) AS
BEGIN
    p3 := p1 * p2;
END;

DECLARE
    v_result NUMBER;
BEGIN
    sample(2, 3.14, v_result);
    PUT_LINE('The result is: '|| v_result);
END;
```

## 存储过程的递归调用

在Ares的作业脚本中，也可以定义递归调用的存储过程，语法示例如下：

```sql
CREATE PROCEDURE sample(p1 IN INT) AS
BEGIN
    IF p1 <= 0 THEN
       PUT_LINE('End of recursion.');
    ELSE
       PUT_LINE('Continuing recursion with p1=' || p1);
       sample(p1-1);
    END IF;
END;

CALL sample(5);
```