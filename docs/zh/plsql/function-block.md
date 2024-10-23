# Ares-PL/SQL语法-函数块 PL语法及调用

## CREATE FUNCTION 语法

在Ares的作业脚本可以定义函数块，提供调用执行内部的过程代码，语法示例如下：

```sql
CREATE FUNCTION sample(p1 IN INT, p2 IN NUMBER) RETURN NUMBER AS
    a NUMBER;
BEGIN
    a := p1 * p2;
    PUT_LINE('The result is: ' || a);
    RETURN a;
END;
```

函数和存储过程的区别为：函数不支持出参；函数必须申明返回值类型且必须有返回值；函数体内部不支持`SQL`语句，只支持过程语法。

## 调用存储过程

在Ares作业脚本中，可以通过`CALL`语句调用函数，语法示例如下：

```sql
CALL sample(2, 3.14);
```

也可以直接忽略`CALL`关键字，直接使用存储过程名调用，语法示例如下：

```sql
DECLARE
    v_result NUMBER;
BEGIN
    v_result := sample(2, 3.14);
    PUT_LINE('The result is: '|| v_result);
END;
```

在Ares作业脚本中定义的函数，可以直接在`SQL`中使用，语法示例如下：
```sql
SELECT sample(2, 3.14);

INSERT INTO table1 (col1, col2) VALUES (sample(2, 3.14), 'test');
```

## 函数的递归调用

在Ares的作业脚本中，也可以定义递归调用的函数，语法示例如下：

```sql
CREATE FUNCTION sample(p1 IN INT) RETURN NUMBER AS
BEGIN
    IF p1 <= 0 THEN
       PUT_LINE('End of recursion.');
       RETURN 0;
    ELSE
       PUT_LINE('Continuing recursion with p1=' || p1);
       RETURN sample(p1-1);
    END IF;
END;

SELECT sample(5) AS col_1;
```