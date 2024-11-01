# Ares-PL/SQL语法-循环语法

在Ares的作业脚本中，可以使用FOR/WHILE语法来实现循环，语法示例如下：

```sql
DECLARE
    i INT := 0;
BEGIN
    WHILE i < 5 LOOP
        PUT_LINE('INDEX: ' || i);
        i := i + 1;
    END LOOP;
END;
```

```sql
BEGIN
    FOR i IN 1..10 LOOP
        PUT_LINE('INDEX: ' || i);
    END LOOP;
END;
```
注意：循环代码块必须在BEGIN和END代码块之间；使用`FOR`语法时步进变量`i`不需要事先定义声明，而使用`WHILE`语法时，需要事先定义循环变量`i`并初始化值。

## 循环控制语句

在Ares-PL/SQL中，可以通过`EXIT`, `CONTINUE`语句来控制循环的执行，语法示例：

```sql
DECLARE
    i INT := 0;
BEGIN
    WHILE i < 5 LOOP
        IF i = 3 THEN
            EXIT;
        END IF;
        PUT_LINE('INDEX: ' || i);
        i := i + 1;
    END LOOP;
END;
```

```sql
BEGIN
    FOR i IN 1..10 LOOP
        IF j = 3 THEN
            CONTINUE;
        END IF;
        PUT_LINE('INDEX: ' || i);
    END LOOP;
END;
```

## 游标循环

在Ares-PL/SQL中，可以通过游标循环来实现对表或SQL执行结果的遍历，语法示例：

```sql
BEGIN
    FOR cur IN (select 1 as id, 'Eric' as name, '2024-01-02 12:23:34' as c_time 
      union all select 2, 'John', '2025-02-03 13:24:35' 
      union all select 3, 'Mary', '2026-03-04 14:25:36') LOOP
        println(cur.id||' '||cur.name||' '||cur.c_time);
    END LOOP;
END;
```

在游标循环代码块中也支持`EXIT`和`CONTINUE`语句。

注意：在Ares-PL/SQL中，需要游标遍历的结果集将会全量加载到内存中，如果数据量过大可能由于内存溢出的情况！
