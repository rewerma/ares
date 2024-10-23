# Ares-PL/SQL语法-IF语法

在Ares的作业脚本中，可以使用IF语法来进行条件判断，语法示例如下：

```sql
DECLARE
    id NUMBER;
BEGIN
    SELECT id INTO :id FROM table WHERE name = 'John';
    IF id < 4 THEN
        PUT_LINE('ID less than 4 ' || id);
    ELSIF id = 4 THEN
        PUT_LINE('ID equals 4 ' || id);
    ELSE
        PUT_LINE('ID great than 4 ' || id);
    END IF;
END;
```
注意：IF代码块必须在BEGIN和END代码块之间。