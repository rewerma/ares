# Ares-PL/SQL语法-IF语法

用 `if`、`elsif`（或 `elseif`）、`else` 做条件判断，用 `end` 结束。

```sql
def id = 0;
SELECT id INTO :id FROM table_name WHERE name = 'John';
if id < 4
    PUT_LINE('ID less than 4 ' || id);
elsif id = 4
    PUT_LINE('ID equals 4 ' || id);
else
    PUT_LINE('ID great than 4 ' || id);
end
```

比较使用 `=`、`!=`、`<`、`>`、`<=`、`>=`。`end` 后面可以写分号，也可以不写。
