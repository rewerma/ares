# Ares-PL/SQL语法-函数

脚本不再支持 `CREATE FUNCTION`、`RETURN`，也不能在 SQL 里调用自定义函数。计算直接写成表达式，或先赋给变量再使用。

```sql
def p1 = 2;
def p2 = 3.14;
def a = p1 * p2;
PUT_LINE('The result is: ' || a);

SELECT :a AS col_1;
```

内置函数 `PUT_LINE`、`LOGGER`、`SLEEP`、`ASSERT_EQUALS` 仍然按函数调用书写，见[内置函数](pl-function.md)。
