# Ares-PL/SQL语法-存储过程

脚本不再支持 `CREATE PROCEDURE`、`CALL` 和出参。原来放在存储过程里的逻辑，直接写在脚本中，用 `def` 保存中间结果。

```sql
def p1 = 2;
def p2 = 3.14;
def a = p1 * p2;
PUT_LINE('The result is: ' || a);
```

重复执行的一段逻辑，用 `while` 或 `for` 写在脚本里。语法见[循环](loop-block.md)。
