# Ares-PL/SQL语法-异常捕获语法

用 `try` 和 `catch` 捕获异常，用 `end` 结束。`end` 后面的分号可写可不写。`catch` 里的异常对象是 `ex`，详细信息是 `ex.message`。

```sql
try
    INSERT INTO t_user (id, name) VALUES (1, 'Alice');
catch
    PUT_LINE('Exception message: ' || ex.message);
end
```

`try` 可以写在脚本中任意语句可以出现的位置，包括 `if`、`while` 和 `for` 里面。进入 `catch` 之前，当前还没提交的事务批次会先回滚。事务段要写到 `END TRANSACTION` 才结束，详见[事务语法](transaction-block.md)。

## 抛出异常

`raise` 只能写在 `catch` 里。执行后脚本停止，并输出异常信息。

```sql
try
    INSERT INTO t_user (id, name) VALUES (1, 'Alice');
catch
    PUT_LINE('Exception message: ' || ex.message);
    raise;
end
```
