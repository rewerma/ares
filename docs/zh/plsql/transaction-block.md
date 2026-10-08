# Ares-PL/SQL语法-事务语法

JDBC 的 INSERT、UPDATE、DELETE、MERGE 可以放在事务段里执行。事务段从 `START TRANSACTION` 开始，到 `END TRANSACTION` 结束。这两条语句，以及 `COMMIT`、`ROLLBACK`，都以分号结束。

```sql
START TRANSACTION;
UPDATE t_user SET name = 'Tom' WHERE id = 1;
COMMIT;
END TRANSACTION;
```

`START TRANSACTION` 之后，每个 JDBC 数据源各自使用一条连接，直到 `COMMIT` 才写入该数据源。同一条 URL、同一个用户下的多张表共用这一条连接。`COMMIT` 和 `ROLLBACK` 会对事务段里的每个数据源都执行一次，各自提交或回滚自己的本地事务。`COMMIT` 只提交当前这一批，事务段仍然开着，后面的 DML 不用再写 `START TRANSACTION`。

某个数据源 `COMMIT` 失败时，已经提交成功的数据源会保留结果，其余还没提交的数据源会回滚。

`END TRANSACTION` 结束事务段。如果这时还有没提交的修改，会先提交再结束。脚本在没有 `END TRANSACTION` 时结束，还没提交的这一批会回滚，已经 `COMMIT` 的数据保留。

`ROLLBACK` 丢掉当前还没提交的这一批，事务段仍然开着，需要 `END TRANSACTION` 才结束。

事务段里可以写多个 JDBC 数据源。`TRUNCATE` 不能写在事务段里。`for` 游标里的 `SELECT` 仍按普通查询执行。

```sql
START TRANSACTION;
UPDATE mysql_a SET name = 'Tom' WHERE id = 1;
UPDATE mysql_b SET name = 'Tom' WHERE id = 1;
COMMIT;
END TRANSACTION;
```

## 分批提交

`START TRANSACTION` 写在循环外面，循环里按批次 `COMMIT`。循环结束后把剩余行提交，再 `END TRANSACTION`。

```sql
def cnt = 0;
try
    START TRANSACTION;
    for cur in (SELECT * FROM test2 WHERE id > 0 LIMIT 10)
        UPDATE test2 SET name = :cur.name||'_', c_time = :cur.c_time WHERE id = :cur.id;
        cnt = cnt + 1;
        if cnt >= 3
            COMMIT;
            cnt = 0;
        end
    end
    if cnt > 0
        COMMIT;
    end
    END TRANSACTION;
catch
    ROLLBACK;
    END TRANSACTION;
    PUT_LINE(ex.message);
    raise;
end
```

进入 `catch` 之前，当前还没提交的这一批会先回滚。`catch` 里再写 `ROLLBACK` 和 `END TRANSACTION`，用来结束事务段。
