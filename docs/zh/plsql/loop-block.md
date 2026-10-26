# Ares-PL/SQL语法-循环语法

用 `while` 和 `for` 写循环，用 `end` 结束循环体。`end` 后面的分号可写可不写。

```sql
def i = 0;
while i < 5
    PUT_LINE('INDEX: ' || i);
    i = i + 1;
end
```

```sql
for i in 1 .. 10
    PUT_LINE('INDEX: ' || i);
end
```

`for` 的步进变量只在本次循环内有效，不需要事先 `def`。`while` 的循环变量要先 `def` 并赋初值。

## 循环控制语句

用 `break` 结束当前循环，用 `continue` 进入下一轮。这两条语句以分号结束。

```sql
def i = 0;
while i < 5
    if i = 3
        break;
    end
    PUT_LINE('INDEX: ' || i);
    i = i + 1;
end
```

```sql
for i in 1 .. 10
    if i = 3
        continue;
    end
    PUT_LINE('INDEX: ' || i);
end
```

## 游标循环

`for` 后面跟括号里的查询，可以逐行遍历结果。括号内的查询不能写分号，可以是 `SELECT`，也可以是 `WITH` 查询。

```sql
for cur in (select 1 as id, 'Eric' as name, '2024-01-02 12:23:34' as c_time
  union all select 2, 'John', '2025-02-03 13:24:35'
  union all select 3, 'Mary', '2026-03-04 14:25:36')
    println(cur.id || ' ' || cur.name || ' ' || cur.c_time);
end
```

游标循环里同样可以使用 `break` 和 `continue`。行字段写成 `cur.id` 这种形式。

游标结果会整批载入内存。数据量很大时可能把内存占满。
