# Ares-PL/SQL语法-脚本与变量

脚本由顶层语句顺序组成，直接执行，不需要 `BEGIN/END` 包裹。

```sql
def v_name = 'John';
PUT_LINE(v_name);
```

## 变量

用 `def` 定义变量，用 `=` 赋值。不再使用 `DECLARE` 和 `:=`。

```sql
def v_name;
def v_age = 35;
def v_email = 'xxx@xxx.xxx';

v_name = 'John';
v_email = 'john@example.com';
PUT_LINE('Name: ' || v_name || ', Age: ' || v_age || ', Email: ' || v_email);

SELECT * FROM table_name WHERE age > :v_age;
```

初值决定变量类型：

- 整数字面量按整数处理；超过 9 位按长整数处理
- 小数字面量按双精度处理
- 单引号字符串按字符串处理
- `true` / `false` 按布尔处理
- 不写初值，或初值是函数调用时，按字符串处理

在 SQL 里引用变量必须加冒号，例如 `:v_age`。过程表达式里直接写变量名。

## SELECT结果赋值给变量

参考[SELECT SQL语法](select-sql.md)
