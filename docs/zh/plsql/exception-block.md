# Ares-PL/SQL语法-异常捕获语法

在Ares的作业脚本中，可以通过`EXCEPTION WHEN`语法捕获异常，并进行相应的处理，语法示例如下：

```sql
BEGIN
    ......
    EXCEPTION WHEN ex THEN
        PUT_LINE('Exception message: ' || ex.message);
END;
```
异常类型固定为`ex`，内置message属性可以获取异常的详细信息。

注意：异常捕获语法必须在`BEGIN`和`END`代码块的最下方；如果需要针对多段代码进行异常捕获，则可以将每段代码块定成`PROCEDURE`或`FUNCION`并在内部进行异常捕获。

## 抛出异常

在Ares的作业脚本中，捕获异常的代码块中可以通过`RAISE`语句抛出异常，语法示例如下：

```sql
BEGIN
    ......
    EXCEPTION WHEN ex THEN
        PUT_LINE('Exception message: ' || ex.message);
        RAISE;
END;
```

抛出异常后，脚本执行会立即停止，并输出异常堆栈信息。