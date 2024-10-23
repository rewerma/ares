# Ares-PL/SQL语法-内置PL函数

## 1. 打印输出函数

在Ares的作业脚本中，可以使用`PUT_LINE`函数来打印输出内容到控制台。

```sql
PUT_LINE('abc');
        
PUT_LINE('abc' || 123);
```

## 2. 日志输出函数

在Ares的作业脚本中，可以使用`LOG_MSG`函数来输出日志信息，输出的日志会以`[PL-LOGGER]`前缀开头。

```sql
LOGGER('INFO', 'This is a info log message.');
LOGGER('DEBUG', 'This is a debug log message: {}.', 'debug info');
LOGGER('ERROR', 'This is a error log message.');
LOGGER('WARN', 'This is a warn log message, {}, {}.', 'test', 3.1415926);
```

## 3. 延时函数

在Ares的作业脚本中，可以使用`SLEEP`函数来延时指定的时间(单位：秒)。

```sql
SLEEP(10);
```

## 4. 断言函数

在Ares的作业脚本中，可以使用`ASSERT_EQUALS`函数来进行断言，如果断言失败，则会抛出`ASSERT_EQUALS failed`异常。
```sql
ASSERT_EQUALS(1, 1);
             
ASSERT_EQUALS(IF(1 < 2, 'a', 'b'), 'a');
```