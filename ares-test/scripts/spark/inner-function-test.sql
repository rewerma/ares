def v = 'Hello, World!';
put_line(v);

put_line(-1 + 1);

SELECT 10 + 1 as test;

def t = '2021-01-31 12:34:56';
put_line(date_format(to_timestamp(date_add(t, 1) || ' ' || date_format(t, 'HH:mm:ss')), 'yyyy/MM/dd HH:mm:ss'));

-- 原先的递归函数 test3(1, 4) / test3(2, 7)
def p1 = 1;
def p2 = 4;
while p1 < p2
    put_line(p1 || '<' || p2);
    p1 = p1 + 1;
end
put_line(p1 || '=' || p2);

p1 = 2;
p2 = 7;
while p1 < p2
    put_line(p1 || '<' || p2);
    p1 = p1 + 1;
end
put_line(p1 || '=' || p2);
SELECT :p1 as test3;

put_line((3 + 1) * (3 + 1));
