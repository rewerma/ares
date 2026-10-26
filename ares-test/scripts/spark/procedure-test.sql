def p1 = 5;
def p2 = 3.14;
def a = 'test';
def b = 1;
def c = '2021-01-01 12:23:34.567';
def d = 1.124;
PUT_LINE(d);
while b <= p1
    PUT_LINE('Current index: '||b);
    if b > 2
        PUT_LINE('Break while loop!');
        break;
    end
    b = b + 1;
end

p1 = 2;
p2 = 3.14;
def v1 = (p1 * p2) || '_';
put_line('Result: '||v1);
