assert_equals(1 + 3, 4);
assert_equals(1 + 3.0, 4.0);
assert_equals(cast(1.345 as decimal(10, 2)) + 3, 4.35);
assert_equals(3 - 1, 2);
assert_equals(3.0 - 2, 1.0);
assert_equals(cast(3.345 as decimal(10, 2)) - 1, 2.35);
assert_equals(3 * 2, 6);
assert_equals(3.0 * 2, 6.0);
assert_equals(cast(3.345 as decimal(10, 2)) * 2, 6.70);
assert_equals(cast(6.345 as decimal(10, 2)) * cast(1.23 as decimal(10, 2)), 7.8105);
assert_equals(6 / 2, 3);
assert_equals(6.0 / 2, 3.0);
assert_equals(cast(6.345 as decimal(10, 2)) / 2, 3.175);
assert_equals(cast(6.345 as decimal(10, 2)) / cast(1.23 as decimal(10, 2)), 5.162601626016);

assert_equals(3 & 5, 1);
assert_equals(3 | 5, 7);
assert_equals(3 ^ 5, 6);

assert_equals(abs(-1), 1);
assert_equals(abs(-124.56), 124.56);
assert_equals(abs(cast(-124.565 as decimal(10,2))), cast(124.57 as decimal(10,2)));

assert_equals(acos(1), 0.0);
assert_equals(asin(1), 1.5707963267948966);
assert_equals(atan(1), 0.7853981633974483);
assert_equals(atan2(0, 0), 0.0);

assert_equals(bin(1234), '10011010010');
assert_equals(cbrt(27.0), 3.0);

assert_equals(conv('100', 2, 10), '4');
assert_equals(conv(10, 10, 16), 'A');
assert_equals(conv(-10, 16, -10), '-16');

assert_equals(cos(0), 1.0);
assert_equals(cosh(0), 1.0);
assert_equals(cot(1), 0.6420926159343306);

assert_equals(degrees(3.141592653589793), 180.0);
assert_equals(e(), 2.718281828459045);

assert_equals(exp(0), 1.0);
assert_equals(expm1(0), 0.0);

assert_equals(factorial(5), 120);
assert_equals(factorial(21), null);

assert_equals(find_in_set('ab','abc,b,ab,c,def'), 3);

assert_equals(floor(-0.1), -1);
assert_equals(floor(3.1411, 3), 3.141);

assert_equals(format_number(12332.123456, 4), '12,332.1235');
assert_equals(format_number(12332.123456, '##################.###'), '12332.123');

assert_equals(greatest(1, 2, 3, 4, 5), 5);
assert_equals(greatest(1, 2, 6, 4.4, 5.5), 6.0);

assert_equals(hypot(3, 4), 5.0);

assert_equals(ln(1), 0.0);
assert_equals(log(10, 100), 2.0);
assert_equals(log10(10), 1.0);
assert_equals(log1p(0), 0.0);
assert_equals(log2(8), 3.0);

 assert_equals(mod(2, 1.8), 0.2);
assert_equals(mod(cast(1.3 as decimal), 2.0), 1.0);

assert_equals(negative(1), -1);
assert_equals(negative(3.1415926), -3.1415926);

assert_equals(pi(), 3.141592653589793);

assert_equals(positive(1), 1);

assert_equals(pow(2, 3), 8.0);
assert_equals(power(2, 3), 8.0);

assert_equals(radians(180), 3.141592653589793);

assert_equals(rint(12.3456), 12.0);

assert_equals(round(2.5, 0), 3);

assert_equals(shiftleft(2, 1), 4);
assert_equals(shiftright(4, 1), 2);
assert_equals(shiftrightunsigned(4, 1), 2);

assert_equals(sign(40), 1.0);
assert_equals(sign(-5.345), -1.0);
assert_equals(sign(0), 0.0);
assert_equals(signum(40), 1.0);
assert_equals(signum(-5.345), -1.0);
assert_equals(signum(0), 0.0);

assert_equals(sin(0), 0.0);
assert_equals(sinh(0), 0.0);

assert_equals(sqrt(4), 2.0);

assert_equals(tan(0), 0.0);
assert_equals(tanh(0), 0.0);