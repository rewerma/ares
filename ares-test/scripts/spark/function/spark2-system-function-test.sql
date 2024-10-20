assert_equals(coalesce(NULL, 1, NULL), 1);
assert_equals(ifnull(NULL, to_date('2021-01-01')), '2021-01-01');

assert_equals(cast('123' AS INT), 123);
assert_equals(cast('123.123345' AS DECIMAL(10,3)), 123.123);
assert_equals(cast('2021-01-04' AS DATE), '2021-01-04');
assert_equals(cast('2021-01-04 12:34:45.12345' AS TIMESTAMP), '2021-01-04 12:34:45.12345');
assert_equals(cast(123 AS TIMESTAMP), '1970-01-01 08:02:03');
assert_equals(cast('abcd' AS BINARY), '61626364');
assert_equals(cast(true AS BOOLEAN), true);
assert_equals(cast(1 AS BOOLEAN), true);

assert_equals(elt(1, 'scala', 'java'), 'scala');
assert_equals(elt(2, 'a', 1), 1);

assert_equals(hash('a'), 1485273170);

assert_equals(if(1 < 2, 'a', 'b'), 'a');

assert_equals(least(10, 9, 2, 4, 3), 2);
assert_equals(least(10.4, 9, 2, 4, 3.2), 2.0);

assert_equals(nullif(2, 2), null);
assert_equals(nvl(NULL, 2), 2);
assert_equals(nvl2(NULL, 2, 1), 1);

begin
    raise_error('Test raise error');
    exception when ex then
        assert_equals(ex.message, 'Test raise error');
end;

put_line(rand());
put_line(rand(null));
put_line(rand(0));
put_line(random());
put_line(randn());
put_line(randn(null));
put_line(randn(0));