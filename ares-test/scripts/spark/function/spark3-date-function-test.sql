assert_equals(add_months('2016-08-31', 1), '2016-09-30');
assert_equals(convert_timezone('Asia/Shanghai','America/Los_Angeles', '2024-01-01 12:23:34.123'), '2023-12-31 20:23:34.123');
assert_equals(convert_timezone('Asia/Shanghai','America/Los_Angeles', '2024-01-01'), '2023-12-31 08:00:00');
assert_equals(convert_timezone('America/Los_Angeles', '2024-01-01 12:23:34.123'), '2023-12-31 20:23:34.123');
assert_equals(convert_timezone('Europe/Brussels', 'America/Los_Angeles', '2024-01-01 12:23:34.123'), '2024-01-01 03:23:34.123');

put_line(current_date);
put_line(curdate());
put_line(current_timestamp);
put_line(now());
assert_equals(current_timezone(), 'Asia/Shanghai');

assert_equals(date_add('2024-01-29', 4), '2024-02-02');
assert_equals(date_add('2024-01-29 12:23:34.12', 17), '2024-02-15');
assert_equals(dateadd('2024-01-29', 4), '2024-02-02');

assert_equals(date_diff('2024-02-01', '2024-01-29'), 3);
assert_equals(date_diff('2024-03-11 01:23:34', '2024-02-28 23:23:45'), 12);
assert_equals(datediff('2024-02-01', '2024-01-29'), 3);

assert_equals(date_part('year', '2024-03-11 01:23:34.123'), 2024);
assert_equals(date_part('month', '2024-03-11 01:23:34.123'), 3);
assert_equals(date_part('day', '2024-03-11 01:23:34.123'), 11);
assert_equals(date_part('hour', '2024-03-11 01:23:34.123'), 1);
assert_equals(date_part('minute', '2024-03-11 01:23:34.123'), 23);
assert_equals(date_part('second', '2024-03-11 01:23:34.123234'), 34.123234);
assert_equals(date_part('quarter', '2024-03-11 01:23:34'), 1);
assert_equals(date_part('doy', '2024-03-11 01:23:34'), 71);
assert_equals(date_part('dayofweek', '2024-03-11 01:23:34'), 2);
assert_equals(date_part('week', '2024-03-11 01:23:34'), 11);

assert_equals(date_format('2024-03-11 01:23:34.123','yyyy/MM/dd HH:mm:ss'), '2024/03/11 01:23:34');
assert_equals(date_format('2024-03-11 01:23:34.123','y'), '2024');
assert_equals(date_format('2024-03-11 01:23:34.123','SSS'), '123');

-- assert_equals(date_from_unix_date(1), '1970-01-02');
-- assert_equals(date_from_unix_date(10000), '1997-05-19');

assert_equals(to_date('2024-03-11'), '2024-03-11');
assert_equals(to_date('2024/03/11', 'yyyy/MM/dd'), '2024-03-11');
assert_equals(to_timestamp('2024-03-11 01:23:34.123'), '2024-03-11 01:23:34.123');
assert_equals(to_timestamp('2024/03/11 01:23:34.123', 'yyyy/MM/dd HH:mm:ss.SSS'), '2024-03-11 01:23:34.123');
assert_equals(to_timestamp('2024'), '2024-01-01 00:00:00');
assert_equals(to_timestamp(2), '1970-01-01 08:00:02');
assert_equals(to_timestamp('2024-03-11 01:23:34.123456'), '2024-03-11 01:23:34.123456');

assert_equals(day('2024-03-19'), 19);
assert_equals(day('2024-03-11 01:23:34.123'), 11);
assert_equals(dayofmonth('2024-03-19'), 19);
assert_equals(dayofmonth('2024-03-11 01:23:34.123'), 11);
assert_equals(dayofweek('2024-03-19'), 3);
assert_equals(dayofweek('2024-03-11 01:23:34.123'), 2);
assert_equals(dayofyear('2024-03-19'), 79);
assert_equals(dayofyear('2024-03-11 01:23:34.123'), 71);

assert_equals(date_sub('2024-02-03', 4), '2024-01-30');
assert_equals(date_sub('2024-02-07 12:23:34.12', 17), '2024-01-21');

assert_equals(date_trunc('YEAR', '2015-03-05T09:32:05.359'), '2015-01-01 00:00:00');
assert_equals(date_trunc('MM', '2015-03-05T09:32:05.359'), '2015-03-01 00:00:00');
assert_equals(date_trunc('DD', '2015-03-05T09:32:05.359'), '2015-03-05 00:00:00');
assert_equals(date_trunc('HOUR', '2015-03-05T09:32:05.359'), '2015-03-05 09:00:00');
assert_equals(date_trunc('MILLISECOND', '2015-03-05T09:32:05.123456'), '2015-03-05 09:32:05.123');

assert_equals(extract(YEAR FROM '2024-03-11 01:23:34.123'), 2024);
assert_equals(extract(MONTH FROM  '2024-03-11 01:23:34.123'), 3);
assert_equals(extract(DAY FROM '2024-03-11 01:23:34.123'), 11);
assert_equals(extract(HOUR FROM to_timestamp('2024-03-11 01:23:34.123')), 1);
assert_equals(extract(MINUTE FROM '2024-03-11 01:23:34.123'), 23);
assert_equals(extract(SECOND FROM '2024-03-11 01:23:34.123234'), 34.123234);
assert_equals(extract(WEEK FROM '2024-03-11 01:23:34.123234'), 11);
assert_equals(extract(DOY FROM '2024-03-11 01:23:34.123234'), 71);
assert_equals(extract(DAYOFWEEK FROM '2024-03-17 01:23:34.123234'), 1);
assert_equals(extract(QUARTER FROM '2024-04-11 01:23:34.123234'), 2);

-- assert_equals(from_unixtime(1), '1970-01-01 08:00:01');
-- assert_equals(from_unixtime(3, 'yyyy-MM-dd HH:mm:ss'), '1970-01-01 08:00:03');
-- assert_equals(from_utc_timestamp('2024-01-02 16:00:00', 'Asia/Shanghai'), '2024-01-03 00:00:00');
-- assert_equals(convert_timezone('Asia/Shanghai', 'UTC', from_unixtime(1)), '1970-01-01 00:00:01');

assert_equals(hour('2009-07-30 12:58:59'), 12);

assert_equals(last_day('2009-01-12'), '2009-01-31');

put_line(localtimestamp());

assert_equals(make_date(2013, 7, 15), '2013-07-15');
assert_equals(make_date(2013, 7, null), null);
assert_equals(make_timestamp(2014, 12, 28, 6, 30, 45.887), '2014-12-28 06:30:45.887');
assert_equals(make_timestamp(2014, 12, 28, 6, 30, 45.887, 'CET'), '2014-12-28 13:30:45.887');

assert_equals(minute('2009-07-30 12:58:59'), 58);
assert_equals(minute(to_timestamp('2009-07-30 12:58:59')), 58);
assert_equals(month('2016-07-30'), 7);

assert_equals(next_day('2015-01-14', 'TU'), '2015-01-20');

assert_equals(quarter('2016-08-31'), 3);
assert_equals(quarter(to_timestamp('2016-08-31 12:23:34')), 3);

assert_equals(second('2009-07-30 12:58:59'), 59);

assert_equals(trunc('2019-08-04', 'week'), '2019-07-29');
assert_equals(trunc('2019-08-04', 'quarter'), '2019-07-01');
assert_equals(trunc('2009-02-12', 'MM'), '2009-02-01');
assert_equals(trunc('2015-10-27', 'YEAR'), '2015-01-01');

assert_equals(weekday('2009-07-30'), 3);
assert_equals(weekofyear('2008-02-20'), 8);

assert_equals(year('2016-07-30'), 2016);