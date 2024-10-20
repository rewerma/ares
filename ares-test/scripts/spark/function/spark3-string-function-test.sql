assert_equals(ascii('a'), 97);

assert_equals(bit_length('hello world'), 88);
assert_equals(bit_length(12), 16);
assert_equals(bit_length(unhex(hex('hello world'))), 88);

assert_equals(contains('hello world', 'world'), true);

assert_equals(crc32('hello world'), 222957957);
assert_equals(crc32(cast('hello world' as binary)), 222957957);

assert_equals(trim('    Ares-PLSQL   '), 'Ares-PLSQL');
assert_equals(trim('SSparkSQLS', 'SL'), 'parkSQ');

assert_equals(btrim('    Ares-PLSQL   '), 'Ares-PLSQL');
assert_equals(btrim('SSparkSQLS', 'SL'), 'parkSQ');

assert_equals(ltrim('    Ares-PLSQL   '), 'Ares-PLSQL   ');
assert_equals(rtrim('    Ares-PLSQL   '), '    Ares-PLSQL');
assert_equals(rtrim('SSparkSQLS', 'SL'), 'SSparkSQ');

assert_equals(base64('Ares-PLSQL'), 'QXJlcy1QTFNRTA==');
assert_equals(unbase64(base64('Ares-PLSQL')), '417265732D504C53514C');
assert_equals(cast(unbase64(base64('Ares-PLSQL')) as varchar), 'Ares-PLSQL')

assert_equals(hex('Ares-PLSQL'), '417265732D504C53514C');
assert_equals(unhex(hex('Ares-PLSQL')), cast('Ares-PLSQL' as binary));

assert_equals(encode('asdf', 'UTF-8'), '61736466');
assert_equals(encode('中文', 'UTF-8'), 'E4B8ADE69687');
assert_equals(encode('中文', 'GBK'), 'D6D0CEC4');
assert_equals(decode(encode('asdf', 'UTF-8'), 'UTF-8'), 'asdf');
assert_equals(decode(encode('中文', 'GBK'), 'GBK'), '中文');

assert_equals(endswith('hello world', 'world'), true);
assert_equals(endswith(cast('hello world' as binary), cast('world' as binary)), true);

assert_equals(format_string('Hello World %d %s', 100, 'days'), 'Hello World 100 days');

assert_equals(initcap('sPark sql'), 'Spark Sql');

assert_equals(instr('SparkSQL', 'SQL'), 6);

assert_equals(lcase('Ares-PL/SQL'), 'ares-pl/sql');
assert_equals(lower('Ares-PL/SQL'), 'ares-pl/sql');

assert_equals(left('Spark SQL', 3), 'Spa');
assert_equals(left(cast('Spark SQL' as binary), 3), unhex('537061'));

assert_equals(len('Spark SQL '), 10);
assert_equals(len(cast('abc' as binary)), 3);

assert_equals(levenshtein('kitten', 'sitting'), 3);

assert_equals(locate('bar', 'foobarbar'), 4);
assert_equals(locate('bar', 'foobarbar', 5), 7);
assert_equals(log2(8), 3.0);

assert_equals(lpad('hi', 5, '??'), '???hi');
assert_equals(lpad('hi', 1, '??'), 'h');
assert_equals(lpad('hi', 5), '   hi');
assert_equals(rpad('hi', 5, '??'), 'hi???');
assert_equals(rpad('hi', 1, '??'), 'h');
assert_equals(rpad('hi', 5), 'hi   ');

assert_equals(mask('abcd-EFGH-8765-4321'), 'xxxx-XXXX-nnnn-nnnn');
assert_equals(mask('abcd-EFGH-8765-4321', 'Q'), 'xxxx-QQQQ-nnnn-nnnn');
assert_equals(mask('AbCD123-@$#', 'Q', 'q'), 'QqQQnnn-@$#');
assert_equals(mask('AbCD123-@$#', 'Q', 'q', 'd'), 'QqQQddd-@$#');
assert_equals(mask('AbCD123-@$#', 'Q', 'q', 'd', 'o'), 'QqQQdddoooo');
assert_equals(mask('AbCD123-@$#', NULL, 'q', 'd', 'o'), 'AqCDdddoooo');
assert_equals(mask('AbCD123-@$#', NULL, NULL, 'd', 'o'), 'AbCDdddoooo');
assert_equals(mask('AbCD123-@$#', NULL, NULL, NULL, 'o'), 'AbCD123oooo');
assert_equals(mask(NULL, NULL, NULL, NULL, 'o'), null);
assert_equals(mask(NULL), null);

assert_equals(md5('Spark'), '8cde774d6f7333752ed72cacddb05126');

assert_equals(octet_length('Spark SQL'), 9);
assert_equals(octet_length(unhex('537061726b2053514c')), 9);

assert_equals(parse_url('http://spark.apache.org/path?query=1', 'HOST'), 'spark.apache.org');
assert_equals(parse_url('http://spark.apache.org/path?query=1', 'QUERY'), 'query=1');
assert_equals(parse_url('http://spark.apache.org/path?query=1', 'QUERY', 'query'), '1');

assert_equals(position('bar', 'foobarbar'), 4);
assert_equals(position('bar', 'foobarbar', 5), 7);

assert_equals(printf('Hello World %d %s', 100, 'days'), 'Hello World 100 days');

-- assert_equals(regexp('%SystemDrive%\Users\John', '%SystemDrive%\\Users.*'), true);
assert_equals(regexp_like('%SystemDrive%\Users\John', '%SystemDrive%\\Users.*'), true);
assert_equals(regexp_instr('user@spark.apache.org', '@[^.]*'), 5);

assert_equals(right('Spark SQL', 3), 'SQL');
assert_equals(right(cast('Spark SQL' as binary), 3), unhex('53514c'));

assert_equals(regexp_replace('100-200', '(\d+)', 'num'), 'num-num');

assert_equals(regexp_substr('Steven Jones and Stephen Smith are the best players', 'Ste(v|ph)en'), 'Steven');
assert_equals(regexp_substr('Steven Jones and Stephen Smith are the best players', 'Jeck'), null);

assert_equals(repeat('abc', 2), 'abcabc');

assert_equals(replace('ABCabc', 'abc', 'DEF'), 'ABCDEF');

assert_equals(reverse('Spark SQL'), 'LQS krapS');
assert_equals(reverse(cast('abc' as binary)), '636261');

-- assert_equals(rlike('%SystemDrive%\Users\John', '%SystemDrive%\\Users.*'), true);

assert_equals(sha('Spark'), '85f5955f4b27a9a4c2aab6ffe5d7189fc298b92c');
assert_equals(sha1('Spark'), '85f5955f4b27a9a4c2aab6ffe5d7189fc298b92c');
assert_equals(sha2('Spark', 256), '529bc3b07127ecb7e53a4dcf1991d9152c24537d919178022b2c42657f79a26b');

assert_equals(soundex('Miller'), 'M460');

assert_equals(space(3), '   ');
assert_equals(space('5.1'), '     ');

assert_equals(split_part('11.12.13', '.', 3), '13');
assert_equals(split_part('11.12.', '.', 3), '');
assert_equals(split_part('11.12.13', '.', 4), '');

assert_equals(startswith('Spark SQL', 'Spark'), true);
assert_equals(startswith('Spark SQL', 'SQL'), false);
assert_equals(startswith('Spark SQL', null), null);
assert_equals( startswith(cast('537061726b2053514c' as binary), cast('537061726b' as binary)), true);

assert_equals(substr('Spark SQL', 5), 'k SQL');
assert_equals(substr('Spark SQL', -3), 'SQL');
assert_equals(substr('Spark SQL', 5, 1), 'k');
assert_equals(substring('Spark SQL', 5), 'k SQL');
assert_equals(substring('Spark SQL', -3), 'SQL');
assert_equals(substring('Spark SQL', 5, 1), 'k');

assert_equals(substring_index('www.apache.org', '.', 2), 'www.apache');

assert_equals(to_binary('abc', 'utf-8'), cast('abc' as binary));
assert_equals(to_binary('616263', 'hex'), cast('abc' as binary));
assert_equals(to_binary('YWJj', 'base64'), cast('abc' as binary));

assert_equals(to_char(454, '999'), '454');
assert_equals(to_char(454.00, '000D00'), '454.00');
assert_equals(to_char(12454, '99G999'), '12,454');
assert_equals(to_char(78.12, '$99.99'), '$78.12');
assert_equals(to_char(-12454.8, '99G999D9S'), '12,454.8-');

assert_equals(translate('AaBbCc', 'abc', '123'), 'A1B2C3');

assert_equals(ucase('SparkSql'), 'SPARKSQL');

assert_equals(url_encode('https://spark.apache.org'), 'https%3A%2F%2Fspark.apache.org');
assert_equals(url_decode('https%3A%2F%2Fspark.apache.org'), 'https://spark.apache.org');

put_line(uuid());
assert_equals(length(uuid()), 36);