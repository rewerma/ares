DECLARE
    a BIGINT := 123456789012345;
    b INT := 12347;
    c SMALLINT := -1234;
    d BYTE := 123;
    e DOUBLE := 3.1415926;
    f FLOAT := 3.1415;
    g DECIMAL(20, 4) := 12345.67890123;
    h VARCHAR := 'Hello World';
    i DATE := current_date;
    j TIMESTAMP := now();
    k BINARY := 'abc';
    l BOOLEAN := true;
BEGIN
    put_line(a/2);
    a := a/2;
    put_line(a);
    put_line(b/3);
    b := b/3;
    put_line(b);
    put_line(c/3);
    c := c/3;
    put_line(c);
    put_line(d/2 * 3);
    d := (d/2 + 1)*2;
    put_line(d);
    e := e * 4;
    put_line(e);
    f := f * 4;
    put_line(f);
    put_line(g);
    g := g / 7;
    put_line(g);
    h := h || '!';
    put_line(h);
    i := date_add(i, 2);
    put_line(i);
    put_line(j);
    put_line(k);
    put_line(l);
END;

CREATE FUNCTION f_bigint(p1 BIGINT) RETURN BIGINT AS
BEGIN
    RETURN p1/2;
END;
put_line(f_bigint(1234567));

CREATE FUNCTION f_int(p1 INT) RETURN INT AS
BEGIN
    RETURN p1/2;
END;
put_line(f_int(234567));

CREATE FUNCTION f_smallint(p1 SMALLINT) RETURN SMALLINT AS
BEGIN
    RETURN p1/2;
END;
put_line(f_int(12345));

CREATE FUNCTION f_byte(p1 BYTE) RETURN BYTE AS
BEGIN
    RETURN p1/2;
END;
put_line(f_byte(123));

CREATE FUNCTION f_double(p1 DOUBLE) RETURN DOUBLE AS
BEGIN
    RETURN p1*2;
END;
put_line(f_double(3.1415926));

CREATE FUNCTION f_float(p1 FLOAT) RETURN FLOAT AS
BEGIN
    RETURN p1*2;
END;
put_line(f_float(3.14159));

CREATE FUNCTION f_decimal(p1 DECIMAL) RETURN DECIMAL AS
BEGIN
    RETURN p1;
END;
put_line(f_decimal(12345.12345678912));

CREATE FUNCTION f_decimal2(p1 DECIMAL(20, 6)) RETURN DECIMAL(20, 6) AS
BEGIN
    RETURN p1 * 2;
END;
put_line(f_decimal2(12345.12345678912));

CREATE FUNCTION f_varchar(p1 VARCHAR) RETURN VARCHAR AS
BEGIN
    RETURN p1 || '!';
END;
put_line(f_varchar('Hello World'));

CREATE FUNCTION f_date(p1 DATE) RETURN DATE AS
BEGIN
    RETURN date_add(p1, 2);
END;
put_line(f_date(to_date('2021-01-01')));

CREATE FUNCTION f_timestamp(p1 TIMESTAMP) RETURN TIMESTAMP AS
BEGIN
    RETURN to_timestamp(date_add(p1, 2)||' '||date_format(p1, 'HH:mm:ss'));
END;
put_line(f_timestamp(to_timestamp('2021-01-01 12:23:34')));

CREATE FUNCTION f_binary(p1 BINARY) RETURN BINARY AS
BEGIN
    RETURN p1;
END;
put_line(cast('asdf' AS BINARY));

CREATE FUNCTION f_boolean(p1 BOOLEAN, p2 INT) RETURN BOOLEAN AS
BEGIN
    put_line(p1);
    RETURN p2 > 0;
END;
put_line(f_boolean(false, 2));



CREATE PROCEDURE p_bigint(p1 IN BIGINT, p2 OUT BIGINT) AS
BEGIN
    p2 := p1/2;
END;

DECLARE
    v1 BIGINT;
BEGIN
    p_bigint(1234567, v1);
    put_line(v1);
END;

CREATE PROCEDURE p_int(p1 IN INT, p2 OUT INT) AS
BEGIN
    p2 := p1/2;
END;

DECLARE
    v1 INT;
BEGIN
    p_int(1234567, v1);
    put_line(v1);
END;

CREATE PROCEDURE p_smallint(p1 IN SMALLINT, p2 OUT SMALLINT) AS
BEGIN
    p2 := p1/2;
END;

DECLARE
    v1 SMALLINT;
BEGIN
    p_smallint(2345, v1);
    put_line(v1);
END;

CREATE PROCEDURE p_byte(p1 IN BYTE, p2 OUT BYTE) AS
BEGIN
    p2 := p1/2;
END;

DECLARE
    v1 BYTE;
BEGIN
    p_byte(123, v1);
    put_line(v1);
END;

CREATE PROCEDURE p_double(p1 IN DOUBLE, p2 OUT DOUBLE) AS
BEGIN
    p2 := p1/2;
END;

DECLARE
    v1 DOUBLE;
BEGIN
    p_double(3.1415926, v1);
    put_line(v1);
END;

CREATE PROCEDURE p_float(p1 IN FLOAT, p2 OUT FLOAT) AS
BEGIN
    p2 := p1/2;
END;

DECLARE
    v1 FLOAT;
BEGIN
    p_double(3.1415, v1);
    put_line(v1);
END;

CREATE PROCEDURE p_decimal(p1 IN DECIMAL, p2 OUT DECIMAL) AS
BEGIN
    p2 := p1;
END;

DECLARE
    v1 DECIMAL(20, 6);
BEGIN
    p_decimal(12345.12345678912, v1);
    put_line(v1);
END;

CREATE PROCEDURE p_decimal2(p1 IN DECIMAL(20, 6), p2 OUT DECIMAL(20, 6)) AS
BEGIN
    p2 := p1 * 2;
END;

DECLARE
    v1 DECIMAL;
BEGIN
    p_decimal2(12345.12345678912, v1);
    put_line(v1);
END;

CREATE PROCEDURE p_varchar(p1 IN VARCHAR, p2 OUT VARCHAR) AS
BEGIN
    p2 := p1||'!';
END;

DECLARE
    v1 VARCHAR;
BEGIN
    p_varchar('Hello World', v1);
    put_line(v1);
END;

CREATE PROCEDURE p_date(p1 IN DATE, p2 OUT DATE) AS
BEGIN
    p2 := date_add(p1, 2);
END;

DECLARE
    v1 DATE;
BEGIN
    p_date(current_date, v1);
    put_line(v1);
END;

CREATE PROCEDURE p_timestamp(p1 IN TIMESTAMP, p2 OUT TIMESTAMP) AS
BEGIN
    p2 := to_timestamp(date_add(p1, 2)||' '||date_format(p1, 'HH:mm:ss'));
END;

DECLARE
    v1 TIMESTAMP;
BEGIN
    p_timestamp(current_timestamp, v1);
    put_line(v1);
END;

CREATE PROCEDURE p_binary(p1 IN BINARY, p2 OUT BINARY) AS
BEGIN
    p2 := p1;
END;

DECLARE
    v1 BINARY;
BEGIN
    p_binary(cast('abc' AS BINARY), v1);
    put_line(v1);
END;

CREATE PROCEDURE p_boolean(p1 IN BOOLEAN, p2 OUT BOOLEAN) AS
BEGIN
    put_line(p1);
    p2 := 1>0;
END;

DECLARE
    v1 BOOLEAN;
BEGIN
    p_boolean(false, v1);
    put_line(v1);
END;