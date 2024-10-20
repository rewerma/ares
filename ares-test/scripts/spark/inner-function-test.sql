CREATE FUNCTION hello_world(p1 VARCHAR) RETURN VARCHAR AS
    v VARCHAR := 'Hello, World!';
BEGIN
    put_line(v);
    RETURN v;
END;

hello_world('test');

CREATE FUNCTION test(num INT) RETURN INT AS
BEGIN
    RETURN num+1;
END;

put_line(test(-1));

SELECT test(10) as test;

CREATE FUNCTION test2(p1 TIMESTAMP) RETURN VARCHAR AS
    v1 TIMESTAMP;
BEGIN
    v1 := date_add(p1, 1) || ' ' || date_format(p1, 'HH:mm:ss');
    RETURN date_format(to_timestamp(v1), 'yyyy/MM/dd HH:mm:ss');
END;

DEClARE
    t TIMESTAMP := '2021-01-31 12:34:56';
BEGIN
    put_line(test2(t));
END;

-- recursion
CREATE FUNCTION test3(p1 INT, p2 INT) RETURN INT AS
BEGIN
    IF p1 < p2 THEN
       put_line(p1 ||'<'|| p2);
       RETURN test3(p1+1, p2);
    ELSE
       put_line(p1 ||'='|| p2);
       RETURN p1;
    END IF;
END;

test3(1,4);

SELECT test3(2, 7) as test3;

CREATE FUNCTION test4(p1 INT) RETURN INT AS
BEGIN
    RETURN test5(p1 + 1);
END;

CREATE FUNCTION test5(p1 INT) RETURN INT AS
BEGIN
    RETURN p1 * p1;
END;

put_line(test4(3));