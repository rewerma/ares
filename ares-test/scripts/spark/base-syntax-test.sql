DECLARE
    i INT := 0;
    e INT := 5;
BEGIN
    WHILE i < 5 LOOP
        IF i > 2 THEN
            EXIT;
        END IF;
        PUT_LINE('INDEX: ' || i);
        i := i + 1;
    END LOOP;

    FOR j IN 1..e LOOP
        IF j = 3 THEN
            EXIT;
        END IF;
        PUT_LINE('INDEX: ' || j);
    END LOOP;
END;