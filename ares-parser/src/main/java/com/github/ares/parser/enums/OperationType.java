package com.github.ares.parser.enums;

public enum OperationType {
    CREATE_PROCEDURE("createProcedure"),
    CREATE_FUNCTION("createFunction"),
    CALL_FUNCTION("callFunction"),
    ANONYMOUS_BODY("anonymousBody"),
    ASSIGNMENT("assignment"),
    DECLARE_PARAMS("declareParams"),
    EXIT_LOOP("exitLoop"),
    CONTINUE_LOOP("continueLoop"),
    RETURN_VALUE("returnValue"),
    FOR_LOOP("forLoop"),
    FOR_CURSOR_LOOP("forCursorLoop"),
    WHILE_LOOP("whileLoop"),
    IF_ELSE("ifElse"),
    CREATE_SOURCE_TABLE("createSourceTable"),
    CREATE_SINK_TABLE("createSinkTable"),
    CREATE_TABLE_AS_SQL("createTableAsSQL"),
    SELECT_INTO_SQL("selectIntoSQL"),
    SELECT_SQL("selectSQL"),
    INSERT_SELECT_SQL("insertSelectSQL"),
    UPDATE_SELECT_SQL("updateSelectSQL"),
    DELETE_SELECT_SQL("deleteSelectSQL"),
    MERGE_INTO_SQL("mergeIntoSQL"),
    TRUNCATE_SQL("truncateSQL"),
    EXCEPTION_HANDLER("exceptionHandler"),
    EXPRESSION("expression"),
    SET_CONFIG("setConfig");

    private final String name;

    OperationType(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
