/**
 * Ares PL/SQL Lexer (simplified).
 *
 * Trimmed from the original Oracle 11g/12c PL/SQL grammar (Alexandre Porcelli,
 * Ivan Kochurkin, Mark Adams). Only the tokens actually consumed by the ares
 * parser visitors are retained. All unused keyword tokens (non_reserved_keywords_*)
 * and obsolete helpers (REMARK_COMMENT, PROMPT_MESSAGE, START_CMD, QS_*,
 * CHAR_STRING_PERL, BIT_STRING_LIT, HEX_STRING_LIT, etc.) have been removed.
 *
 * Licensed under the Apache License, Version 2.0.
 */

lexer grammar PlSqlLexer;

options {
    superClass=PlSqlLexerBase;
    caseInsensitive = true;
}

@lexer::postinclude {
#include <PlSqlLexerBase.h>
}

// ---------------------------------------------------------------------------
// DML / SQL top-level keywords
// ---------------------------------------------------------------------------

CREATE:                        'CREATE';
TABLE:                         'TABLE';
WITH:                          'WITH';
SET:                           'SET';
SELECT:                        'SELECT';
INSERT:                        'INSERT';
UPDATE:                        'UPDATE';
DELETE:                        'DELETE';
MERGE:                         'MERGE';
INTO:                          'INTO';
FROM:                          'FROM';
WHERE:                         'WHERE';
TRUNCATE:                      'TRUNCATE';

// ---------------------------------------------------------------------------
// PL/SQL block / control-flow keywords
// ---------------------------------------------------------------------------

PROCEDURE:                     'PROCEDURE';
FUNCTION:                      'FUNCTION';
RETURN:                        'RETURN';
BEGIN:                         'BEGIN';
END:                           'END';
EXCEPTION:                     'EXCEPTION';
WHEN:                          'WHEN';
THEN:                          'THEN';
ELSE:                          'ELSE';
ELSIF:                         'ELSIF';
IF:                            'IF';
LOOP:                          'LOOP';
FOR:                           'FOR';
WHILE:                         'WHILE';
EXIT:                          'EXIT';
CONTINUE:                      'CONTINUE';
RAISE:                         'RAISE';
DO:                            'DO';
DECLARE:                       'DECLARE';
CALL:                          'CALL';
AS:                            'AS';
IS:                            'IS';

// ---------------------------------------------------------------------------
// Logical / null / boolean
// ---------------------------------------------------------------------------

AND:                           'AND';
OR:                            'OR';
NOT:                           'NOT';
NULL_:                         'NULL';
TRUE:                          'TRUE';
FALSE:                         'FALSE';

// SQL predicates used in expressions (BETWEEN / LIKE family).
BETWEEN:                       'BETWEEN';
LIKE:                          'LIKE';
LIKEC:                         'LIKEC';
LIKE2:                         'LIKE2';
LIKE4:                         'LIKE4';
ESCAPE:                        'ESCAPE';

// ---------------------------------------------------------------------------
// Parameter / declaration keywords
// ---------------------------------------------------------------------------

IN:                            'IN';
OUT:                           'OUT';
CONSTANT:                      'CONSTANT';
DEFAULT:                       'DEFAULT';

// ---------------------------------------------------------------------------
// Native datatype keywords (consumed by native_datatype_element)
// ---------------------------------------------------------------------------

INT:                           'INT';
BYTE:                          'BYTE';
SMALLINT:                      'SMALLINT';
BIGINT:                        'BIGINT';
NUMBER:                        'NUMBER';
DECIMAL:                       'DECIMAL';
DOUBLE:                        'DOUBLE';
FLOAT:                         'FLOAT';
VARCHAR:                       'VARCHAR';
VARCHAR2:                      'VARCHAR2';
STRING:                        'STRING';
BOOLEAN:                       'BOOLEAN';
DATE:                          'DATE';
TIMESTAMP:                     'TIMESTAMP';
BINARY:                        'BINARY';
BLOB:                          'BLOB';

// ---------------------------------------------------------------------------
// Punctuation and operators
// ---------------------------------------------------------------------------

LEFT_PAREN:                    '(';
RIGHT_PAREN:                   ')';
COMMA:                         ',';
SEMICOLON:                     ';';
COLON:                         ':';
PERIOD:                        '.';
DOUBLE_PERIOD:                 '..';

ASSIGN_OP:                     ':=';
EQUALS_OP:                     '=';
NOT_EQUAL_OP:                  '!=' | '<>' | '^=' | '~=';

PLUS_SIGN:                     '+';
MINUS_SIGN:                    '-';
ASTERISK:                      '*';
SOLIDUS:                       '/';
BAR:                           '|';

// Standalone operator tokens used by relational_operator / unary_expression.
// The original grammar collapsed these into NOT_EQUAL_OP, but the parser
// references them individually so we expose them as their own tokens.
LESS_THAN_OP:                 '<';
GREATER_THAN_OP:              '>';
EXCLAMATION_OPERATOR_PART:    '!';
CARRET_OPERATOR_PART:         '^';

// ---------------------------------------------------------------------------
// Literals
// ---------------------------------------------------------------------------

// Oracle quoted string; supports embedded quotes via doubling and embedded newlines.
CHAR_STRING:                   '\''  (~('\'' | '\r' | '\n') | '\'' '\'' | NEWLINE)* '\'';

// N'...' literal (kept because some dialects use it; cheap to support).
NATIONAL_CHAR_STRING_LIT:      'N' '\'' (~('\'' | '\r' | '\n') | '\'' '\'' | NEWLINE)* '\'';

UNSIGNED_INTEGER:              [0-9]+;
APPROXIMATE_NUM_LIT:           FLOAT_FRAGMENT ('E' ('+'|'-')? (FLOAT_FRAGMENT | [0-9]+))? ('D' | 'F')?;

// Quoted identifier: "my column"
DELIMITED_ID:                  '"' (~('"' | '\r' | '\n') | '"' '"')+ '"' ;

// Bind variable: :name, :"quoted", or :123
BINDVAR:                       ':' SIMPLE_LETTER (SIMPLE_LETTER | [0-9] | '_')*
                            | ':' DELIMITED_ID
                            | ':' UNSIGNED_INTEGER;

// Regular identifier (variable names, table names, etc.).
REGULAR_ID:                    SIMPLE_LETTER (SIMPLE_LETTER | '$' | '_' | '#' | [0-9])*;

// ---------------------------------------------------------------------------
// Comments and whitespace
// ---------------------------------------------------------------------------

SINGLE_LINE_COMMENT:           '--' ~('\r' | '\n')* NEWLINE_EOF                  -> channel(HIDDEN);
MULTI_LINE_COMMENT:            '/*' ~[+] .*? '*/'                                -> channel(HIDDEN);

SPACES:                        [ \t\r\n]+                                        -> channel(HIDDEN);

// ---------------------------------------------------------------------------
// Fragment rules
// ---------------------------------------------------------------------------

fragment NEWLINE_EOF:          NEWLINE | EOF;
fragment SIMPLE_LETTER:        [A-Z];
fragment FLOAT_FRAGMENT:       UNSIGNED_INTEGER* '.'? UNSIGNED_INTEGER+;
fragment NEWLINE:              '\r'? '\n';
