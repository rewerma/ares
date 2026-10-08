/**
 * Ares script lexer.
 *
 * Lightweight replacement for the trimmed Oracle PL/SQL lexer. SQL text is
 * tokenized with this same lexer and reconstructed by the visitor, so every
 * character that can appear outside a string in a Spark statement still needs
 * a token. Keywords that are not part of the script language fall through to
 * REGULAR_ID and are consumed by the raw-SQL rules.
 *
 * Licensed under the Apache License, Version 2.0.
 */

lexer grammar PlSqlLexer;

options {
    superClass=PlSqlLexerBase;
    caseInsensitive = true;
}

// ---------------------------------------------------------------------------
// Script keywords
// ---------------------------------------------------------------------------

DEF:                           'DEF';
IF:                            'IF';
ELSE:                          'ELSE';
ELSIF:                         'ELSIF';
ELSEIF:                        'ELSEIF';
WHILE:                         'WHILE';
FOR:                           'FOR';
IN:                            'IN';
TRY:                           'TRY';
CATCH:                         'CATCH';
BREAK:                         'BREAK';
CONTINUE:                      'CONTINUE';
RAISE:                         'RAISE';

// ---------------------------------------------------------------------------
// Statement prefixes. The rest of these statements is raw text until ';'.
// ---------------------------------------------------------------------------

SET:                           'SET';
START:                         'START';
END:                           'END';
TRANSACTION:                   'TRANSACTION';
COMMIT:                        'COMMIT';
ROLLBACK:                      'ROLLBACK';

SELECT:                        'SELECT';
INSERT:                        'INSERT';
UPDATE:                        'UPDATE';
DELETE:                        'DELETE';
MERGE:                         'MERGE';
TRUNCATE:                      'TRUNCATE';
CREATE:                        'CREATE';
DROP:                          'DROP';
ALTER:                         'ALTER';
WITH:                          'WITH';

// ---------------------------------------------------------------------------
// Expression keywords
// ---------------------------------------------------------------------------

AND:                           'AND';
OR:                            'OR';
NOT:                           'NOT';
IS:                            'IS';
NULL_:                         'NULL';
TRUE:                          'TRUE';
FALSE:                         'FALSE';
BETWEEN:                       'BETWEEN';
LIKE:                          'LIKE';
ESCAPE:                        'ESCAPE';

// ---------------------------------------------------------------------------
// Punctuation and operators
// Longer tokens are listed first. ANTLR still picks the longest match;
// the order matters when two rules match the same length.
// ---------------------------------------------------------------------------

NULL_SAFE_EQUALS:              '<=>';
NOT_EQUAL_OP:                  '!=' | '<>' | '^=' | '~=';
LESS_THAN_OP:                  '<';
GREATER_THAN_OP:               '>';
EQUALS_OP:                     '=';

PLUS_SIGN:                     '+';
MINUS_SIGN:                    '-';
ASTERISK:                      '*';
SOLIDUS:                       '/';
PERCENT:                       '%';
CONCAT:                        '||';
BAR:                           '|';
AMPERSAND:                     '&';
CARET:                         '^';

LEFT_PAREN:                    '(';
RIGHT_PAREN:                   ')';
LEFT_BRACE:                    '{';
RIGHT_BRACE:                   '}';
LEFT_BRACKET:                  '[';
RIGHT_BRACKET:                 ']';
COMMA:                         ',';
SEMICOLON:                     ';';
PERIOD:                        '.';
DOUBLE_PERIOD:                 '..';

// ---------------------------------------------------------------------------
// Literals
// ---------------------------------------------------------------------------

// Oracle-style quoted string. Embedded quotes are doubled. Newlines are kept
// so a connector option can span lines.
CHAR_STRING:                   '\'' (~('\'' | '\r' | '\n') | '\'' '\'' | NEWLINE)* '\'';

NATIONAL_CHAR_STRING_LIT:      'N' '\'' (~('\'' | '\r' | '\n') | '\'' '\'' | NEWLINE)* '\'';

UNSIGNED_INTEGER:              [0-9]+;
APPROXIMATE_NUM_LIT:           FLOAT_FRAGMENT ('E' ('+'|'-')? (FLOAT_FRAGMENT | [0-9]+))? ('D' | 'F')?;

// Quoted identifier: "my column" or `my column`.
DELIMITED_ID:                  '"' (~('"' | '\r' | '\n') | '"' '"')+ '"';
BACKTICK_ID:                   '`' (~('`' | '\r' | '\n') | '`' '`')+ '`';

// Bind variable: :name, :"quoted", or :123. Used inside SQL text.
BINDVAR:                       ':' SIMPLE_LETTER (SIMPLE_LETTER | [0-9] | '_')*
                            | ':' DELIMITED_ID
                            | ':' UNSIGNED_INTEGER;

REGULAR_ID:                    SIMPLE_LETTER (SIMPLE_LETTER | '$' | '_' | '#' | [0-9])*;

// ---------------------------------------------------------------------------
// Comments and whitespace
// ---------------------------------------------------------------------------

SINGLE_LINE_COMMENT:           '--' ~('\r' | '\n')* NEWLINE_EOF                  -> channel(HIDDEN);
MULTI_LINE_COMMENT:            '/*' .*? '*/'                                     -> channel(HIDDEN);
SPACES:                        [ \t\r\n]+                                        -> channel(HIDDEN);

fragment NEWLINE_EOF:          NEWLINE | EOF;
fragment SIMPLE_LETTER:        [A-Z];
fragment FLOAT_FRAGMENT:       UNSIGNED_INTEGER* '.'? UNSIGNED_INTEGER+;
fragment NEWLINE:              '\r'? '\n';
