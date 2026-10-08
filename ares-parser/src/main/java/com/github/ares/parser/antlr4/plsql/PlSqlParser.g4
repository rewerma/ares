/**
 * Ares script parser.
 *
 * Flat statement list. There is no DECLARE section and no BEGIN/END wrapper.
 * Control flow ends with END. SQL statements are not parsed here: the leading
 * keyword selects them, and every token up to the terminating semicolon is
 * kept so a visitor can pass the text to Spark.
 *
 *   def a = 1;
 *   a = a + 1;
 *   if a > 0 PUT_LINE(a); elsif a = 0 break; else PUT_LINE(a); end
 *   while a < 10 a = a + 1; end
 *   for i in 1 .. 10 PUT_LINE(i); end
 *   for cur in (SELECT id FROM t) PUT_LINE(cur.id); end
 *   try INSERT INTO t VALUES (:a); catch PUT_LINE(ex.message); raise; end
 *   CREATE TABLE t USING mysql OPTIONS ( 'dbtable' = 't_user' );
 *
 * Licensed under the Apache License, Version 2.0.
 */

parser grammar PlSqlParser;

options {
    tokenVocab=PlSqlLexer;
}

// ============================================================================
// Script
// ============================================================================

sql_script
    : statement* EOF
    ;

statement
    : terminated_statement
    | if_statement
    | while_statement
    | for_statement
    | try_statement
    ;

// Statements that end with ';'. SQL and SET carry their own semicolon because
// the raw-token loop has to stop on it. The END that closes if/while/for/try
// may be followed by a semicolon.
terminated_statement
    : (def_statement
      | assignment_statement
      | call_statement
      | transaction_statement
      | break_statement
      | continue_statement
      | raise_statement
      ) SEMICOLON
    | set_statement
    | sql_statement
    ;

// END closes if/while/for/try. END TRANSACTION stays a statement, so the body
// continues only when END is followed by TRANSACTION.
body
    : ( { _input.LA(1) != END || _input.LA(2) == TRANSACTION }? statement )*
    ;

// ============================================================================
// Variables, calls, config, transaction
// ============================================================================

// def a = 1;    or    def a;
def_statement
    : DEF identifier (EQUALS_OP expression)?
    ;

// a = a + 1;    dotted targets cover cur.id = ...
assignment_statement
    : identifier (PERIOD identifier)* EQUALS_OP expression
    ;

// PUT_LINE(a);    LOGGER('INFO', msg);
// Arguments are token sequences, not the expression grammar, so Spark calls
// such as cast(x AS int) and extract(YEAR FROM x) survive as text.
call_statement
    : identifier (PERIOD identifier)* LEFT_PAREN func_args? RIGHT_PAREN
    ;

set_statement
    : SET raw_token* SEMICOLON
    ;

transaction_statement
    : START TRANSACTION
    | END TRANSACTION
    | COMMIT
    | ROLLBACK
    ;

break_statement:             BREAK;
continue_statement:          CONTINUE;
raise_statement:             RAISE;

// ============================================================================
// Control flow
// ============================================================================

if_statement
    : IF expression body elsif_clause* else_clause? END SEMICOLON?
    ;

elsif_clause
    : (ELSIF | ELSEIF) expression body
    ;

else_clause
    : ELSE body
    ;

while_statement
    : WHILE expression body END SEMICOLON?
    ;

for_statement
    : FOR identifier IN for_source body END SEMICOLON?
    ;

// Range first would also start with '(', so the query form is the alternative
// that requires SELECT or WITH immediately inside the parentheses.
for_source
    : for_query
    | expression DOUBLE_PERIOD expression
    ;

for_query
    : LEFT_PAREN (SELECT | WITH) for_query_item* RIGHT_PAREN
    ;

// Any token except a parenthesis, plus nested parentheses, so
// SELECT * FROM (SELECT 1) t  survives as text.
for_query_item
    : LEFT_PAREN for_query_item* RIGHT_PAREN
    | ~(LEFT_PAREN | RIGHT_PAREN)
    ;

try_statement
    : TRY body CATCH body END SEMICOLON?
    ;

// ============================================================================
// SQL passthrough
// ============================================================================

sql_statement
    : sql_prefix raw_token* SEMICOLON
    ;

sql_prefix
    : SELECT
    | INSERT
    | UPDATE
    | DELETE
    | MERGE
    | TRUNCATE
    | CREATE
    | DROP
    | ALTER
    | WITH
    ;

// One token that is not a statement terminator. Semicolons inside quoted
// strings are part of CHAR_STRING, so they do not end the statement.
raw_token
    : ~(SEMICOLON)
    ;

// ============================================================================
// Expressions
// ============================================================================

expression:                  or_expression;

or_expression
    : and_expression (OR and_expression)*
    ;

and_expression
    : not_expression (AND not_expression)*
    ;

not_expression
    : NOT not_expression
    | predicate
    ;

predicate
    : concatenation predicate_tail?
    ;

predicate_tail
    : relational_operator concatenation
    | NOT? IN LEFT_PAREN expression (COMMA expression)* RIGHT_PAREN
    | NOT? BETWEEN concatenation AND concatenation
    | NOT? LIKE concatenation (ESCAPE concatenation)?
    | IS NOT? NULL_
    ;

relational_operator
    : EQUALS_OP
    | NOT_EQUAL_OP
    | NULL_SAFE_EQUALS
    | LESS_THAN_OP EQUALS_OP?
    | GREATER_THAN_OP EQUALS_OP?
    ;

concatenation
    : additive (CONCAT additive | BAR BAR additive | BAR additive)*
    ;

additive
    : multiplicative ((PLUS_SIGN | MINUS_SIGN) multiplicative)*
    ;

multiplicative
    : unary ((ASTERISK | SOLIDUS | PERCENT) unary)*
    ;

unary
    : (PLUS_SIGN | MINUS_SIGN) unary
    | atom
    ;

atom
    : literal
    | BINDVAR
    | identifier (PERIOD identifier)* (LEFT_PAREN func_args? RIGHT_PAREN)?
    | LEFT_PAREN expression RIGHT_PAREN
    ;

// A call argument is every token up to a comma or parenthesis at this depth.
// Nested parentheses keep their own commas, so assert_equals(if(1 < 2, 'a', 'b'), 'a')
// is two arguments.
func_args
    : func_arg (COMMA func_arg)*
    ;

func_arg
    : func_top+
    ;

func_top
    : LEFT_PAREN func_nested* RIGHT_PAREN
    | ~(LEFT_PAREN | RIGHT_PAREN | COMMA)
    ;

func_nested
    : LEFT_PAREN func_nested* RIGHT_PAREN
    | ~(LEFT_PAREN | RIGHT_PAREN)
    ;

literal
    : numeric
    | CHAR_STRING
    | NATIONAL_CHAR_STRING_LIT
    | NULL_
    | TRUE
    | FALSE
    ;

numeric
    : UNSIGNED_INTEGER
    | APPROXIMATE_NUM_LIT
    ;

identifier
    : REGULAR_ID
    | DELIMITED_ID
    | BACKTICK_ID
    ;
