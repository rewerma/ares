/**
 * Ares PL/SQL Parser (simplified).
 *
 * Trimmed from the original Oracle 11g/12c PL/SQL grammar (Alexandre Porcelli,
 * Ivan Kochurkin, Mark Adams). Only the rules actually consumed by the ares
 * parser visitors are retained. The removed rules (and their reasoning) are:
 *
 *   - block, rule_on_column, identity_clause / identity_options_*,
 *     encryption_spec, query_block, rollup_cube_clause, grouping_sets_elements,
 *     truncate_table (bare), column_collation_name
 *     => never referenced by any visitor; emit "unsupported syntax" instead.
 *
 *   - select_block / insert_block / update_block / delete_block / merge_block
 *     and their create_table_as / truncate_table_block counterparts
 *     => unified via the greedy .*? capture pattern that the visitors consume
 *       via getFullText().
 *
 *   - REVERSE in cursor_loop_param, the cursor_name branch, label_name suffix,
 *     OR exception_name repetition
 *     => present in the grammar but never read by visitors.
 *
 *   - model_expression, interval_expression, quantified_expression,
 *     case_statement / simple_case_statement / searched_case_statement,
 *     other_function (CAST/XMLCAST/EXTRACT), outer_join_sign, INTRODUCER paths
 *     => not exercised by ares test scripts.
 *
 *   - non_reserved_keywords_in_12c / non_reserved_keywords_pre12c
 *     => ~1700 lines of keyword-as-identifier alternatives that the visitor
 *       code never traverses.
 *
 *   - non_reserved_keywords_* shrunk to a minimal "regular_id" set covering
 *     only the keyword tokens still referenced as keywords elsewhere.
 *
 * Licensed under the Apache License, Version 2.0.
 */

parser grammar PlSqlParser;

options {
    tokenVocab=PlSqlLexer;
}

// ============================================================================
// Top-level
// ============================================================================

sql_script
    : ((unit_statement) SEMICOLON?)* EOF
    ;

unit_statement
    : anonymous_body
    | create_procedure_body
    | create_function_body
    | create_table
    | create_table_as
    | set_bleck

    | select_block
    | insert_block
    | update_block
    | delete_block
    | merge_block
    | truncate_table_block

    | call_statement
    ;

// ============================================================================
// PL/SQL program units
// ============================================================================

function_body
    : FUNCTION identifier (LEFT_PAREN parameter (COMMA parameter)* RIGHT_PAREN)?
      RETURN type_spec
      AS (DECLARE? seq_of_declare_specs? body) SEMICOLON
    ;

create_procedure_body
    : CREATE PROCEDURE procedure_name (LEFT_PAREN parameter (COMMA parameter)* RIGHT_PAREN)?
      AS
      (DECLARE? seq_of_declare_specs? body) SEMICOLON
    ;

create_function_body
    : CREATE function_body
    ;

anonymous_body
    : DECLARE? seq_of_declare_specs? body SEMICOLON
    ;

// CREATE TABLE t AS SELECT ...   (visitor takes getFullText())
create_table_as
    : CREATE TABLE table_name AS SELECT .*? SEMICOLON
    ;

// Same shape, used inside a PL body where the surrounding statement provides
// the terminating SEMICOLON. The visitor (PlBodyVisitor) explicitly looks up
// create_table_as2(), so this duplicate rule is load-bearing.
create_table_as2
    : CREATE TABLE table_name AS SELECT .*?
    ;

// CREATE TABLE ... (col_defs) ... WITH ('k'='v', ...)
create_table
    : CREATE
        TABLE table_name
        (relational_table)
        (create_with)
      SEMICOLON
    ;

create_with:                  WITH LEFT_PAREN create_options RIGHT_PAREN;
create_options:              option_ (COMMA option_)*;
option_:                     CHAR_STRING EQUALS_OP CHAR_STRING;

table_name:                  identifier;

relational_table:            (LEFT_PAREN relational_property (COMMA relational_property)* RIGHT_PAREN)?;
relational_property:         column_definition;

// The visitor (PlCreateSourceTableVisitor) only reads column_name and datatype
// here; the rest of the Oracle column-definition syntax is not consumed.
column_definition
    : column_name datatype
    ;

// Top-level SQL DML/DDL blocks (greedy capture, consumed via getFullText()).
truncate_table_block:        TRUNCATE TABLE .*? SEMICOLON;
select_block:                SELECT .*? SEMICOLON;
update_block:                UPDATE .*? SEMICOLON;
delete_block:                DELETE FROM .*? SEMICOLON;
insert_block:                INSERT .*? SEMICOLON;
merge_block:                 MERGE INTO .*? SEMICOLON;
set_bleck:                   SET .*? SEMICOLON;

parameter
    : parameter_name (IN | OUT)* type_spec? default_value_part?
    ;

default_value_part
    : (ASSIGN_OP | DEFAULT) expression
    ;

// ============================================================================
// Declarations
// ============================================================================

seq_of_declare_specs:        declare_spec+;
declare_spec:                variable_declaration;

variable_declaration
    : identifier CONSTANT? type_spec (NOT NULL_)? default_value_part? SEMICOLON
    ;

// ============================================================================
// Statements inside a PL body
// ============================================================================

seq_of_statements:           (statement (SEMICOLON | EOF))+;

statement
    : transaction_statement
    | assignment_statement
    | continue_statement
    | exit_statement
    | if_statement
    | loop_statement
    | raise_statement
    | return_statement
    | sql_statement
    | call_statement
    ;

transaction_statement
    : START TRANSACTION
    | BEGIN TRANSACTION
    | COMMIT
    | ROLLBACK
    ;

assignment_statement:        general_element ASSIGN_OP expression;
continue_statement:          CONTINUE;
exit_statement:              EXIT;

if_statement:                IF condition THEN seq_of_statements elsif_part* else_part? END IF;
elsif_part:                  ELSIF condition THEN seq_of_statements;
else_part:                   ELSE seq_of_statements;

loop_statement
    : (WHILE condition | FOR cursor_loop_param)? LOOP seq_of_statements END LOOP
    ;

// Loop iterator: index IN lower..upper, or record IN (select ...)
cursor_loop_param
    : index_name IN lower_bound DOUBLE_PERIOD upper_bound
    | record_name IN LEFT_PAREN select_statement RIGHT_PAREN
    ;

select_statement:            SELECT .*? SEMICOLON;

lower_bound:                 concatenation;
upper_bound:                 concatenation;

raise_statement:             RAISE exception_name?;
return_statement:            RETURN expression?;

call_statement:              CALL? routine_name function_argument?;

// ============================================================================
// Body & exception handling
// ============================================================================

body
    : BEGIN seq_of_statements (EXCEPTION exception_handler+)? END
    ;

exception_handler
    : WHEN exception_name (OR exception_name)* THEN seq_of_statements
    ;

// ============================================================================
// SQL embedded inside a PL body (greedy capture, consumed via getFullText())
// ============================================================================

sql_statement
    : data_manipulation_language_statements
    ;

data_manipulation_language_statements
    : merge_statement
    | select_statement
    | update_statement
    | delete_statement
    | insert_statement
    | create_table_as2
    | truncate_table_block
    ;

update_statement:            UPDATE .*? SEMICOLON;
delete_statement:            DELETE FROM .*? SEMICOLON;
insert_statement:            INSERT .*? SEMICOLON;
merge_statement:             MERGE INTO .*? SEMICOLON;

// ============================================================================
// Expressions
// ============================================================================

condition:                   expression;
expressions:                 expression (COMMA expression)*;

expression:                  logical_expression;

logical_expression
    : unary_logical_expression
    | logical_expression AND logical_expression
    | logical_expression OR logical_expression
    ;

unary_logical_expression
    : NOT? multiset_expression (IS NOT? NULL_)?
    ;

multiset_expression
    : relational_expression
    ;

relational_expression
    : relational_expression relational_operator relational_expression
    | compound_expression
    ;

compound_expression
    : concatenation
      (NOT? ( IN in_elements
            | BETWEEN between_elements
            | like_type=(LIKE | LIKEC | LIKE2 | LIKE4) concatenation (ESCAPE concatenation)?))?
    ;

relational_operator
    : EQUALS_OP
    | NOT_EQUAL_OP
    | LESS_THAN_OP GREATER_THAN_OP
    | EXCLAMATION_OPERATOR_PART EQUALS_OP
    | CARRET_OPERATOR_PART EQUALS_OP
    | (LESS_THAN_OP | GREATER_THAN_OP) EQUALS_OP?
    ;

in_elements
    : LEFT_PAREN concatenation (COMMA concatenation)* RIGHT_PAREN
    | constant
    | bind_variable
    | general_element
    ;

between_elements:            concatenation AND concatenation;

concatenation
    : concatenation op=ASTERISK concatenation
    | concatenation op=SOLIDUS concatenation
    | concatenation op=PLUS_SIGN concatenation
    | concatenation op=MINUS_SIGN concatenation
    | concatenation op=BAR concatenation
    | concatenation BAR BAR concatenation
    | atom
    ;

unary_expression
    : (MINUS_SIGN | PLUS_SIGN) unary_expression
    | atom
    ;

atom
    : bind_variable
    | constant
    | general_element
    | LEFT_PAREN expressions RIGHT_PAREN
    | quoted_string
    ;

// ============================================================================
// Names and parameters
// ============================================================================

routine_name:                identifier;
parameter_name:              identifier;
procedure_name:              identifier;
exception_name:              identifier;
index_name:                  identifier;
record_name:                 identifier;
column_name:                 identifier;

function_argument:           LEFT_PAREN (argument (COMMA argument)*)? RIGHT_PAREN;
argument:                    expression;

// ============================================================================
// Types
// ============================================================================

type_spec:                   datatype;

datatype:                    native_datatype_element precision_part?;

precision_part
    : LEFT_PAREN numeric (COMMA numeric)? RIGHT_PAREN
    ;

native_datatype_element
    : INT
    | BYTE
    | SMALLINT
    | BIGINT
    | NUMBER
    | DECIMAL
    | DOUBLE
    | FLOAT
    | VARCHAR
    | STRING
    | BOOLEAN
    | DATE
    | TIMESTAMP
    | BINARY
    | BLOB
    ;

bind_variable:               BINDVAR;

general_element:             id_expression (PERIOD id_expression)*;

// ============================================================================
// Literals / constants / identifiers
// ============================================================================

constant
    : TIMESTAMP (quoted_string | bind_variable)
    | numeric
    | DATE quoted_string
    | quoted_string
    | NULL_
    | TRUE
    | FALSE
    | DEFAULT
    ;

numeric:                     UNSIGNED_INTEGER | APPROXIMATE_NUM_LIT;

quoted_string:               CHAR_STRING | NATIONAL_CHAR_STRING_LIT;

identifier:                  id_expression;
id_expression:               regular_id | DELIMITED_ID;

// Minimal keyword set that ares still needs to recognise as identifiers.
// All other keyword tokens are recognised as REGULAR_ID via the lexer.
regular_id
    : REGULAR_ID
    | AS | IS | IN | OUT | AND | OR | NOT | SET | CALL
    | CREATE | TABLE | WITH
    | SELECT | INSERT | UPDATE | DELETE | MERGE | INTO | FROM | WHERE
    | BEGIN | END | LOOP | FOR | WHILE | IF | THEN | ELSE | ELSIF | EXIT
    | CONTINUE | RAISE | RETURN | DECLARE | EXCEPTION | WHEN | PROCEDURE | FUNCTION
    | START | TRANSACTION | COMMIT | ROLLBACK
    | CONSTANT | DEFAULT
    | INT | BIGINT | SMALLINT | BYTE | NUMBER | DECIMAL | DOUBLE | FLOAT
    | VARCHAR | VARCHAR2 | STRING | BOOLEAN | DATE | TIMESTAMP | BINARY | BLOB
    ;
