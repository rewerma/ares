// Generated from /Users/rewerma/Develop/git_workspace/ares/ares-parser/src/main/java/com/github/ares/parser/antlr4/plsql/PlSqlParser.g4 by ANTLR 4.13.1
package com.github.ares.parser.antlr4.plsql;
import com.github.ares.org.antlr.v4.runtime.atn.*;
import com.github.ares.org.antlr.v4.runtime.dfa.DFA;
import com.github.ares.org.antlr.v4.runtime.*;
import com.github.ares.org.antlr.v4.runtime.misc.*;
import com.github.ares.org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast", "CheckReturnValue"})
public class PlSqlParser extends Parser {
	static { RuntimeMetaData.checkVersion("4.13.1", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		CREATE=1, TABLE=2, WITH=3, SET=4, SELECT=5, INSERT=6, UPDATE=7, DELETE=8, 
		MERGE=9, INTO=10, FROM=11, WHERE=12, TRUNCATE=13, PROCEDURE=14, FUNCTION=15, 
		RETURN=16, BEGIN=17, END=18, EXCEPTION=19, WHEN=20, THEN=21, ELSE=22, 
		ELSIF=23, IF=24, LOOP=25, FOR=26, WHILE=27, EXIT=28, CONTINUE=29, RAISE=30, 
		DO=31, DECLARE=32, CALL=33, AS=34, IS=35, START=36, TRANSACTION=37, COMMIT=38, 
		ROLLBACK=39, AND=40, OR=41, NOT=42, NULL_=43, TRUE=44, FALSE=45, BETWEEN=46, 
		LIKE=47, LIKEC=48, LIKE2=49, LIKE4=50, ESCAPE=51, IN=52, OUT=53, CONSTANT=54, 
		DEFAULT=55, INT=56, BYTE=57, SMALLINT=58, BIGINT=59, NUMBER=60, DECIMAL=61, 
		DOUBLE=62, FLOAT=63, VARCHAR=64, VARCHAR2=65, STRING=66, BOOLEAN=67, DATE=68, 
		TIMESTAMP=69, BINARY=70, BLOB=71, LEFT_PAREN=72, RIGHT_PAREN=73, COMMA=74, 
		SEMICOLON=75, COLON=76, PERIOD=77, DOUBLE_PERIOD=78, ASSIGN_OP=79, EQUALS_OP=80, 
		NOT_EQUAL_OP=81, PLUS_SIGN=82, MINUS_SIGN=83, ASTERISK=84, SOLIDUS=85, 
		BAR=86, LESS_THAN_OP=87, GREATER_THAN_OP=88, EXCLAMATION_OPERATOR_PART=89, 
		CARRET_OPERATOR_PART=90, CHAR_STRING=91, NATIONAL_CHAR_STRING_LIT=92, 
		UNSIGNED_INTEGER=93, APPROXIMATE_NUM_LIT=94, DELIMITED_ID=95, BINDVAR=96, 
		REGULAR_ID=97, SINGLE_LINE_COMMENT=98, MULTI_LINE_COMMENT=99, SPACES=100;
	public static final int
		RULE_sql_script = 0, RULE_unit_statement = 1, RULE_function_body = 2, 
		RULE_create_procedure_body = 3, RULE_create_function_body = 4, RULE_anonymous_body = 5, 
		RULE_create_table_as = 6, RULE_create_table_as2 = 7, RULE_create_table = 8, 
		RULE_create_with = 9, RULE_create_options = 10, RULE_option_ = 11, RULE_table_name = 12, 
		RULE_relational_table = 13, RULE_relational_property = 14, RULE_column_definition = 15, 
		RULE_truncate_table_block = 16, RULE_select_block = 17, RULE_update_block = 18, 
		RULE_delete_block = 19, RULE_insert_block = 20, RULE_merge_block = 21, 
		RULE_set_bleck = 22, RULE_parameter = 23, RULE_default_value_part = 24, 
		RULE_seq_of_declare_specs = 25, RULE_declare_spec = 26, RULE_variable_declaration = 27, 
		RULE_seq_of_statements = 28, RULE_statement = 29, RULE_transaction_statement = 30, 
		RULE_assignment_statement = 31, RULE_continue_statement = 32, RULE_exit_statement = 33, 
		RULE_if_statement = 34, RULE_elsif_part = 35, RULE_else_part = 36, RULE_loop_statement = 37, 
		RULE_cursor_loop_param = 38, RULE_select_statement = 39, RULE_lower_bound = 40, 
		RULE_upper_bound = 41, RULE_raise_statement = 42, RULE_return_statement = 43, 
		RULE_call_statement = 44, RULE_body = 45, RULE_exception_handler = 46, 
		RULE_sql_statement = 47, RULE_data_manipulation_language_statements = 48, 
		RULE_update_statement = 49, RULE_delete_statement = 50, RULE_insert_statement = 51, 
		RULE_merge_statement = 52, RULE_condition = 53, RULE_expressions = 54, 
		RULE_expression = 55, RULE_logical_expression = 56, RULE_unary_logical_expression = 57, 
		RULE_multiset_expression = 58, RULE_relational_expression = 59, RULE_compound_expression = 60, 
		RULE_relational_operator = 61, RULE_in_elements = 62, RULE_between_elements = 63, 
		RULE_concatenation = 64, RULE_unary_expression = 65, RULE_atom = 66, RULE_routine_name = 67, 
		RULE_parameter_name = 68, RULE_procedure_name = 69, RULE_exception_name = 70, 
		RULE_index_name = 71, RULE_record_name = 72, RULE_column_name = 73, RULE_function_argument = 74, 
		RULE_argument = 75, RULE_type_spec = 76, RULE_datatype = 77, RULE_precision_part = 78, 
		RULE_native_datatype_element = 79, RULE_bind_variable = 80, RULE_general_element = 81, 
		RULE_constant = 82, RULE_numeric = 83, RULE_quoted_string = 84, RULE_identifier = 85, 
		RULE_id_expression = 86, RULE_regular_id = 87;
	private static String[] makeRuleNames() {
		return new String[] {
			"sql_script", "unit_statement", "function_body", "create_procedure_body", 
			"create_function_body", "anonymous_body", "create_table_as", "create_table_as2", 
			"create_table", "create_with", "create_options", "option_", "table_name", 
			"relational_table", "relational_property", "column_definition", "truncate_table_block", 
			"select_block", "update_block", "delete_block", "insert_block", "merge_block", 
			"set_bleck", "parameter", "default_value_part", "seq_of_declare_specs", 
			"declare_spec", "variable_declaration", "seq_of_statements", "statement", 
			"transaction_statement", "assignment_statement", "continue_statement", 
			"exit_statement", "if_statement", "elsif_part", "else_part", "loop_statement", 
			"cursor_loop_param", "select_statement", "lower_bound", "upper_bound", 
			"raise_statement", "return_statement", "call_statement", "body", "exception_handler", 
			"sql_statement", "data_manipulation_language_statements", "update_statement", 
			"delete_statement", "insert_statement", "merge_statement", "condition", 
			"expressions", "expression", "logical_expression", "unary_logical_expression", 
			"multiset_expression", "relational_expression", "compound_expression", 
			"relational_operator", "in_elements", "between_elements", "concatenation", 
			"unary_expression", "atom", "routine_name", "parameter_name", "procedure_name", 
			"exception_name", "index_name", "record_name", "column_name", "function_argument", 
			"argument", "type_spec", "datatype", "precision_part", "native_datatype_element", 
			"bind_variable", "general_element", "constant", "numeric", "quoted_string", 
			"identifier", "id_expression", "regular_id"
		};
	}
	public static final String[] ruleNames = makeRuleNames();

	private static String[] makeLiteralNames() {
		return new String[] {
			null, "'CREATE'", "'TABLE'", "'WITH'", "'SET'", "'SELECT'", "'INSERT'", 
			"'UPDATE'", "'DELETE'", "'MERGE'", "'INTO'", "'FROM'", "'WHERE'", "'TRUNCATE'", 
			"'PROCEDURE'", "'FUNCTION'", "'RETURN'", "'BEGIN'", "'END'", "'EXCEPTION'", 
			"'WHEN'", "'THEN'", "'ELSE'", "'ELSIF'", "'IF'", "'LOOP'", "'FOR'", "'WHILE'", 
			"'EXIT'", "'CONTINUE'", "'RAISE'", "'DO'", "'DECLARE'", "'CALL'", "'AS'", 
			"'IS'", "'START'", "'TRANSACTION'", "'COMMIT'", "'ROLLBACK'", "'AND'", 
			"'OR'", "'NOT'", "'NULL'", "'TRUE'", "'FALSE'", "'BETWEEN'", "'LIKE'", 
			"'LIKEC'", "'LIKE2'", "'LIKE4'", "'ESCAPE'", "'IN'", "'OUT'", "'CONSTANT'", 
			"'DEFAULT'", "'INT'", "'BYTE'", "'SMALLINT'", "'BIGINT'", "'NUMBER'", 
			"'DECIMAL'", "'DOUBLE'", "'FLOAT'", "'VARCHAR'", "'VARCHAR2'", "'STRING'", 
			"'BOOLEAN'", "'DATE'", "'TIMESTAMP'", "'BINARY'", "'BLOB'", "'('", "')'", 
			"','", "';'", "':'", "'.'", "'..'", "':='", "'='", null, "'+'", "'-'", 
			"'*'", "'/'", "'|'", "'<'", "'>'", "'!'", "'^'"
		};
	}
	private static final String[] _LITERAL_NAMES = makeLiteralNames();
	private static String[] makeSymbolicNames() {
		return new String[] {
			null, "CREATE", "TABLE", "WITH", "SET", "SELECT", "INSERT", "UPDATE", 
			"DELETE", "MERGE", "INTO", "FROM", "WHERE", "TRUNCATE", "PROCEDURE", 
			"FUNCTION", "RETURN", "BEGIN", "END", "EXCEPTION", "WHEN", "THEN", "ELSE", 
			"ELSIF", "IF", "LOOP", "FOR", "WHILE", "EXIT", "CONTINUE", "RAISE", "DO", 
			"DECLARE", "CALL", "AS", "IS", "START", "TRANSACTION", "COMMIT", "ROLLBACK", 
			"AND", "OR", "NOT", "NULL_", "TRUE", "FALSE", "BETWEEN", "LIKE", "LIKEC", 
			"LIKE2", "LIKE4", "ESCAPE", "IN", "OUT", "CONSTANT", "DEFAULT", "INT", 
			"BYTE", "SMALLINT", "BIGINT", "NUMBER", "DECIMAL", "DOUBLE", "FLOAT", 
			"VARCHAR", "VARCHAR2", "STRING", "BOOLEAN", "DATE", "TIMESTAMP", "BINARY", 
			"BLOB", "LEFT_PAREN", "RIGHT_PAREN", "COMMA", "SEMICOLON", "COLON", "PERIOD", 
			"DOUBLE_PERIOD", "ASSIGN_OP", "EQUALS_OP", "NOT_EQUAL_OP", "PLUS_SIGN", 
			"MINUS_SIGN", "ASTERISK", "SOLIDUS", "BAR", "LESS_THAN_OP", "GREATER_THAN_OP", 
			"EXCLAMATION_OPERATOR_PART", "CARRET_OPERATOR_PART", "CHAR_STRING", "NATIONAL_CHAR_STRING_LIT", 
			"UNSIGNED_INTEGER", "APPROXIMATE_NUM_LIT", "DELIMITED_ID", "BINDVAR", 
			"REGULAR_ID", "SINGLE_LINE_COMMENT", "MULTI_LINE_COMMENT", "SPACES"
		};
	}
	private static final String[] _SYMBOLIC_NAMES = makeSymbolicNames();
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}

	@Override
	public String getGrammarFileName() { return "PlSqlParser.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }

	public PlSqlParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Sql_scriptContext extends ParserRuleContext {
		public TerminalNode EOF() { return getToken(PlSqlParser.EOF, 0); }
		public List<Unit_statementContext> unit_statement() {
			return getRuleContexts(Unit_statementContext.class);
		}
		public Unit_statementContext unit_statement(int i) {
			return getRuleContext(Unit_statementContext.class,i);
		}
		public List<TerminalNode> SEMICOLON() { return getTokens(PlSqlParser.SEMICOLON); }
		public TerminalNode SEMICOLON(int i) {
			return getToken(PlSqlParser.SEMICOLON, i);
		}
		public Sql_scriptContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sql_script; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterSql_script(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitSql_script(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitSql_script(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Sql_scriptContext sql_script() throws RecognitionException {
		Sql_scriptContext _localctx = new Sql_scriptContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_sql_script);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(182);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while ((((_la) & ~0x3f) == 0 && ((1L << _la) & -4494805681831938L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & 10737418495L) != 0)) {
				{
				{
				{
				setState(176);
				unit_statement();
				}
				setState(178);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SEMICOLON) {
					{
					setState(177);
					match(SEMICOLON);
					}
				}

				}
				}
				setState(184);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(185);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Unit_statementContext extends ParserRuleContext {
		public Anonymous_bodyContext anonymous_body() {
			return getRuleContext(Anonymous_bodyContext.class,0);
		}
		public Create_procedure_bodyContext create_procedure_body() {
			return getRuleContext(Create_procedure_bodyContext.class,0);
		}
		public Create_function_bodyContext create_function_body() {
			return getRuleContext(Create_function_bodyContext.class,0);
		}
		public Create_tableContext create_table() {
			return getRuleContext(Create_tableContext.class,0);
		}
		public Create_table_asContext create_table_as() {
			return getRuleContext(Create_table_asContext.class,0);
		}
		public Set_bleckContext set_bleck() {
			return getRuleContext(Set_bleckContext.class,0);
		}
		public Select_blockContext select_block() {
			return getRuleContext(Select_blockContext.class,0);
		}
		public Insert_blockContext insert_block() {
			return getRuleContext(Insert_blockContext.class,0);
		}
		public Update_blockContext update_block() {
			return getRuleContext(Update_blockContext.class,0);
		}
		public Delete_blockContext delete_block() {
			return getRuleContext(Delete_blockContext.class,0);
		}
		public Merge_blockContext merge_block() {
			return getRuleContext(Merge_blockContext.class,0);
		}
		public Truncate_table_blockContext truncate_table_block() {
			return getRuleContext(Truncate_table_blockContext.class,0);
		}
		public Call_statementContext call_statement() {
			return getRuleContext(Call_statementContext.class,0);
		}
		public Unit_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_unit_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterUnit_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitUnit_statement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitUnit_statement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Unit_statementContext unit_statement() throws RecognitionException {
		Unit_statementContext _localctx = new Unit_statementContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_unit_statement);
		try {
			setState(200);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,2,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(187);
				anonymous_body();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(188);
				create_procedure_body();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(189);
				create_function_body();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(190);
				create_table();
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(191);
				create_table_as();
				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(192);
				set_bleck();
				}
				break;
			case 7:
				enterOuterAlt(_localctx, 7);
				{
				setState(193);
				select_block();
				}
				break;
			case 8:
				enterOuterAlt(_localctx, 8);
				{
				setState(194);
				insert_block();
				}
				break;
			case 9:
				enterOuterAlt(_localctx, 9);
				{
				setState(195);
				update_block();
				}
				break;
			case 10:
				enterOuterAlt(_localctx, 10);
				{
				setState(196);
				delete_block();
				}
				break;
			case 11:
				enterOuterAlt(_localctx, 11);
				{
				setState(197);
				merge_block();
				}
				break;
			case 12:
				enterOuterAlt(_localctx, 12);
				{
				setState(198);
				truncate_table_block();
				}
				break;
			case 13:
				enterOuterAlt(_localctx, 13);
				{
				setState(199);
				call_statement();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Function_bodyContext extends ParserRuleContext {
		public TerminalNode FUNCTION() { return getToken(PlSqlParser.FUNCTION, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode RETURN() { return getToken(PlSqlParser.RETURN, 0); }
		public Type_specContext type_spec() {
			return getRuleContext(Type_specContext.class,0);
		}
		public TerminalNode AS() { return getToken(PlSqlParser.AS, 0); }
		public TerminalNode SEMICOLON() { return getToken(PlSqlParser.SEMICOLON, 0); }
		public BodyContext body() {
			return getRuleContext(BodyContext.class,0);
		}
		public TerminalNode LEFT_PAREN() { return getToken(PlSqlParser.LEFT_PAREN, 0); }
		public List<ParameterContext> parameter() {
			return getRuleContexts(ParameterContext.class);
		}
		public ParameterContext parameter(int i) {
			return getRuleContext(ParameterContext.class,i);
		}
		public TerminalNode RIGHT_PAREN() { return getToken(PlSqlParser.RIGHT_PAREN, 0); }
		public TerminalNode DECLARE() { return getToken(PlSqlParser.DECLARE, 0); }
		public Seq_of_declare_specsContext seq_of_declare_specs() {
			return getRuleContext(Seq_of_declare_specsContext.class,0);
		}
		public List<TerminalNode> COMMA() { return getTokens(PlSqlParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(PlSqlParser.COMMA, i);
		}
		public Function_bodyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_function_body; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterFunction_body(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitFunction_body(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitFunction_body(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Function_bodyContext function_body() throws RecognitionException {
		Function_bodyContext _localctx = new Function_bodyContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_function_body);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(202);
			match(FUNCTION);
			setState(203);
			identifier();
			setState(215);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==LEFT_PAREN) {
				{
				setState(204);
				match(LEFT_PAREN);
				setState(205);
				parameter();
				setState(210);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(206);
					match(COMMA);
					setState(207);
					parameter();
					}
					}
					setState(212);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(213);
				match(RIGHT_PAREN);
				}
			}

			setState(217);
			match(RETURN);
			setState(218);
			type_spec();
			setState(219);
			match(AS);
			{
			setState(221);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,5,_ctx) ) {
			case 1:
				{
				setState(220);
				match(DECLARE);
				}
				break;
			}
			setState(224);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,6,_ctx) ) {
			case 1:
				{
				setState(223);
				seq_of_declare_specs();
				}
				break;
			}
			setState(226);
			body();
			}
			setState(228);
			match(SEMICOLON);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Create_procedure_bodyContext extends ParserRuleContext {
		public TerminalNode CREATE() { return getToken(PlSqlParser.CREATE, 0); }
		public TerminalNode PROCEDURE() { return getToken(PlSqlParser.PROCEDURE, 0); }
		public Procedure_nameContext procedure_name() {
			return getRuleContext(Procedure_nameContext.class,0);
		}
		public TerminalNode AS() { return getToken(PlSqlParser.AS, 0); }
		public TerminalNode SEMICOLON() { return getToken(PlSqlParser.SEMICOLON, 0); }
		public BodyContext body() {
			return getRuleContext(BodyContext.class,0);
		}
		public TerminalNode LEFT_PAREN() { return getToken(PlSqlParser.LEFT_PAREN, 0); }
		public List<ParameterContext> parameter() {
			return getRuleContexts(ParameterContext.class);
		}
		public ParameterContext parameter(int i) {
			return getRuleContext(ParameterContext.class,i);
		}
		public TerminalNode RIGHT_PAREN() { return getToken(PlSqlParser.RIGHT_PAREN, 0); }
		public TerminalNode DECLARE() { return getToken(PlSqlParser.DECLARE, 0); }
		public Seq_of_declare_specsContext seq_of_declare_specs() {
			return getRuleContext(Seq_of_declare_specsContext.class,0);
		}
		public List<TerminalNode> COMMA() { return getTokens(PlSqlParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(PlSqlParser.COMMA, i);
		}
		public Create_procedure_bodyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_create_procedure_body; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterCreate_procedure_body(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitCreate_procedure_body(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitCreate_procedure_body(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Create_procedure_bodyContext create_procedure_body() throws RecognitionException {
		Create_procedure_bodyContext _localctx = new Create_procedure_bodyContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_create_procedure_body);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(230);
			match(CREATE);
			setState(231);
			match(PROCEDURE);
			setState(232);
			procedure_name();
			setState(244);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==LEFT_PAREN) {
				{
				setState(233);
				match(LEFT_PAREN);
				setState(234);
				parameter();
				setState(239);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(235);
					match(COMMA);
					setState(236);
					parameter();
					}
					}
					setState(241);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(242);
				match(RIGHT_PAREN);
				}
			}

			setState(246);
			match(AS);
			{
			setState(248);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,9,_ctx) ) {
			case 1:
				{
				setState(247);
				match(DECLARE);
				}
				break;
			}
			setState(251);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,10,_ctx) ) {
			case 1:
				{
				setState(250);
				seq_of_declare_specs();
				}
				break;
			}
			setState(253);
			body();
			}
			setState(255);
			match(SEMICOLON);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Create_function_bodyContext extends ParserRuleContext {
		public TerminalNode CREATE() { return getToken(PlSqlParser.CREATE, 0); }
		public Function_bodyContext function_body() {
			return getRuleContext(Function_bodyContext.class,0);
		}
		public Create_function_bodyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_create_function_body; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterCreate_function_body(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitCreate_function_body(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitCreate_function_body(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Create_function_bodyContext create_function_body() throws RecognitionException {
		Create_function_bodyContext _localctx = new Create_function_bodyContext(_ctx, getState());
		enterRule(_localctx, 8, RULE_create_function_body);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(257);
			match(CREATE);
			setState(258);
			function_body();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Anonymous_bodyContext extends ParserRuleContext {
		public BodyContext body() {
			return getRuleContext(BodyContext.class,0);
		}
		public TerminalNode SEMICOLON() { return getToken(PlSqlParser.SEMICOLON, 0); }
		public TerminalNode DECLARE() { return getToken(PlSqlParser.DECLARE, 0); }
		public Seq_of_declare_specsContext seq_of_declare_specs() {
			return getRuleContext(Seq_of_declare_specsContext.class,0);
		}
		public Anonymous_bodyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_anonymous_body; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterAnonymous_body(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitAnonymous_body(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitAnonymous_body(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Anonymous_bodyContext anonymous_body() throws RecognitionException {
		Anonymous_bodyContext _localctx = new Anonymous_bodyContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_anonymous_body);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(261);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,11,_ctx) ) {
			case 1:
				{
				setState(260);
				match(DECLARE);
				}
				break;
			}
			setState(264);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,12,_ctx) ) {
			case 1:
				{
				setState(263);
				seq_of_declare_specs();
				}
				break;
			}
			setState(266);
			body();
			setState(267);
			match(SEMICOLON);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Create_table_asContext extends ParserRuleContext {
		public TerminalNode CREATE() { return getToken(PlSqlParser.CREATE, 0); }
		public TerminalNode TABLE() { return getToken(PlSqlParser.TABLE, 0); }
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public TerminalNode AS() { return getToken(PlSqlParser.AS, 0); }
		public TerminalNode SELECT() { return getToken(PlSqlParser.SELECT, 0); }
		public TerminalNode SEMICOLON() { return getToken(PlSqlParser.SEMICOLON, 0); }
		public Create_table_asContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_create_table_as; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterCreate_table_as(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitCreate_table_as(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitCreate_table_as(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Create_table_asContext create_table_as() throws RecognitionException {
		Create_table_asContext _localctx = new Create_table_asContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_create_table_as);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(269);
			match(CREATE);
			setState(270);
			match(TABLE);
			setState(271);
			table_name();
			setState(272);
			match(AS);
			setState(273);
			match(SELECT);
			setState(277);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,13,_ctx);
			while ( _alt!=1 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1+1 ) {
					{
					{
					setState(274);
					matchWildcard();
					}
					} 
				}
				setState(279);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,13,_ctx);
			}
			setState(280);
			match(SEMICOLON);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Create_table_as2Context extends ParserRuleContext {
		public TerminalNode CREATE() { return getToken(PlSqlParser.CREATE, 0); }
		public TerminalNode TABLE() { return getToken(PlSqlParser.TABLE, 0); }
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public TerminalNode AS() { return getToken(PlSqlParser.AS, 0); }
		public TerminalNode SELECT() { return getToken(PlSqlParser.SELECT, 0); }
		public Create_table_as2Context(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_create_table_as2; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterCreate_table_as2(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitCreate_table_as2(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitCreate_table_as2(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Create_table_as2Context create_table_as2() throws RecognitionException {
		Create_table_as2Context _localctx = new Create_table_as2Context(_ctx, getState());
		enterRule(_localctx, 14, RULE_create_table_as2);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(282);
			match(CREATE);
			setState(283);
			match(TABLE);
			setState(284);
			table_name();
			setState(285);
			match(AS);
			setState(286);
			match(SELECT);
			setState(290);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,14,_ctx);
			while ( _alt!=1 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1+1 ) {
					{
					{
					setState(287);
					matchWildcard();
					}
					} 
				}
				setState(292);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,14,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Create_tableContext extends ParserRuleContext {
		public TerminalNode CREATE() { return getToken(PlSqlParser.CREATE, 0); }
		public TerminalNode TABLE() { return getToken(PlSqlParser.TABLE, 0); }
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public TerminalNode SEMICOLON() { return getToken(PlSqlParser.SEMICOLON, 0); }
		public Relational_tableContext relational_table() {
			return getRuleContext(Relational_tableContext.class,0);
		}
		public Create_withContext create_with() {
			return getRuleContext(Create_withContext.class,0);
		}
		public Create_tableContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_create_table; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterCreate_table(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitCreate_table(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitCreate_table(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Create_tableContext create_table() throws RecognitionException {
		Create_tableContext _localctx = new Create_tableContext(_ctx, getState());
		enterRule(_localctx, 16, RULE_create_table);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(293);
			match(CREATE);
			setState(294);
			match(TABLE);
			setState(295);
			table_name();
			{
			setState(296);
			relational_table();
			}
			{
			setState(297);
			create_with();
			}
			setState(298);
			match(SEMICOLON);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Create_withContext extends ParserRuleContext {
		public TerminalNode WITH() { return getToken(PlSqlParser.WITH, 0); }
		public TerminalNode LEFT_PAREN() { return getToken(PlSqlParser.LEFT_PAREN, 0); }
		public Create_optionsContext create_options() {
			return getRuleContext(Create_optionsContext.class,0);
		}
		public TerminalNode RIGHT_PAREN() { return getToken(PlSqlParser.RIGHT_PAREN, 0); }
		public Create_withContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_create_with; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterCreate_with(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitCreate_with(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitCreate_with(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Create_withContext create_with() throws RecognitionException {
		Create_withContext _localctx = new Create_withContext(_ctx, getState());
		enterRule(_localctx, 18, RULE_create_with);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(300);
			match(WITH);
			setState(301);
			match(LEFT_PAREN);
			setState(302);
			create_options();
			setState(303);
			match(RIGHT_PAREN);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Create_optionsContext extends ParserRuleContext {
		public List<Option_Context> option_() {
			return getRuleContexts(Option_Context.class);
		}
		public Option_Context option_(int i) {
			return getRuleContext(Option_Context.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(PlSqlParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(PlSqlParser.COMMA, i);
		}
		public Create_optionsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_create_options; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterCreate_options(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitCreate_options(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitCreate_options(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Create_optionsContext create_options() throws RecognitionException {
		Create_optionsContext _localctx = new Create_optionsContext(_ctx, getState());
		enterRule(_localctx, 20, RULE_create_options);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(305);
			option_();
			setState(310);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(306);
				match(COMMA);
				setState(307);
				option_();
				}
				}
				setState(312);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Option_Context extends ParserRuleContext {
		public List<TerminalNode> CHAR_STRING() { return getTokens(PlSqlParser.CHAR_STRING); }
		public TerminalNode CHAR_STRING(int i) {
			return getToken(PlSqlParser.CHAR_STRING, i);
		}
		public TerminalNode EQUALS_OP() { return getToken(PlSqlParser.EQUALS_OP, 0); }
		public Option_Context(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_option_; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterOption_(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitOption_(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitOption_(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Option_Context option_() throws RecognitionException {
		Option_Context _localctx = new Option_Context(_ctx, getState());
		enterRule(_localctx, 22, RULE_option_);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(313);
			match(CHAR_STRING);
			setState(314);
			match(EQUALS_OP);
			setState(315);
			match(CHAR_STRING);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Table_nameContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public Table_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_table_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterTable_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitTable_name(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitTable_name(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Table_nameContext table_name() throws RecognitionException {
		Table_nameContext _localctx = new Table_nameContext(_ctx, getState());
		enterRule(_localctx, 24, RULE_table_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(317);
			identifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Relational_tableContext extends ParserRuleContext {
		public TerminalNode LEFT_PAREN() { return getToken(PlSqlParser.LEFT_PAREN, 0); }
		public List<Relational_propertyContext> relational_property() {
			return getRuleContexts(Relational_propertyContext.class);
		}
		public Relational_propertyContext relational_property(int i) {
			return getRuleContext(Relational_propertyContext.class,i);
		}
		public TerminalNode RIGHT_PAREN() { return getToken(PlSqlParser.RIGHT_PAREN, 0); }
		public List<TerminalNode> COMMA() { return getTokens(PlSqlParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(PlSqlParser.COMMA, i);
		}
		public Relational_tableContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_relational_table; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterRelational_table(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitRelational_table(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitRelational_table(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Relational_tableContext relational_table() throws RecognitionException {
		Relational_tableContext _localctx = new Relational_tableContext(_ctx, getState());
		enterRule(_localctx, 26, RULE_relational_table);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(330);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==LEFT_PAREN) {
				{
				setState(319);
				match(LEFT_PAREN);
				setState(320);
				relational_property();
				setState(325);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(321);
					match(COMMA);
					setState(322);
					relational_property();
					}
					}
					setState(327);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(328);
				match(RIGHT_PAREN);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Relational_propertyContext extends ParserRuleContext {
		public Column_definitionContext column_definition() {
			return getRuleContext(Column_definitionContext.class,0);
		}
		public Relational_propertyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_relational_property; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterRelational_property(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitRelational_property(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitRelational_property(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Relational_propertyContext relational_property() throws RecognitionException {
		Relational_propertyContext _localctx = new Relational_propertyContext(_ctx, getState());
		enterRule(_localctx, 28, RULE_relational_property);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(332);
			column_definition();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Column_definitionContext extends ParserRuleContext {
		public Column_nameContext column_name() {
			return getRuleContext(Column_nameContext.class,0);
		}
		public DatatypeContext datatype() {
			return getRuleContext(DatatypeContext.class,0);
		}
		public Column_definitionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_column_definition; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterColumn_definition(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitColumn_definition(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitColumn_definition(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Column_definitionContext column_definition() throws RecognitionException {
		Column_definitionContext _localctx = new Column_definitionContext(_ctx, getState());
		enterRule(_localctx, 30, RULE_column_definition);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(334);
			column_name();
			setState(335);
			datatype();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Truncate_table_blockContext extends ParserRuleContext {
		public TerminalNode TRUNCATE() { return getToken(PlSqlParser.TRUNCATE, 0); }
		public TerminalNode TABLE() { return getToken(PlSqlParser.TABLE, 0); }
		public TerminalNode SEMICOLON() { return getToken(PlSqlParser.SEMICOLON, 0); }
		public Truncate_table_blockContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_truncate_table_block; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterTruncate_table_block(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitTruncate_table_block(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitTruncate_table_block(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Truncate_table_blockContext truncate_table_block() throws RecognitionException {
		Truncate_table_blockContext _localctx = new Truncate_table_blockContext(_ctx, getState());
		enterRule(_localctx, 32, RULE_truncate_table_block);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(337);
			match(TRUNCATE);
			setState(338);
			match(TABLE);
			setState(342);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,18,_ctx);
			while ( _alt!=1 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1+1 ) {
					{
					{
					setState(339);
					matchWildcard();
					}
					} 
				}
				setState(344);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,18,_ctx);
			}
			setState(345);
			match(SEMICOLON);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Select_blockContext extends ParserRuleContext {
		public TerminalNode SELECT() { return getToken(PlSqlParser.SELECT, 0); }
		public TerminalNode SEMICOLON() { return getToken(PlSqlParser.SEMICOLON, 0); }
		public Select_blockContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_select_block; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterSelect_block(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitSelect_block(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitSelect_block(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Select_blockContext select_block() throws RecognitionException {
		Select_blockContext _localctx = new Select_blockContext(_ctx, getState());
		enterRule(_localctx, 34, RULE_select_block);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(347);
			match(SELECT);
			setState(351);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,19,_ctx);
			while ( _alt!=1 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1+1 ) {
					{
					{
					setState(348);
					matchWildcard();
					}
					} 
				}
				setState(353);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,19,_ctx);
			}
			setState(354);
			match(SEMICOLON);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Update_blockContext extends ParserRuleContext {
		public TerminalNode UPDATE() { return getToken(PlSqlParser.UPDATE, 0); }
		public TerminalNode SEMICOLON() { return getToken(PlSqlParser.SEMICOLON, 0); }
		public Update_blockContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_update_block; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterUpdate_block(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitUpdate_block(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitUpdate_block(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Update_blockContext update_block() throws RecognitionException {
		Update_blockContext _localctx = new Update_blockContext(_ctx, getState());
		enterRule(_localctx, 36, RULE_update_block);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(356);
			match(UPDATE);
			setState(360);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,20,_ctx);
			while ( _alt!=1 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1+1 ) {
					{
					{
					setState(357);
					matchWildcard();
					}
					} 
				}
				setState(362);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,20,_ctx);
			}
			setState(363);
			match(SEMICOLON);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Delete_blockContext extends ParserRuleContext {
		public TerminalNode DELETE() { return getToken(PlSqlParser.DELETE, 0); }
		public TerminalNode FROM() { return getToken(PlSqlParser.FROM, 0); }
		public TerminalNode SEMICOLON() { return getToken(PlSqlParser.SEMICOLON, 0); }
		public Delete_blockContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_delete_block; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterDelete_block(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitDelete_block(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitDelete_block(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Delete_blockContext delete_block() throws RecognitionException {
		Delete_blockContext _localctx = new Delete_blockContext(_ctx, getState());
		enterRule(_localctx, 38, RULE_delete_block);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(365);
			match(DELETE);
			setState(366);
			match(FROM);
			setState(370);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,21,_ctx);
			while ( _alt!=1 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1+1 ) {
					{
					{
					setState(367);
					matchWildcard();
					}
					} 
				}
				setState(372);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,21,_ctx);
			}
			setState(373);
			match(SEMICOLON);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Insert_blockContext extends ParserRuleContext {
		public TerminalNode INSERT() { return getToken(PlSqlParser.INSERT, 0); }
		public TerminalNode SEMICOLON() { return getToken(PlSqlParser.SEMICOLON, 0); }
		public Insert_blockContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_insert_block; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterInsert_block(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitInsert_block(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitInsert_block(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Insert_blockContext insert_block() throws RecognitionException {
		Insert_blockContext _localctx = new Insert_blockContext(_ctx, getState());
		enterRule(_localctx, 40, RULE_insert_block);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(375);
			match(INSERT);
			setState(379);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,22,_ctx);
			while ( _alt!=1 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1+1 ) {
					{
					{
					setState(376);
					matchWildcard();
					}
					} 
				}
				setState(381);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,22,_ctx);
			}
			setState(382);
			match(SEMICOLON);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Merge_blockContext extends ParserRuleContext {
		public TerminalNode MERGE() { return getToken(PlSqlParser.MERGE, 0); }
		public TerminalNode INTO() { return getToken(PlSqlParser.INTO, 0); }
		public TerminalNode SEMICOLON() { return getToken(PlSqlParser.SEMICOLON, 0); }
		public Merge_blockContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_merge_block; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterMerge_block(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitMerge_block(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitMerge_block(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Merge_blockContext merge_block() throws RecognitionException {
		Merge_blockContext _localctx = new Merge_blockContext(_ctx, getState());
		enterRule(_localctx, 42, RULE_merge_block);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(384);
			match(MERGE);
			setState(385);
			match(INTO);
			setState(389);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,23,_ctx);
			while ( _alt!=1 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1+1 ) {
					{
					{
					setState(386);
					matchWildcard();
					}
					} 
				}
				setState(391);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,23,_ctx);
			}
			setState(392);
			match(SEMICOLON);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Set_bleckContext extends ParserRuleContext {
		public TerminalNode SET() { return getToken(PlSqlParser.SET, 0); }
		public TerminalNode SEMICOLON() { return getToken(PlSqlParser.SEMICOLON, 0); }
		public Set_bleckContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_set_bleck; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterSet_bleck(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitSet_bleck(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitSet_bleck(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Set_bleckContext set_bleck() throws RecognitionException {
		Set_bleckContext _localctx = new Set_bleckContext(_ctx, getState());
		enterRule(_localctx, 44, RULE_set_bleck);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(394);
			match(SET);
			setState(398);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,24,_ctx);
			while ( _alt!=1 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1+1 ) {
					{
					{
					setState(395);
					matchWildcard();
					}
					} 
				}
				setState(400);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,24,_ctx);
			}
			setState(401);
			match(SEMICOLON);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ParameterContext extends ParserRuleContext {
		public Parameter_nameContext parameter_name() {
			return getRuleContext(Parameter_nameContext.class,0);
		}
		public Type_specContext type_spec() {
			return getRuleContext(Type_specContext.class,0);
		}
		public Default_value_partContext default_value_part() {
			return getRuleContext(Default_value_partContext.class,0);
		}
		public List<TerminalNode> IN() { return getTokens(PlSqlParser.IN); }
		public TerminalNode IN(int i) {
			return getToken(PlSqlParser.IN, i);
		}
		public List<TerminalNode> OUT() { return getTokens(PlSqlParser.OUT); }
		public TerminalNode OUT(int i) {
			return getToken(PlSqlParser.OUT, i);
		}
		public ParameterContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_parameter; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterParameter(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitParameter(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitParameter(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ParameterContext parameter() throws RecognitionException {
		ParameterContext _localctx = new ParameterContext(_ctx, getState());
		enterRule(_localctx, 46, RULE_parameter);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(403);
			parameter_name();
			setState(407);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==IN || _la==OUT) {
				{
				{
				setState(404);
				_la = _input.LA(1);
				if ( !(_la==IN || _la==OUT) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
				}
				setState(409);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(411);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (((((_la - 56)) & ~0x3f) == 0 && ((1L << (_la - 56)) & 65023L) != 0)) {
				{
				setState(410);
				type_spec();
				}
			}

			setState(414);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==DEFAULT || _la==ASSIGN_OP) {
				{
				setState(413);
				default_value_part();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Default_value_partContext extends ParserRuleContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode ASSIGN_OP() { return getToken(PlSqlParser.ASSIGN_OP, 0); }
		public TerminalNode DEFAULT() { return getToken(PlSqlParser.DEFAULT, 0); }
		public Default_value_partContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_default_value_part; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterDefault_value_part(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitDefault_value_part(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitDefault_value_part(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Default_value_partContext default_value_part() throws RecognitionException {
		Default_value_partContext _localctx = new Default_value_partContext(_ctx, getState());
		enterRule(_localctx, 48, RULE_default_value_part);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(416);
			_la = _input.LA(1);
			if ( !(_la==DEFAULT || _la==ASSIGN_OP) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			setState(417);
			expression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Seq_of_declare_specsContext extends ParserRuleContext {
		public List<Declare_specContext> declare_spec() {
			return getRuleContexts(Declare_specContext.class);
		}
		public Declare_specContext declare_spec(int i) {
			return getRuleContext(Declare_specContext.class,i);
		}
		public Seq_of_declare_specsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_seq_of_declare_specs; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterSeq_of_declare_specs(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitSeq_of_declare_specs(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitSeq_of_declare_specs(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Seq_of_declare_specsContext seq_of_declare_specs() throws RecognitionException {
		Seq_of_declare_specsContext _localctx = new Seq_of_declare_specsContext(_ctx, getState());
		enterRule(_localctx, 50, RULE_seq_of_declare_specs);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(420); 
			_errHandler.sync(this);
			_alt = 1;
			do {
				switch (_alt) {
				case 1:
					{
					{
					setState(419);
					declare_spec();
					}
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(422); 
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,28,_ctx);
			} while ( _alt!=2 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Declare_specContext extends ParserRuleContext {
		public Variable_declarationContext variable_declaration() {
			return getRuleContext(Variable_declarationContext.class,0);
		}
		public Declare_specContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_declare_spec; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterDeclare_spec(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitDeclare_spec(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitDeclare_spec(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Declare_specContext declare_spec() throws RecognitionException {
		Declare_specContext _localctx = new Declare_specContext(_ctx, getState());
		enterRule(_localctx, 52, RULE_declare_spec);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(424);
			variable_declaration();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Variable_declarationContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public Type_specContext type_spec() {
			return getRuleContext(Type_specContext.class,0);
		}
		public TerminalNode SEMICOLON() { return getToken(PlSqlParser.SEMICOLON, 0); }
		public TerminalNode CONSTANT() { return getToken(PlSqlParser.CONSTANT, 0); }
		public TerminalNode NOT() { return getToken(PlSqlParser.NOT, 0); }
		public TerminalNode NULL_() { return getToken(PlSqlParser.NULL_, 0); }
		public Default_value_partContext default_value_part() {
			return getRuleContext(Default_value_partContext.class,0);
		}
		public Variable_declarationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_variable_declaration; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterVariable_declaration(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitVariable_declaration(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitVariable_declaration(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Variable_declarationContext variable_declaration() throws RecognitionException {
		Variable_declarationContext _localctx = new Variable_declarationContext(_ctx, getState());
		enterRule(_localctx, 54, RULE_variable_declaration);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(426);
			identifier();
			setState(428);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==CONSTANT) {
				{
				setState(427);
				match(CONSTANT);
				}
			}

			setState(430);
			type_spec();
			setState(433);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==NOT) {
				{
				setState(431);
				match(NOT);
				setState(432);
				match(NULL_);
				}
			}

			setState(436);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==DEFAULT || _la==ASSIGN_OP) {
				{
				setState(435);
				default_value_part();
				}
			}

			setState(438);
			match(SEMICOLON);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Seq_of_statementsContext extends ParserRuleContext {
		public List<StatementContext> statement() {
			return getRuleContexts(StatementContext.class);
		}
		public StatementContext statement(int i) {
			return getRuleContext(StatementContext.class,i);
		}
		public List<TerminalNode> SEMICOLON() { return getTokens(PlSqlParser.SEMICOLON); }
		public TerminalNode SEMICOLON(int i) {
			return getToken(PlSqlParser.SEMICOLON, i);
		}
		public List<TerminalNode> EOF() { return getTokens(PlSqlParser.EOF); }
		public TerminalNode EOF(int i) {
			return getToken(PlSqlParser.EOF, i);
		}
		public Seq_of_statementsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_seq_of_statements; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterSeq_of_statements(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitSeq_of_statements(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitSeq_of_statements(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Seq_of_statementsContext seq_of_statements() throws RecognitionException {
		Seq_of_statementsContext _localctx = new Seq_of_statementsContext(_ctx, getState());
		enterRule(_localctx, 56, RULE_seq_of_statements);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(443); 
			_errHandler.sync(this);
			_alt = 1;
			do {
				switch (_alt) {
				case 1:
					{
					{
					setState(440);
					statement();
					setState(441);
					_la = _input.LA(1);
					if ( !(_la==EOF || _la==SEMICOLON) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(445); 
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,32,_ctx);
			} while ( _alt!=2 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class StatementContext extends ParserRuleContext {
		public Transaction_statementContext transaction_statement() {
			return getRuleContext(Transaction_statementContext.class,0);
		}
		public Assignment_statementContext assignment_statement() {
			return getRuleContext(Assignment_statementContext.class,0);
		}
		public Continue_statementContext continue_statement() {
			return getRuleContext(Continue_statementContext.class,0);
		}
		public Exit_statementContext exit_statement() {
			return getRuleContext(Exit_statementContext.class,0);
		}
		public If_statementContext if_statement() {
			return getRuleContext(If_statementContext.class,0);
		}
		public Loop_statementContext loop_statement() {
			return getRuleContext(Loop_statementContext.class,0);
		}
		public Raise_statementContext raise_statement() {
			return getRuleContext(Raise_statementContext.class,0);
		}
		public Return_statementContext return_statement() {
			return getRuleContext(Return_statementContext.class,0);
		}
		public Sql_statementContext sql_statement() {
			return getRuleContext(Sql_statementContext.class,0);
		}
		public Call_statementContext call_statement() {
			return getRuleContext(Call_statementContext.class,0);
		}
		public StatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitStatement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitStatement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final StatementContext statement() throws RecognitionException {
		StatementContext _localctx = new StatementContext(_ctx, getState());
		enterRule(_localctx, 58, RULE_statement);
		try {
			setState(457);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,33,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(447);
				transaction_statement();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(448);
				assignment_statement();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(449);
				continue_statement();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(450);
				exit_statement();
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(451);
				if_statement();
				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(452);
				loop_statement();
				}
				break;
			case 7:
				enterOuterAlt(_localctx, 7);
				{
				setState(453);
				raise_statement();
				}
				break;
			case 8:
				enterOuterAlt(_localctx, 8);
				{
				setState(454);
				return_statement();
				}
				break;
			case 9:
				enterOuterAlt(_localctx, 9);
				{
				setState(455);
				sql_statement();
				}
				break;
			case 10:
				enterOuterAlt(_localctx, 10);
				{
				setState(456);
				call_statement();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Transaction_statementContext extends ParserRuleContext {
		public TerminalNode START() { return getToken(PlSqlParser.START, 0); }
		public TerminalNode TRANSACTION() { return getToken(PlSqlParser.TRANSACTION, 0); }
		public TerminalNode BEGIN() { return getToken(PlSqlParser.BEGIN, 0); }
		public TerminalNode COMMIT() { return getToken(PlSqlParser.COMMIT, 0); }
		public TerminalNode ROLLBACK() { return getToken(PlSqlParser.ROLLBACK, 0); }
		public Transaction_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_transaction_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterTransaction_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitTransaction_statement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitTransaction_statement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Transaction_statementContext transaction_statement() throws RecognitionException {
		Transaction_statementContext _localctx = new Transaction_statementContext(_ctx, getState());
		enterRule(_localctx, 60, RULE_transaction_statement);
		try {
			setState(465);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case START:
				enterOuterAlt(_localctx, 1);
				{
				setState(459);
				match(START);
				setState(460);
				match(TRANSACTION);
				}
				break;
			case BEGIN:
				enterOuterAlt(_localctx, 2);
				{
				setState(461);
				match(BEGIN);
				setState(462);
				match(TRANSACTION);
				}
				break;
			case COMMIT:
				enterOuterAlt(_localctx, 3);
				{
				setState(463);
				match(COMMIT);
				}
				break;
			case ROLLBACK:
				enterOuterAlt(_localctx, 4);
				{
				setState(464);
				match(ROLLBACK);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Assignment_statementContext extends ParserRuleContext {
		public General_elementContext general_element() {
			return getRuleContext(General_elementContext.class,0);
		}
		public TerminalNode ASSIGN_OP() { return getToken(PlSqlParser.ASSIGN_OP, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public Assignment_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_assignment_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterAssignment_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitAssignment_statement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitAssignment_statement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Assignment_statementContext assignment_statement() throws RecognitionException {
		Assignment_statementContext _localctx = new Assignment_statementContext(_ctx, getState());
		enterRule(_localctx, 62, RULE_assignment_statement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(467);
			general_element();
			setState(468);
			match(ASSIGN_OP);
			setState(469);
			expression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Continue_statementContext extends ParserRuleContext {
		public TerminalNode CONTINUE() { return getToken(PlSqlParser.CONTINUE, 0); }
		public Continue_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_continue_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterContinue_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitContinue_statement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitContinue_statement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Continue_statementContext continue_statement() throws RecognitionException {
		Continue_statementContext _localctx = new Continue_statementContext(_ctx, getState());
		enterRule(_localctx, 64, RULE_continue_statement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(471);
			match(CONTINUE);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Exit_statementContext extends ParserRuleContext {
		public TerminalNode EXIT() { return getToken(PlSqlParser.EXIT, 0); }
		public Exit_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_exit_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterExit_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitExit_statement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitExit_statement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Exit_statementContext exit_statement() throws RecognitionException {
		Exit_statementContext _localctx = new Exit_statementContext(_ctx, getState());
		enterRule(_localctx, 66, RULE_exit_statement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(473);
			match(EXIT);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class If_statementContext extends ParserRuleContext {
		public List<TerminalNode> IF() { return getTokens(PlSqlParser.IF); }
		public TerminalNode IF(int i) {
			return getToken(PlSqlParser.IF, i);
		}
		public ConditionContext condition() {
			return getRuleContext(ConditionContext.class,0);
		}
		public TerminalNode THEN() { return getToken(PlSqlParser.THEN, 0); }
		public Seq_of_statementsContext seq_of_statements() {
			return getRuleContext(Seq_of_statementsContext.class,0);
		}
		public TerminalNode END() { return getToken(PlSqlParser.END, 0); }
		public List<Elsif_partContext> elsif_part() {
			return getRuleContexts(Elsif_partContext.class);
		}
		public Elsif_partContext elsif_part(int i) {
			return getRuleContext(Elsif_partContext.class,i);
		}
		public Else_partContext else_part() {
			return getRuleContext(Else_partContext.class,0);
		}
		public If_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_if_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterIf_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitIf_statement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitIf_statement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final If_statementContext if_statement() throws RecognitionException {
		If_statementContext _localctx = new If_statementContext(_ctx, getState());
		enterRule(_localctx, 68, RULE_if_statement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(475);
			match(IF);
			setState(476);
			condition();
			setState(477);
			match(THEN);
			setState(478);
			seq_of_statements();
			setState(482);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==ELSIF) {
				{
				{
				setState(479);
				elsif_part();
				}
				}
				setState(484);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(486);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ELSE) {
				{
				setState(485);
				else_part();
				}
			}

			setState(488);
			match(END);
			setState(489);
			match(IF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Elsif_partContext extends ParserRuleContext {
		public TerminalNode ELSIF() { return getToken(PlSqlParser.ELSIF, 0); }
		public ConditionContext condition() {
			return getRuleContext(ConditionContext.class,0);
		}
		public TerminalNode THEN() { return getToken(PlSqlParser.THEN, 0); }
		public Seq_of_statementsContext seq_of_statements() {
			return getRuleContext(Seq_of_statementsContext.class,0);
		}
		public Elsif_partContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_elsif_part; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterElsif_part(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitElsif_part(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitElsif_part(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Elsif_partContext elsif_part() throws RecognitionException {
		Elsif_partContext _localctx = new Elsif_partContext(_ctx, getState());
		enterRule(_localctx, 70, RULE_elsif_part);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(491);
			match(ELSIF);
			setState(492);
			condition();
			setState(493);
			match(THEN);
			setState(494);
			seq_of_statements();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Else_partContext extends ParserRuleContext {
		public TerminalNode ELSE() { return getToken(PlSqlParser.ELSE, 0); }
		public Seq_of_statementsContext seq_of_statements() {
			return getRuleContext(Seq_of_statementsContext.class,0);
		}
		public Else_partContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_else_part; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterElse_part(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitElse_part(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitElse_part(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Else_partContext else_part() throws RecognitionException {
		Else_partContext _localctx = new Else_partContext(_ctx, getState());
		enterRule(_localctx, 72, RULE_else_part);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(496);
			match(ELSE);
			setState(497);
			seq_of_statements();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Loop_statementContext extends ParserRuleContext {
		public List<TerminalNode> LOOP() { return getTokens(PlSqlParser.LOOP); }
		public TerminalNode LOOP(int i) {
			return getToken(PlSqlParser.LOOP, i);
		}
		public Seq_of_statementsContext seq_of_statements() {
			return getRuleContext(Seq_of_statementsContext.class,0);
		}
		public TerminalNode END() { return getToken(PlSqlParser.END, 0); }
		public TerminalNode WHILE() { return getToken(PlSqlParser.WHILE, 0); }
		public ConditionContext condition() {
			return getRuleContext(ConditionContext.class,0);
		}
		public TerminalNode FOR() { return getToken(PlSqlParser.FOR, 0); }
		public Cursor_loop_paramContext cursor_loop_param() {
			return getRuleContext(Cursor_loop_paramContext.class,0);
		}
		public Loop_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_loop_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterLoop_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitLoop_statement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitLoop_statement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Loop_statementContext loop_statement() throws RecognitionException {
		Loop_statementContext _localctx = new Loop_statementContext(_ctx, getState());
		enterRule(_localctx, 74, RULE_loop_statement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(503);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case WHILE:
				{
				setState(499);
				match(WHILE);
				setState(500);
				condition();
				}
				break;
			case FOR:
				{
				setState(501);
				match(FOR);
				setState(502);
				cursor_loop_param();
				}
				break;
			case LOOP:
				break;
			default:
				break;
			}
			setState(505);
			match(LOOP);
			setState(506);
			seq_of_statements();
			setState(507);
			match(END);
			setState(508);
			match(LOOP);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Cursor_loop_paramContext extends ParserRuleContext {
		public Index_nameContext index_name() {
			return getRuleContext(Index_nameContext.class,0);
		}
		public TerminalNode IN() { return getToken(PlSqlParser.IN, 0); }
		public Lower_boundContext lower_bound() {
			return getRuleContext(Lower_boundContext.class,0);
		}
		public TerminalNode DOUBLE_PERIOD() { return getToken(PlSqlParser.DOUBLE_PERIOD, 0); }
		public Upper_boundContext upper_bound() {
			return getRuleContext(Upper_boundContext.class,0);
		}
		public Record_nameContext record_name() {
			return getRuleContext(Record_nameContext.class,0);
		}
		public TerminalNode LEFT_PAREN() { return getToken(PlSqlParser.LEFT_PAREN, 0); }
		public Select_statementContext select_statement() {
			return getRuleContext(Select_statementContext.class,0);
		}
		public TerminalNode RIGHT_PAREN() { return getToken(PlSqlParser.RIGHT_PAREN, 0); }
		public Cursor_loop_paramContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_cursor_loop_param; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterCursor_loop_param(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitCursor_loop_param(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitCursor_loop_param(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Cursor_loop_paramContext cursor_loop_param() throws RecognitionException {
		Cursor_loop_paramContext _localctx = new Cursor_loop_paramContext(_ctx, getState());
		enterRule(_localctx, 76, RULE_cursor_loop_param);
		try {
			setState(522);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,38,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(510);
				index_name();
				setState(511);
				match(IN);
				setState(512);
				lower_bound();
				setState(513);
				match(DOUBLE_PERIOD);
				setState(514);
				upper_bound();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(516);
				record_name();
				setState(517);
				match(IN);
				setState(518);
				match(LEFT_PAREN);
				setState(519);
				select_statement();
				setState(520);
				match(RIGHT_PAREN);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Select_statementContext extends ParserRuleContext {
		public TerminalNode SELECT() { return getToken(PlSqlParser.SELECT, 0); }
		public TerminalNode SEMICOLON() { return getToken(PlSqlParser.SEMICOLON, 0); }
		public Select_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_select_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterSelect_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitSelect_statement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitSelect_statement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Select_statementContext select_statement() throws RecognitionException {
		Select_statementContext _localctx = new Select_statementContext(_ctx, getState());
		enterRule(_localctx, 78, RULE_select_statement);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(524);
			match(SELECT);
			setState(528);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,39,_ctx);
			while ( _alt!=1 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1+1 ) {
					{
					{
					setState(525);
					matchWildcard();
					}
					} 
				}
				setState(530);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,39,_ctx);
			}
			setState(531);
			match(SEMICOLON);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Lower_boundContext extends ParserRuleContext {
		public ConcatenationContext concatenation() {
			return getRuleContext(ConcatenationContext.class,0);
		}
		public Lower_boundContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_lower_bound; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterLower_bound(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitLower_bound(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitLower_bound(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Lower_boundContext lower_bound() throws RecognitionException {
		Lower_boundContext _localctx = new Lower_boundContext(_ctx, getState());
		enterRule(_localctx, 80, RULE_lower_bound);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(533);
			concatenation(0);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Upper_boundContext extends ParserRuleContext {
		public ConcatenationContext concatenation() {
			return getRuleContext(ConcatenationContext.class,0);
		}
		public Upper_boundContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_upper_bound; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterUpper_bound(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitUpper_bound(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitUpper_bound(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Upper_boundContext upper_bound() throws RecognitionException {
		Upper_boundContext _localctx = new Upper_boundContext(_ctx, getState());
		enterRule(_localctx, 82, RULE_upper_bound);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(535);
			concatenation(0);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Raise_statementContext extends ParserRuleContext {
		public TerminalNode RAISE() { return getToken(PlSqlParser.RAISE, 0); }
		public Exception_nameContext exception_name() {
			return getRuleContext(Exception_nameContext.class,0);
		}
		public Raise_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_raise_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterRaise_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitRaise_statement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitRaise_statement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Raise_statementContext raise_statement() throws RecognitionException {
		Raise_statementContext _localctx = new Raise_statementContext(_ctx, getState());
		enterRule(_localctx, 84, RULE_raise_statement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(537);
			match(RAISE);
			setState(539);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & -4494805681840130L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & 10737418495L) != 0)) {
				{
				setState(538);
				exception_name();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Return_statementContext extends ParserRuleContext {
		public TerminalNode RETURN() { return getToken(PlSqlParser.RETURN, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public Return_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_return_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterReturn_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitReturn_statement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitReturn_statement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Return_statementContext return_statement() throws RecognitionException {
		Return_statementContext _localctx = new Return_statementContext(_ctx, getState());
		enterRule(_localctx, 86, RULE_return_statement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(541);
			match(RETURN);
			setState(543);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & -4433233030684674L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & 17045651967L) != 0)) {
				{
				setState(542);
				expression();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Call_statementContext extends ParserRuleContext {
		public Routine_nameContext routine_name() {
			return getRuleContext(Routine_nameContext.class,0);
		}
		public TerminalNode CALL() { return getToken(PlSqlParser.CALL, 0); }
		public Function_argumentContext function_argument() {
			return getRuleContext(Function_argumentContext.class,0);
		}
		public Call_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_call_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterCall_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitCall_statement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitCall_statement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Call_statementContext call_statement() throws RecognitionException {
		Call_statementContext _localctx = new Call_statementContext(_ctx, getState());
		enterRule(_localctx, 88, RULE_call_statement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(546);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,42,_ctx) ) {
			case 1:
				{
				setState(545);
				match(CALL);
				}
				break;
			}
			setState(548);
			routine_name();
			setState(550);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==LEFT_PAREN) {
				{
				setState(549);
				function_argument();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class BodyContext extends ParserRuleContext {
		public TerminalNode BEGIN() { return getToken(PlSqlParser.BEGIN, 0); }
		public Seq_of_statementsContext seq_of_statements() {
			return getRuleContext(Seq_of_statementsContext.class,0);
		}
		public TerminalNode END() { return getToken(PlSqlParser.END, 0); }
		public TerminalNode EXCEPTION() { return getToken(PlSqlParser.EXCEPTION, 0); }
		public List<Exception_handlerContext> exception_handler() {
			return getRuleContexts(Exception_handlerContext.class);
		}
		public Exception_handlerContext exception_handler(int i) {
			return getRuleContext(Exception_handlerContext.class,i);
		}
		public BodyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_body; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterBody(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitBody(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitBody(this);
			else return visitor.visitChildren(this);
		}
	}

	public final BodyContext body() throws RecognitionException {
		BodyContext _localctx = new BodyContext(_ctx, getState());
		enterRule(_localctx, 90, RULE_body);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(552);
			match(BEGIN);
			setState(553);
			seq_of_statements();
			setState(560);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==EXCEPTION) {
				{
				setState(554);
				match(EXCEPTION);
				setState(556); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(555);
					exception_handler();
					}
					}
					setState(558); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==WHEN );
				}
			}

			setState(562);
			match(END);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Exception_handlerContext extends ParserRuleContext {
		public TerminalNode WHEN() { return getToken(PlSqlParser.WHEN, 0); }
		public List<Exception_nameContext> exception_name() {
			return getRuleContexts(Exception_nameContext.class);
		}
		public Exception_nameContext exception_name(int i) {
			return getRuleContext(Exception_nameContext.class,i);
		}
		public TerminalNode THEN() { return getToken(PlSqlParser.THEN, 0); }
		public Seq_of_statementsContext seq_of_statements() {
			return getRuleContext(Seq_of_statementsContext.class,0);
		}
		public List<TerminalNode> OR() { return getTokens(PlSqlParser.OR); }
		public TerminalNode OR(int i) {
			return getToken(PlSqlParser.OR, i);
		}
		public Exception_handlerContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_exception_handler; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterException_handler(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitException_handler(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitException_handler(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Exception_handlerContext exception_handler() throws RecognitionException {
		Exception_handlerContext _localctx = new Exception_handlerContext(_ctx, getState());
		enterRule(_localctx, 92, RULE_exception_handler);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(564);
			match(WHEN);
			setState(565);
			exception_name();
			setState(570);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==OR) {
				{
				{
				setState(566);
				match(OR);
				setState(567);
				exception_name();
				}
				}
				setState(572);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(573);
			match(THEN);
			setState(574);
			seq_of_statements();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Sql_statementContext extends ParserRuleContext {
		public Data_manipulation_language_statementsContext data_manipulation_language_statements() {
			return getRuleContext(Data_manipulation_language_statementsContext.class,0);
		}
		public Sql_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sql_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterSql_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitSql_statement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitSql_statement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Sql_statementContext sql_statement() throws RecognitionException {
		Sql_statementContext _localctx = new Sql_statementContext(_ctx, getState());
		enterRule(_localctx, 94, RULE_sql_statement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(576);
			data_manipulation_language_statements();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Data_manipulation_language_statementsContext extends ParserRuleContext {
		public Merge_statementContext merge_statement() {
			return getRuleContext(Merge_statementContext.class,0);
		}
		public Select_statementContext select_statement() {
			return getRuleContext(Select_statementContext.class,0);
		}
		public Update_statementContext update_statement() {
			return getRuleContext(Update_statementContext.class,0);
		}
		public Delete_statementContext delete_statement() {
			return getRuleContext(Delete_statementContext.class,0);
		}
		public Insert_statementContext insert_statement() {
			return getRuleContext(Insert_statementContext.class,0);
		}
		public Create_table_as2Context create_table_as2() {
			return getRuleContext(Create_table_as2Context.class,0);
		}
		public Truncate_table_blockContext truncate_table_block() {
			return getRuleContext(Truncate_table_blockContext.class,0);
		}
		public Data_manipulation_language_statementsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_data_manipulation_language_statements; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterData_manipulation_language_statements(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitData_manipulation_language_statements(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitData_manipulation_language_statements(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Data_manipulation_language_statementsContext data_manipulation_language_statements() throws RecognitionException {
		Data_manipulation_language_statementsContext _localctx = new Data_manipulation_language_statementsContext(_ctx, getState());
		enterRule(_localctx, 96, RULE_data_manipulation_language_statements);
		try {
			setState(585);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case MERGE:
				enterOuterAlt(_localctx, 1);
				{
				setState(578);
				merge_statement();
				}
				break;
			case SELECT:
				enterOuterAlt(_localctx, 2);
				{
				setState(579);
				select_statement();
				}
				break;
			case UPDATE:
				enterOuterAlt(_localctx, 3);
				{
				setState(580);
				update_statement();
				}
				break;
			case DELETE:
				enterOuterAlt(_localctx, 4);
				{
				setState(581);
				delete_statement();
				}
				break;
			case INSERT:
				enterOuterAlt(_localctx, 5);
				{
				setState(582);
				insert_statement();
				}
				break;
			case CREATE:
				enterOuterAlt(_localctx, 6);
				{
				setState(583);
				create_table_as2();
				}
				break;
			case TRUNCATE:
				enterOuterAlt(_localctx, 7);
				{
				setState(584);
				truncate_table_block();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Update_statementContext extends ParserRuleContext {
		public TerminalNode UPDATE() { return getToken(PlSqlParser.UPDATE, 0); }
		public TerminalNode SEMICOLON() { return getToken(PlSqlParser.SEMICOLON, 0); }
		public Update_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_update_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterUpdate_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitUpdate_statement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitUpdate_statement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Update_statementContext update_statement() throws RecognitionException {
		Update_statementContext _localctx = new Update_statementContext(_ctx, getState());
		enterRule(_localctx, 98, RULE_update_statement);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(587);
			match(UPDATE);
			setState(591);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,48,_ctx);
			while ( _alt!=1 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1+1 ) {
					{
					{
					setState(588);
					matchWildcard();
					}
					} 
				}
				setState(593);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,48,_ctx);
			}
			setState(594);
			match(SEMICOLON);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Delete_statementContext extends ParserRuleContext {
		public TerminalNode DELETE() { return getToken(PlSqlParser.DELETE, 0); }
		public TerminalNode FROM() { return getToken(PlSqlParser.FROM, 0); }
		public TerminalNode SEMICOLON() { return getToken(PlSqlParser.SEMICOLON, 0); }
		public Delete_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_delete_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterDelete_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitDelete_statement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitDelete_statement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Delete_statementContext delete_statement() throws RecognitionException {
		Delete_statementContext _localctx = new Delete_statementContext(_ctx, getState());
		enterRule(_localctx, 100, RULE_delete_statement);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(596);
			match(DELETE);
			setState(597);
			match(FROM);
			setState(601);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,49,_ctx);
			while ( _alt!=1 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1+1 ) {
					{
					{
					setState(598);
					matchWildcard();
					}
					} 
				}
				setState(603);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,49,_ctx);
			}
			setState(604);
			match(SEMICOLON);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Insert_statementContext extends ParserRuleContext {
		public TerminalNode INSERT() { return getToken(PlSqlParser.INSERT, 0); }
		public TerminalNode SEMICOLON() { return getToken(PlSqlParser.SEMICOLON, 0); }
		public Insert_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_insert_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterInsert_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitInsert_statement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitInsert_statement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Insert_statementContext insert_statement() throws RecognitionException {
		Insert_statementContext _localctx = new Insert_statementContext(_ctx, getState());
		enterRule(_localctx, 102, RULE_insert_statement);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(606);
			match(INSERT);
			setState(610);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,50,_ctx);
			while ( _alt!=1 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1+1 ) {
					{
					{
					setState(607);
					matchWildcard();
					}
					} 
				}
				setState(612);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,50,_ctx);
			}
			setState(613);
			match(SEMICOLON);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Merge_statementContext extends ParserRuleContext {
		public TerminalNode MERGE() { return getToken(PlSqlParser.MERGE, 0); }
		public TerminalNode INTO() { return getToken(PlSqlParser.INTO, 0); }
		public TerminalNode SEMICOLON() { return getToken(PlSqlParser.SEMICOLON, 0); }
		public Merge_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_merge_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterMerge_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitMerge_statement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitMerge_statement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Merge_statementContext merge_statement() throws RecognitionException {
		Merge_statementContext _localctx = new Merge_statementContext(_ctx, getState());
		enterRule(_localctx, 104, RULE_merge_statement);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(615);
			match(MERGE);
			setState(616);
			match(INTO);
			setState(620);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,51,_ctx);
			while ( _alt!=1 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1+1 ) {
					{
					{
					setState(617);
					matchWildcard();
					}
					} 
				}
				setState(622);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,51,_ctx);
			}
			setState(623);
			match(SEMICOLON);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ConditionContext extends ParserRuleContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public ConditionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_condition; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterCondition(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitCondition(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitCondition(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ConditionContext condition() throws RecognitionException {
		ConditionContext _localctx = new ConditionContext(_ctx, getState());
		enterRule(_localctx, 106, RULE_condition);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(625);
			expression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ExpressionsContext extends ParserRuleContext {
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(PlSqlParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(PlSqlParser.COMMA, i);
		}
		public ExpressionsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_expressions; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterExpressions(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitExpressions(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitExpressions(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ExpressionsContext expressions() throws RecognitionException {
		ExpressionsContext _localctx = new ExpressionsContext(_ctx, getState());
		enterRule(_localctx, 108, RULE_expressions);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(627);
			expression();
			setState(632);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(628);
				match(COMMA);
				setState(629);
				expression();
				}
				}
				setState(634);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ExpressionContext extends ParserRuleContext {
		public Logical_expressionContext logical_expression() {
			return getRuleContext(Logical_expressionContext.class,0);
		}
		public ExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_expression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ExpressionContext expression() throws RecognitionException {
		ExpressionContext _localctx = new ExpressionContext(_ctx, getState());
		enterRule(_localctx, 110, RULE_expression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(635);
			logical_expression(0);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Logical_expressionContext extends ParserRuleContext {
		public Unary_logical_expressionContext unary_logical_expression() {
			return getRuleContext(Unary_logical_expressionContext.class,0);
		}
		public List<Logical_expressionContext> logical_expression() {
			return getRuleContexts(Logical_expressionContext.class);
		}
		public Logical_expressionContext logical_expression(int i) {
			return getRuleContext(Logical_expressionContext.class,i);
		}
		public TerminalNode AND() { return getToken(PlSqlParser.AND, 0); }
		public TerminalNode OR() { return getToken(PlSqlParser.OR, 0); }
		public Logical_expressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_logical_expression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterLogical_expression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitLogical_expression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitLogical_expression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Logical_expressionContext logical_expression() throws RecognitionException {
		return logical_expression(0);
	}

	private Logical_expressionContext logical_expression(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		Logical_expressionContext _localctx = new Logical_expressionContext(_ctx, _parentState);
		Logical_expressionContext _prevctx = _localctx;
		int _startState = 112;
		enterRecursionRule(_localctx, 112, RULE_logical_expression, _p);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			{
			setState(638);
			unary_logical_expression();
			}
			_ctx.stop = _input.LT(-1);
			setState(648);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,54,_ctx);
			while ( _alt!=2 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(646);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,53,_ctx) ) {
					case 1:
						{
						_localctx = new Logical_expressionContext(_parentctx, _parentState);
						pushNewRecursionContext(_localctx, _startState, RULE_logical_expression);
						setState(640);
						if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
						setState(641);
						match(AND);
						setState(642);
						logical_expression(3);
						}
						break;
					case 2:
						{
						_localctx = new Logical_expressionContext(_parentctx, _parentState);
						pushNewRecursionContext(_localctx, _startState, RULE_logical_expression);
						setState(643);
						if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
						setState(644);
						match(OR);
						setState(645);
						logical_expression(2);
						}
						break;
					}
					} 
				}
				setState(650);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,54,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Unary_logical_expressionContext extends ParserRuleContext {
		public Multiset_expressionContext multiset_expression() {
			return getRuleContext(Multiset_expressionContext.class,0);
		}
		public List<TerminalNode> NOT() { return getTokens(PlSqlParser.NOT); }
		public TerminalNode NOT(int i) {
			return getToken(PlSqlParser.NOT, i);
		}
		public TerminalNode IS() { return getToken(PlSqlParser.IS, 0); }
		public TerminalNode NULL_() { return getToken(PlSqlParser.NULL_, 0); }
		public Unary_logical_expressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_unary_logical_expression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterUnary_logical_expression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitUnary_logical_expression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitUnary_logical_expression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Unary_logical_expressionContext unary_logical_expression() throws RecognitionException {
		Unary_logical_expressionContext _localctx = new Unary_logical_expressionContext(_ctx, getState());
		enterRule(_localctx, 114, RULE_unary_logical_expression);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(652);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,55,_ctx) ) {
			case 1:
				{
				setState(651);
				match(NOT);
				}
				break;
			}
			setState(654);
			multiset_expression();
			setState(660);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,57,_ctx) ) {
			case 1:
				{
				setState(655);
				match(IS);
				setState(657);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(656);
					match(NOT);
					}
				}

				setState(659);
				match(NULL_);
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Multiset_expressionContext extends ParserRuleContext {
		public Relational_expressionContext relational_expression() {
			return getRuleContext(Relational_expressionContext.class,0);
		}
		public Multiset_expressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_multiset_expression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterMultiset_expression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitMultiset_expression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitMultiset_expression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Multiset_expressionContext multiset_expression() throws RecognitionException {
		Multiset_expressionContext _localctx = new Multiset_expressionContext(_ctx, getState());
		enterRule(_localctx, 116, RULE_multiset_expression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(662);
			relational_expression(0);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Relational_expressionContext extends ParserRuleContext {
		public Compound_expressionContext compound_expression() {
			return getRuleContext(Compound_expressionContext.class,0);
		}
		public List<Relational_expressionContext> relational_expression() {
			return getRuleContexts(Relational_expressionContext.class);
		}
		public Relational_expressionContext relational_expression(int i) {
			return getRuleContext(Relational_expressionContext.class,i);
		}
		public Relational_operatorContext relational_operator() {
			return getRuleContext(Relational_operatorContext.class,0);
		}
		public Relational_expressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_relational_expression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterRelational_expression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitRelational_expression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitRelational_expression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Relational_expressionContext relational_expression() throws RecognitionException {
		return relational_expression(0);
	}

	private Relational_expressionContext relational_expression(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		Relational_expressionContext _localctx = new Relational_expressionContext(_ctx, _parentState);
		Relational_expressionContext _prevctx = _localctx;
		int _startState = 118;
		enterRecursionRule(_localctx, 118, RULE_relational_expression, _p);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			{
			setState(665);
			compound_expression();
			}
			_ctx.stop = _input.LT(-1);
			setState(673);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,58,_ctx);
			while ( _alt!=2 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					{
					_localctx = new Relational_expressionContext(_parentctx, _parentState);
					pushNewRecursionContext(_localctx, _startState, RULE_relational_expression);
					setState(667);
					if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
					setState(668);
					relational_operator();
					setState(669);
					relational_expression(3);
					}
					} 
				}
				setState(675);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,58,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Compound_expressionContext extends ParserRuleContext {
		public Token like_type;
		public List<ConcatenationContext> concatenation() {
			return getRuleContexts(ConcatenationContext.class);
		}
		public ConcatenationContext concatenation(int i) {
			return getRuleContext(ConcatenationContext.class,i);
		}
		public TerminalNode IN() { return getToken(PlSqlParser.IN, 0); }
		public In_elementsContext in_elements() {
			return getRuleContext(In_elementsContext.class,0);
		}
		public TerminalNode BETWEEN() { return getToken(PlSqlParser.BETWEEN, 0); }
		public Between_elementsContext between_elements() {
			return getRuleContext(Between_elementsContext.class,0);
		}
		public TerminalNode NOT() { return getToken(PlSqlParser.NOT, 0); }
		public TerminalNode LIKE() { return getToken(PlSqlParser.LIKE, 0); }
		public TerminalNode LIKEC() { return getToken(PlSqlParser.LIKEC, 0); }
		public TerminalNode LIKE2() { return getToken(PlSqlParser.LIKE2, 0); }
		public TerminalNode LIKE4() { return getToken(PlSqlParser.LIKE4, 0); }
		public TerminalNode ESCAPE() { return getToken(PlSqlParser.ESCAPE, 0); }
		public Compound_expressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_compound_expression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterCompound_expression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitCompound_expression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitCompound_expression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Compound_expressionContext compound_expression() throws RecognitionException {
		Compound_expressionContext _localctx = new Compound_expressionContext(_ctx, getState());
		enterRule(_localctx, 120, RULE_compound_expression);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(676);
			concatenation(0);
			setState(692);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,62,_ctx) ) {
			case 1:
				{
				setState(678);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(677);
					match(NOT);
					}
				}

				setState(690);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case IN:
					{
					setState(680);
					match(IN);
					setState(681);
					in_elements();
					}
					break;
				case BETWEEN:
					{
					setState(682);
					match(BETWEEN);
					setState(683);
					between_elements();
					}
					break;
				case LIKE:
				case LIKEC:
				case LIKE2:
				case LIKE4:
					{
					setState(684);
					((Compound_expressionContext)_localctx).like_type = _input.LT(1);
					_la = _input.LA(1);
					if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & 2111062325329920L) != 0)) ) {
						((Compound_expressionContext)_localctx).like_type = (Token)_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					setState(685);
					concatenation(0);
					setState(688);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,60,_ctx) ) {
					case 1:
						{
						setState(686);
						match(ESCAPE);
						setState(687);
						concatenation(0);
						}
						break;
					}
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Relational_operatorContext extends ParserRuleContext {
		public TerminalNode EQUALS_OP() { return getToken(PlSqlParser.EQUALS_OP, 0); }
		public TerminalNode NOT_EQUAL_OP() { return getToken(PlSqlParser.NOT_EQUAL_OP, 0); }
		public TerminalNode LESS_THAN_OP() { return getToken(PlSqlParser.LESS_THAN_OP, 0); }
		public TerminalNode GREATER_THAN_OP() { return getToken(PlSqlParser.GREATER_THAN_OP, 0); }
		public TerminalNode EXCLAMATION_OPERATOR_PART() { return getToken(PlSqlParser.EXCLAMATION_OPERATOR_PART, 0); }
		public TerminalNode CARRET_OPERATOR_PART() { return getToken(PlSqlParser.CARRET_OPERATOR_PART, 0); }
		public Relational_operatorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_relational_operator; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterRelational_operator(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitRelational_operator(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitRelational_operator(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Relational_operatorContext relational_operator() throws RecognitionException {
		Relational_operatorContext _localctx = new Relational_operatorContext(_ctx, getState());
		enterRule(_localctx, 122, RULE_relational_operator);
		int _la;
		try {
			setState(706);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,64,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(694);
				match(EQUALS_OP);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(695);
				match(NOT_EQUAL_OP);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(696);
				match(LESS_THAN_OP);
				setState(697);
				match(GREATER_THAN_OP);
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(698);
				match(EXCLAMATION_OPERATOR_PART);
				setState(699);
				match(EQUALS_OP);
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(700);
				match(CARRET_OPERATOR_PART);
				setState(701);
				match(EQUALS_OP);
				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(702);
				_la = _input.LA(1);
				if ( !(_la==LESS_THAN_OP || _la==GREATER_THAN_OP) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(704);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==EQUALS_OP) {
					{
					setState(703);
					match(EQUALS_OP);
					}
				}

				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class In_elementsContext extends ParserRuleContext {
		public TerminalNode LEFT_PAREN() { return getToken(PlSqlParser.LEFT_PAREN, 0); }
		public List<ConcatenationContext> concatenation() {
			return getRuleContexts(ConcatenationContext.class);
		}
		public ConcatenationContext concatenation(int i) {
			return getRuleContext(ConcatenationContext.class,i);
		}
		public TerminalNode RIGHT_PAREN() { return getToken(PlSqlParser.RIGHT_PAREN, 0); }
		public List<TerminalNode> COMMA() { return getTokens(PlSqlParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(PlSqlParser.COMMA, i);
		}
		public ConstantContext constant() {
			return getRuleContext(ConstantContext.class,0);
		}
		public Bind_variableContext bind_variable() {
			return getRuleContext(Bind_variableContext.class,0);
		}
		public General_elementContext general_element() {
			return getRuleContext(General_elementContext.class,0);
		}
		public In_elementsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_in_elements; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterIn_elements(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitIn_elements(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitIn_elements(this);
			else return visitor.visitChildren(this);
		}
	}

	public final In_elementsContext in_elements() throws RecognitionException {
		In_elementsContext _localctx = new In_elementsContext(_ctx, getState());
		enterRule(_localctx, 124, RULE_in_elements);
		int _la;
		try {
			setState(722);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,66,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(708);
				match(LEFT_PAREN);
				setState(709);
				concatenation(0);
				setState(714);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(710);
					match(COMMA);
					setState(711);
					concatenation(0);
					}
					}
					setState(716);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(717);
				match(RIGHT_PAREN);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(719);
				constant();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(720);
				bind_variable();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(721);
				general_element();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Between_elementsContext extends ParserRuleContext {
		public List<ConcatenationContext> concatenation() {
			return getRuleContexts(ConcatenationContext.class);
		}
		public ConcatenationContext concatenation(int i) {
			return getRuleContext(ConcatenationContext.class,i);
		}
		public TerminalNode AND() { return getToken(PlSqlParser.AND, 0); }
		public Between_elementsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_between_elements; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterBetween_elements(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitBetween_elements(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitBetween_elements(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Between_elementsContext between_elements() throws RecognitionException {
		Between_elementsContext _localctx = new Between_elementsContext(_ctx, getState());
		enterRule(_localctx, 126, RULE_between_elements);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(724);
			concatenation(0);
			setState(725);
			match(AND);
			setState(726);
			concatenation(0);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ConcatenationContext extends ParserRuleContext {
		public Token op;
		public AtomContext atom() {
			return getRuleContext(AtomContext.class,0);
		}
		public List<ConcatenationContext> concatenation() {
			return getRuleContexts(ConcatenationContext.class);
		}
		public ConcatenationContext concatenation(int i) {
			return getRuleContext(ConcatenationContext.class,i);
		}
		public TerminalNode ASTERISK() { return getToken(PlSqlParser.ASTERISK, 0); }
		public TerminalNode SOLIDUS() { return getToken(PlSqlParser.SOLIDUS, 0); }
		public TerminalNode PLUS_SIGN() { return getToken(PlSqlParser.PLUS_SIGN, 0); }
		public TerminalNode MINUS_SIGN() { return getToken(PlSqlParser.MINUS_SIGN, 0); }
		public List<TerminalNode> BAR() { return getTokens(PlSqlParser.BAR); }
		public TerminalNode BAR(int i) {
			return getToken(PlSqlParser.BAR, i);
		}
		public ConcatenationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_concatenation; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterConcatenation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitConcatenation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitConcatenation(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ConcatenationContext concatenation() throws RecognitionException {
		return concatenation(0);
	}

	private ConcatenationContext concatenation(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		ConcatenationContext _localctx = new ConcatenationContext(_ctx, _parentState);
		ConcatenationContext _prevctx = _localctx;
		int _startState = 128;
		enterRecursionRule(_localctx, 128, RULE_concatenation, _p);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			{
			setState(729);
			atom();
			}
			_ctx.stop = _input.LT(-1);
			setState(752);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,68,_ctx);
			while ( _alt!=2 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(750);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,67,_ctx) ) {
					case 1:
						{
						_localctx = new ConcatenationContext(_parentctx, _parentState);
						pushNewRecursionContext(_localctx, _startState, RULE_concatenation);
						setState(731);
						if (!(precpred(_ctx, 7))) throw new FailedPredicateException(this, "precpred(_ctx, 7)");
						setState(732);
						((ConcatenationContext)_localctx).op = match(ASTERISK);
						setState(733);
						concatenation(8);
						}
						break;
					case 2:
						{
						_localctx = new ConcatenationContext(_parentctx, _parentState);
						pushNewRecursionContext(_localctx, _startState, RULE_concatenation);
						setState(734);
						if (!(precpred(_ctx, 6))) throw new FailedPredicateException(this, "precpred(_ctx, 6)");
						setState(735);
						((ConcatenationContext)_localctx).op = match(SOLIDUS);
						setState(736);
						concatenation(7);
						}
						break;
					case 3:
						{
						_localctx = new ConcatenationContext(_parentctx, _parentState);
						pushNewRecursionContext(_localctx, _startState, RULE_concatenation);
						setState(737);
						if (!(precpred(_ctx, 5))) throw new FailedPredicateException(this, "precpred(_ctx, 5)");
						setState(738);
						((ConcatenationContext)_localctx).op = match(PLUS_SIGN);
						setState(739);
						concatenation(6);
						}
						break;
					case 4:
						{
						_localctx = new ConcatenationContext(_parentctx, _parentState);
						pushNewRecursionContext(_localctx, _startState, RULE_concatenation);
						setState(740);
						if (!(precpred(_ctx, 4))) throw new FailedPredicateException(this, "precpred(_ctx, 4)");
						setState(741);
						((ConcatenationContext)_localctx).op = match(MINUS_SIGN);
						setState(742);
						concatenation(5);
						}
						break;
					case 5:
						{
						_localctx = new ConcatenationContext(_parentctx, _parentState);
						pushNewRecursionContext(_localctx, _startState, RULE_concatenation);
						setState(743);
						if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
						setState(744);
						((ConcatenationContext)_localctx).op = match(BAR);
						setState(745);
						concatenation(4);
						}
						break;
					case 6:
						{
						_localctx = new ConcatenationContext(_parentctx, _parentState);
						pushNewRecursionContext(_localctx, _startState, RULE_concatenation);
						setState(746);
						if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
						setState(747);
						match(BAR);
						setState(748);
						match(BAR);
						setState(749);
						concatenation(3);
						}
						break;
					}
					} 
				}
				setState(754);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,68,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Unary_expressionContext extends ParserRuleContext {
		public Unary_expressionContext unary_expression() {
			return getRuleContext(Unary_expressionContext.class,0);
		}
		public TerminalNode MINUS_SIGN() { return getToken(PlSqlParser.MINUS_SIGN, 0); }
		public TerminalNode PLUS_SIGN() { return getToken(PlSqlParser.PLUS_SIGN, 0); }
		public AtomContext atom() {
			return getRuleContext(AtomContext.class,0);
		}
		public Unary_expressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_unary_expression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterUnary_expression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitUnary_expression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitUnary_expression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Unary_expressionContext unary_expression() throws RecognitionException {
		Unary_expressionContext _localctx = new Unary_expressionContext(_ctx, getState());
		enterRule(_localctx, 130, RULE_unary_expression);
		int _la;
		try {
			setState(758);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case PLUS_SIGN:
			case MINUS_SIGN:
				enterOuterAlt(_localctx, 1);
				{
				setState(755);
				_la = _input.LA(1);
				if ( !(_la==PLUS_SIGN || _la==MINUS_SIGN) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(756);
				unary_expression();
				}
				break;
			case CREATE:
			case TABLE:
			case WITH:
			case SET:
			case SELECT:
			case INSERT:
			case UPDATE:
			case DELETE:
			case MERGE:
			case INTO:
			case FROM:
			case WHERE:
			case PROCEDURE:
			case FUNCTION:
			case RETURN:
			case BEGIN:
			case END:
			case EXCEPTION:
			case WHEN:
			case THEN:
			case ELSE:
			case ELSIF:
			case IF:
			case LOOP:
			case FOR:
			case WHILE:
			case EXIT:
			case CONTINUE:
			case RAISE:
			case DECLARE:
			case CALL:
			case AS:
			case IS:
			case START:
			case TRANSACTION:
			case COMMIT:
			case ROLLBACK:
			case AND:
			case OR:
			case NOT:
			case NULL_:
			case TRUE:
			case FALSE:
			case IN:
			case OUT:
			case CONSTANT:
			case DEFAULT:
			case INT:
			case BYTE:
			case SMALLINT:
			case BIGINT:
			case NUMBER:
			case DECIMAL:
			case DOUBLE:
			case FLOAT:
			case VARCHAR:
			case VARCHAR2:
			case STRING:
			case BOOLEAN:
			case DATE:
			case TIMESTAMP:
			case BINARY:
			case BLOB:
			case LEFT_PAREN:
			case CHAR_STRING:
			case NATIONAL_CHAR_STRING_LIT:
			case UNSIGNED_INTEGER:
			case APPROXIMATE_NUM_LIT:
			case DELIMITED_ID:
			case BINDVAR:
			case REGULAR_ID:
				enterOuterAlt(_localctx, 2);
				{
				setState(757);
				atom();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class AtomContext extends ParserRuleContext {
		public Bind_variableContext bind_variable() {
			return getRuleContext(Bind_variableContext.class,0);
		}
		public ConstantContext constant() {
			return getRuleContext(ConstantContext.class,0);
		}
		public General_elementContext general_element() {
			return getRuleContext(General_elementContext.class,0);
		}
		public TerminalNode LEFT_PAREN() { return getToken(PlSqlParser.LEFT_PAREN, 0); }
		public ExpressionsContext expressions() {
			return getRuleContext(ExpressionsContext.class,0);
		}
		public TerminalNode RIGHT_PAREN() { return getToken(PlSqlParser.RIGHT_PAREN, 0); }
		public Quoted_stringContext quoted_string() {
			return getRuleContext(Quoted_stringContext.class,0);
		}
		public AtomContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_atom; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterAtom(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitAtom(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitAtom(this);
			else return visitor.visitChildren(this);
		}
	}

	public final AtomContext atom() throws RecognitionException {
		AtomContext _localctx = new AtomContext(_ctx, getState());
		enterRule(_localctx, 132, RULE_atom);
		try {
			setState(768);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,70,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(760);
				bind_variable();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(761);
				constant();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(762);
				general_element();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(763);
				match(LEFT_PAREN);
				setState(764);
				expressions();
				setState(765);
				match(RIGHT_PAREN);
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(767);
				quoted_string();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Routine_nameContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public Routine_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_routine_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterRoutine_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitRoutine_name(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitRoutine_name(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Routine_nameContext routine_name() throws RecognitionException {
		Routine_nameContext _localctx = new Routine_nameContext(_ctx, getState());
		enterRule(_localctx, 134, RULE_routine_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(770);
			identifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Parameter_nameContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public Parameter_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_parameter_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterParameter_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitParameter_name(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitParameter_name(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Parameter_nameContext parameter_name() throws RecognitionException {
		Parameter_nameContext _localctx = new Parameter_nameContext(_ctx, getState());
		enterRule(_localctx, 136, RULE_parameter_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(772);
			identifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Procedure_nameContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public Procedure_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_procedure_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterProcedure_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitProcedure_name(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitProcedure_name(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Procedure_nameContext procedure_name() throws RecognitionException {
		Procedure_nameContext _localctx = new Procedure_nameContext(_ctx, getState());
		enterRule(_localctx, 138, RULE_procedure_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(774);
			identifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Exception_nameContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public Exception_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_exception_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterException_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitException_name(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitException_name(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Exception_nameContext exception_name() throws RecognitionException {
		Exception_nameContext _localctx = new Exception_nameContext(_ctx, getState());
		enterRule(_localctx, 140, RULE_exception_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(776);
			identifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Index_nameContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public Index_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_index_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterIndex_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitIndex_name(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitIndex_name(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Index_nameContext index_name() throws RecognitionException {
		Index_nameContext _localctx = new Index_nameContext(_ctx, getState());
		enterRule(_localctx, 142, RULE_index_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(778);
			identifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Record_nameContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public Record_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_record_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterRecord_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitRecord_name(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitRecord_name(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Record_nameContext record_name() throws RecognitionException {
		Record_nameContext _localctx = new Record_nameContext(_ctx, getState());
		enterRule(_localctx, 144, RULE_record_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(780);
			identifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Column_nameContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public Column_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_column_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterColumn_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitColumn_name(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitColumn_name(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Column_nameContext column_name() throws RecognitionException {
		Column_nameContext _localctx = new Column_nameContext(_ctx, getState());
		enterRule(_localctx, 146, RULE_column_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(782);
			identifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Function_argumentContext extends ParserRuleContext {
		public TerminalNode LEFT_PAREN() { return getToken(PlSqlParser.LEFT_PAREN, 0); }
		public TerminalNode RIGHT_PAREN() { return getToken(PlSqlParser.RIGHT_PAREN, 0); }
		public List<ArgumentContext> argument() {
			return getRuleContexts(ArgumentContext.class);
		}
		public ArgumentContext argument(int i) {
			return getRuleContext(ArgumentContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(PlSqlParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(PlSqlParser.COMMA, i);
		}
		public Function_argumentContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_function_argument; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterFunction_argument(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitFunction_argument(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitFunction_argument(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Function_argumentContext function_argument() throws RecognitionException {
		Function_argumentContext _localctx = new Function_argumentContext(_ctx, getState());
		enterRule(_localctx, 148, RULE_function_argument);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(784);
			match(LEFT_PAREN);
			setState(793);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & -4433233030684674L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & 17045651967L) != 0)) {
				{
				setState(785);
				argument();
				setState(790);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(786);
					match(COMMA);
					setState(787);
					argument();
					}
					}
					setState(792);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(795);
			match(RIGHT_PAREN);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ArgumentContext extends ParserRuleContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public ArgumentContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_argument; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterArgument(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitArgument(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitArgument(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ArgumentContext argument() throws RecognitionException {
		ArgumentContext _localctx = new ArgumentContext(_ctx, getState());
		enterRule(_localctx, 150, RULE_argument);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(797);
			expression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Type_specContext extends ParserRuleContext {
		public DatatypeContext datatype() {
			return getRuleContext(DatatypeContext.class,0);
		}
		public Type_specContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_type_spec; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterType_spec(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitType_spec(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitType_spec(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Type_specContext type_spec() throws RecognitionException {
		Type_specContext _localctx = new Type_specContext(_ctx, getState());
		enterRule(_localctx, 152, RULE_type_spec);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(799);
			datatype();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class DatatypeContext extends ParserRuleContext {
		public Native_datatype_elementContext native_datatype_element() {
			return getRuleContext(Native_datatype_elementContext.class,0);
		}
		public Precision_partContext precision_part() {
			return getRuleContext(Precision_partContext.class,0);
		}
		public DatatypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_datatype; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterDatatype(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitDatatype(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitDatatype(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DatatypeContext datatype() throws RecognitionException {
		DatatypeContext _localctx = new DatatypeContext(_ctx, getState());
		enterRule(_localctx, 154, RULE_datatype);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(801);
			native_datatype_element();
			setState(803);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==LEFT_PAREN) {
				{
				setState(802);
				precision_part();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Precision_partContext extends ParserRuleContext {
		public TerminalNode LEFT_PAREN() { return getToken(PlSqlParser.LEFT_PAREN, 0); }
		public List<NumericContext> numeric() {
			return getRuleContexts(NumericContext.class);
		}
		public NumericContext numeric(int i) {
			return getRuleContext(NumericContext.class,i);
		}
		public TerminalNode RIGHT_PAREN() { return getToken(PlSqlParser.RIGHT_PAREN, 0); }
		public TerminalNode COMMA() { return getToken(PlSqlParser.COMMA, 0); }
		public Precision_partContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_precision_part; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterPrecision_part(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitPrecision_part(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitPrecision_part(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Precision_partContext precision_part() throws RecognitionException {
		Precision_partContext _localctx = new Precision_partContext(_ctx, getState());
		enterRule(_localctx, 156, RULE_precision_part);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(805);
			match(LEFT_PAREN);
			setState(806);
			numeric();
			setState(809);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==COMMA) {
				{
				setState(807);
				match(COMMA);
				setState(808);
				numeric();
				}
			}

			setState(811);
			match(RIGHT_PAREN);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Native_datatype_elementContext extends ParserRuleContext {
		public TerminalNode INT() { return getToken(PlSqlParser.INT, 0); }
		public TerminalNode BYTE() { return getToken(PlSqlParser.BYTE, 0); }
		public TerminalNode SMALLINT() { return getToken(PlSqlParser.SMALLINT, 0); }
		public TerminalNode BIGINT() { return getToken(PlSqlParser.BIGINT, 0); }
		public TerminalNode NUMBER() { return getToken(PlSqlParser.NUMBER, 0); }
		public TerminalNode DECIMAL() { return getToken(PlSqlParser.DECIMAL, 0); }
		public TerminalNode DOUBLE() { return getToken(PlSqlParser.DOUBLE, 0); }
		public TerminalNode FLOAT() { return getToken(PlSqlParser.FLOAT, 0); }
		public TerminalNode VARCHAR() { return getToken(PlSqlParser.VARCHAR, 0); }
		public TerminalNode STRING() { return getToken(PlSqlParser.STRING, 0); }
		public TerminalNode BOOLEAN() { return getToken(PlSqlParser.BOOLEAN, 0); }
		public TerminalNode DATE() { return getToken(PlSqlParser.DATE, 0); }
		public TerminalNode TIMESTAMP() { return getToken(PlSqlParser.TIMESTAMP, 0); }
		public TerminalNode BINARY() { return getToken(PlSqlParser.BINARY, 0); }
		public TerminalNode BLOB() { return getToken(PlSqlParser.BLOB, 0); }
		public Native_datatype_elementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_native_datatype_element; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterNative_datatype_element(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitNative_datatype_element(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitNative_datatype_element(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Native_datatype_elementContext native_datatype_element() throws RecognitionException {
		Native_datatype_elementContext _localctx = new Native_datatype_elementContext(_ctx, getState());
		enterRule(_localctx, 158, RULE_native_datatype_element);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(813);
			_la = _input.LA(1);
			if ( !(((((_la - 56)) & ~0x3f) == 0 && ((1L << (_la - 56)) & 65023L) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Bind_variableContext extends ParserRuleContext {
		public TerminalNode BINDVAR() { return getToken(PlSqlParser.BINDVAR, 0); }
		public Bind_variableContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_bind_variable; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterBind_variable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitBind_variable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitBind_variable(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Bind_variableContext bind_variable() throws RecognitionException {
		Bind_variableContext _localctx = new Bind_variableContext(_ctx, getState());
		enterRule(_localctx, 160, RULE_bind_variable);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(815);
			match(BINDVAR);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class General_elementContext extends ParserRuleContext {
		public List<Id_expressionContext> id_expression() {
			return getRuleContexts(Id_expressionContext.class);
		}
		public Id_expressionContext id_expression(int i) {
			return getRuleContext(Id_expressionContext.class,i);
		}
		public List<TerminalNode> PERIOD() { return getTokens(PlSqlParser.PERIOD); }
		public TerminalNode PERIOD(int i) {
			return getToken(PlSqlParser.PERIOD, i);
		}
		public General_elementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_general_element; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterGeneral_element(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitGeneral_element(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitGeneral_element(this);
			else return visitor.visitChildren(this);
		}
	}

	public final General_elementContext general_element() throws RecognitionException {
		General_elementContext _localctx = new General_elementContext(_ctx, getState());
		enterRule(_localctx, 162, RULE_general_element);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(817);
			id_expression();
			setState(822);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,75,_ctx);
			while ( _alt!=2 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(818);
					match(PERIOD);
					setState(819);
					id_expression();
					}
					} 
				}
				setState(824);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,75,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ConstantContext extends ParserRuleContext {
		public TerminalNode TIMESTAMP() { return getToken(PlSqlParser.TIMESTAMP, 0); }
		public Quoted_stringContext quoted_string() {
			return getRuleContext(Quoted_stringContext.class,0);
		}
		public Bind_variableContext bind_variable() {
			return getRuleContext(Bind_variableContext.class,0);
		}
		public NumericContext numeric() {
			return getRuleContext(NumericContext.class,0);
		}
		public TerminalNode DATE() { return getToken(PlSqlParser.DATE, 0); }
		public TerminalNode NULL_() { return getToken(PlSqlParser.NULL_, 0); }
		public TerminalNode TRUE() { return getToken(PlSqlParser.TRUE, 0); }
		public TerminalNode FALSE() { return getToken(PlSqlParser.FALSE, 0); }
		public TerminalNode DEFAULT() { return getToken(PlSqlParser.DEFAULT, 0); }
		public ConstantContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_constant; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterConstant(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitConstant(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitConstant(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ConstantContext constant() throws RecognitionException {
		ConstantContext _localctx = new ConstantContext(_ctx, getState());
		enterRule(_localctx, 164, RULE_constant);
		try {
			setState(838);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case TIMESTAMP:
				enterOuterAlt(_localctx, 1);
				{
				setState(825);
				match(TIMESTAMP);
				setState(828);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case CHAR_STRING:
				case NATIONAL_CHAR_STRING_LIT:
					{
					setState(826);
					quoted_string();
					}
					break;
				case BINDVAR:
					{
					setState(827);
					bind_variable();
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				break;
			case UNSIGNED_INTEGER:
			case APPROXIMATE_NUM_LIT:
				enterOuterAlt(_localctx, 2);
				{
				setState(830);
				numeric();
				}
				break;
			case DATE:
				enterOuterAlt(_localctx, 3);
				{
				setState(831);
				match(DATE);
				setState(832);
				quoted_string();
				}
				break;
			case CHAR_STRING:
			case NATIONAL_CHAR_STRING_LIT:
				enterOuterAlt(_localctx, 4);
				{
				setState(833);
				quoted_string();
				}
				break;
			case NULL_:
				enterOuterAlt(_localctx, 5);
				{
				setState(834);
				match(NULL_);
				}
				break;
			case TRUE:
				enterOuterAlt(_localctx, 6);
				{
				setState(835);
				match(TRUE);
				}
				break;
			case FALSE:
				enterOuterAlt(_localctx, 7);
				{
				setState(836);
				match(FALSE);
				}
				break;
			case DEFAULT:
				enterOuterAlt(_localctx, 8);
				{
				setState(837);
				match(DEFAULT);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class NumericContext extends ParserRuleContext {
		public TerminalNode UNSIGNED_INTEGER() { return getToken(PlSqlParser.UNSIGNED_INTEGER, 0); }
		public TerminalNode APPROXIMATE_NUM_LIT() { return getToken(PlSqlParser.APPROXIMATE_NUM_LIT, 0); }
		public NumericContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_numeric; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterNumeric(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitNumeric(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitNumeric(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NumericContext numeric() throws RecognitionException {
		NumericContext _localctx = new NumericContext(_ctx, getState());
		enterRule(_localctx, 166, RULE_numeric);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(840);
			_la = _input.LA(1);
			if ( !(_la==UNSIGNED_INTEGER || _la==APPROXIMATE_NUM_LIT) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Quoted_stringContext extends ParserRuleContext {
		public TerminalNode CHAR_STRING() { return getToken(PlSqlParser.CHAR_STRING, 0); }
		public TerminalNode NATIONAL_CHAR_STRING_LIT() { return getToken(PlSqlParser.NATIONAL_CHAR_STRING_LIT, 0); }
		public Quoted_stringContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_quoted_string; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterQuoted_string(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitQuoted_string(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitQuoted_string(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Quoted_stringContext quoted_string() throws RecognitionException {
		Quoted_stringContext _localctx = new Quoted_stringContext(_ctx, getState());
		enterRule(_localctx, 168, RULE_quoted_string);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(842);
			_la = _input.LA(1);
			if ( !(_la==CHAR_STRING || _la==NATIONAL_CHAR_STRING_LIT) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class IdentifierContext extends ParserRuleContext {
		public Id_expressionContext id_expression() {
			return getRuleContext(Id_expressionContext.class,0);
		}
		public IdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IdentifierContext identifier() throws RecognitionException {
		IdentifierContext _localctx = new IdentifierContext(_ctx, getState());
		enterRule(_localctx, 170, RULE_identifier);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(844);
			id_expression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Id_expressionContext extends ParserRuleContext {
		public Regular_idContext regular_id() {
			return getRuleContext(Regular_idContext.class,0);
		}
		public TerminalNode DELIMITED_ID() { return getToken(PlSqlParser.DELIMITED_ID, 0); }
		public Id_expressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_id_expression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterId_expression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitId_expression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitId_expression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Id_expressionContext id_expression() throws RecognitionException {
		Id_expressionContext _localctx = new Id_expressionContext(_ctx, getState());
		enterRule(_localctx, 172, RULE_id_expression);
		try {
			setState(848);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case CREATE:
			case TABLE:
			case WITH:
			case SET:
			case SELECT:
			case INSERT:
			case UPDATE:
			case DELETE:
			case MERGE:
			case INTO:
			case FROM:
			case WHERE:
			case PROCEDURE:
			case FUNCTION:
			case RETURN:
			case BEGIN:
			case END:
			case EXCEPTION:
			case WHEN:
			case THEN:
			case ELSE:
			case ELSIF:
			case IF:
			case LOOP:
			case FOR:
			case WHILE:
			case EXIT:
			case CONTINUE:
			case RAISE:
			case DECLARE:
			case CALL:
			case AS:
			case IS:
			case START:
			case TRANSACTION:
			case COMMIT:
			case ROLLBACK:
			case AND:
			case OR:
			case NOT:
			case IN:
			case OUT:
			case CONSTANT:
			case DEFAULT:
			case INT:
			case BYTE:
			case SMALLINT:
			case BIGINT:
			case NUMBER:
			case DECIMAL:
			case DOUBLE:
			case FLOAT:
			case VARCHAR:
			case VARCHAR2:
			case STRING:
			case BOOLEAN:
			case DATE:
			case TIMESTAMP:
			case BINARY:
			case BLOB:
			case REGULAR_ID:
				enterOuterAlt(_localctx, 1);
				{
				setState(846);
				regular_id();
				}
				break;
			case DELIMITED_ID:
				enterOuterAlt(_localctx, 2);
				{
				setState(847);
				match(DELIMITED_ID);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Regular_idContext extends ParserRuleContext {
		public TerminalNode REGULAR_ID() { return getToken(PlSqlParser.REGULAR_ID, 0); }
		public TerminalNode AS() { return getToken(PlSqlParser.AS, 0); }
		public TerminalNode IS() { return getToken(PlSqlParser.IS, 0); }
		public TerminalNode IN() { return getToken(PlSqlParser.IN, 0); }
		public TerminalNode OUT() { return getToken(PlSqlParser.OUT, 0); }
		public TerminalNode AND() { return getToken(PlSqlParser.AND, 0); }
		public TerminalNode OR() { return getToken(PlSqlParser.OR, 0); }
		public TerminalNode NOT() { return getToken(PlSqlParser.NOT, 0); }
		public TerminalNode SET() { return getToken(PlSqlParser.SET, 0); }
		public TerminalNode CALL() { return getToken(PlSqlParser.CALL, 0); }
		public TerminalNode CREATE() { return getToken(PlSqlParser.CREATE, 0); }
		public TerminalNode TABLE() { return getToken(PlSqlParser.TABLE, 0); }
		public TerminalNode WITH() { return getToken(PlSqlParser.WITH, 0); }
		public TerminalNode SELECT() { return getToken(PlSqlParser.SELECT, 0); }
		public TerminalNode INSERT() { return getToken(PlSqlParser.INSERT, 0); }
		public TerminalNode UPDATE() { return getToken(PlSqlParser.UPDATE, 0); }
		public TerminalNode DELETE() { return getToken(PlSqlParser.DELETE, 0); }
		public TerminalNode MERGE() { return getToken(PlSqlParser.MERGE, 0); }
		public TerminalNode INTO() { return getToken(PlSqlParser.INTO, 0); }
		public TerminalNode FROM() { return getToken(PlSqlParser.FROM, 0); }
		public TerminalNode WHERE() { return getToken(PlSqlParser.WHERE, 0); }
		public TerminalNode BEGIN() { return getToken(PlSqlParser.BEGIN, 0); }
		public TerminalNode END() { return getToken(PlSqlParser.END, 0); }
		public TerminalNode LOOP() { return getToken(PlSqlParser.LOOP, 0); }
		public TerminalNode FOR() { return getToken(PlSqlParser.FOR, 0); }
		public TerminalNode WHILE() { return getToken(PlSqlParser.WHILE, 0); }
		public TerminalNode IF() { return getToken(PlSqlParser.IF, 0); }
		public TerminalNode THEN() { return getToken(PlSqlParser.THEN, 0); }
		public TerminalNode ELSE() { return getToken(PlSqlParser.ELSE, 0); }
		public TerminalNode ELSIF() { return getToken(PlSqlParser.ELSIF, 0); }
		public TerminalNode EXIT() { return getToken(PlSqlParser.EXIT, 0); }
		public TerminalNode CONTINUE() { return getToken(PlSqlParser.CONTINUE, 0); }
		public TerminalNode RAISE() { return getToken(PlSqlParser.RAISE, 0); }
		public TerminalNode RETURN() { return getToken(PlSqlParser.RETURN, 0); }
		public TerminalNode DECLARE() { return getToken(PlSqlParser.DECLARE, 0); }
		public TerminalNode EXCEPTION() { return getToken(PlSqlParser.EXCEPTION, 0); }
		public TerminalNode WHEN() { return getToken(PlSqlParser.WHEN, 0); }
		public TerminalNode PROCEDURE() { return getToken(PlSqlParser.PROCEDURE, 0); }
		public TerminalNode FUNCTION() { return getToken(PlSqlParser.FUNCTION, 0); }
		public TerminalNode START() { return getToken(PlSqlParser.START, 0); }
		public TerminalNode TRANSACTION() { return getToken(PlSqlParser.TRANSACTION, 0); }
		public TerminalNode COMMIT() { return getToken(PlSqlParser.COMMIT, 0); }
		public TerminalNode ROLLBACK() { return getToken(PlSqlParser.ROLLBACK, 0); }
		public TerminalNode CONSTANT() { return getToken(PlSqlParser.CONSTANT, 0); }
		public TerminalNode DEFAULT() { return getToken(PlSqlParser.DEFAULT, 0); }
		public TerminalNode INT() { return getToken(PlSqlParser.INT, 0); }
		public TerminalNode BIGINT() { return getToken(PlSqlParser.BIGINT, 0); }
		public TerminalNode SMALLINT() { return getToken(PlSqlParser.SMALLINT, 0); }
		public TerminalNode BYTE() { return getToken(PlSqlParser.BYTE, 0); }
		public TerminalNode NUMBER() { return getToken(PlSqlParser.NUMBER, 0); }
		public TerminalNode DECIMAL() { return getToken(PlSqlParser.DECIMAL, 0); }
		public TerminalNode DOUBLE() { return getToken(PlSqlParser.DOUBLE, 0); }
		public TerminalNode FLOAT() { return getToken(PlSqlParser.FLOAT, 0); }
		public TerminalNode VARCHAR() { return getToken(PlSqlParser.VARCHAR, 0); }
		public TerminalNode VARCHAR2() { return getToken(PlSqlParser.VARCHAR2, 0); }
		public TerminalNode STRING() { return getToken(PlSqlParser.STRING, 0); }
		public TerminalNode BOOLEAN() { return getToken(PlSqlParser.BOOLEAN, 0); }
		public TerminalNode DATE() { return getToken(PlSqlParser.DATE, 0); }
		public TerminalNode TIMESTAMP() { return getToken(PlSqlParser.TIMESTAMP, 0); }
		public TerminalNode BINARY() { return getToken(PlSqlParser.BINARY, 0); }
		public TerminalNode BLOB() { return getToken(PlSqlParser.BLOB, 0); }
		public Regular_idContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_regular_id; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterRegular_id(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitRegular_id(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitRegular_id(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Regular_idContext regular_id() throws RecognitionException {
		Regular_idContext _localctx = new Regular_idContext(_ctx, getState());
		enterRule(_localctx, 174, RULE_regular_id);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(850);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & -4494805681840130L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & 8589934847L) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public boolean sempred(RuleContext _localctx, int ruleIndex, int predIndex) {
		switch (ruleIndex) {
		case 56:
			return logical_expression_sempred((Logical_expressionContext)_localctx, predIndex);
		case 59:
			return relational_expression_sempred((Relational_expressionContext)_localctx, predIndex);
		case 64:
			return concatenation_sempred((ConcatenationContext)_localctx, predIndex);
		}
		return true;
	}
	private boolean logical_expression_sempred(Logical_expressionContext _localctx, int predIndex) {
		switch (predIndex) {
		case 0:
			return precpred(_ctx, 2);
		case 1:
			return precpred(_ctx, 1);
		}
		return true;
	}
	private boolean relational_expression_sempred(Relational_expressionContext _localctx, int predIndex) {
		switch (predIndex) {
		case 2:
			return precpred(_ctx, 2);
		}
		return true;
	}
	private boolean concatenation_sempred(ConcatenationContext _localctx, int predIndex) {
		switch (predIndex) {
		case 3:
			return precpred(_ctx, 7);
		case 4:
			return precpred(_ctx, 6);
		case 5:
			return precpred(_ctx, 5);
		case 6:
			return precpred(_ctx, 4);
		case 7:
			return precpred(_ctx, 3);
		case 8:
			return precpred(_ctx, 2);
		}
		return true;
	}

	public static final String _serializedATN =
		"\u0004\u0001d\u0355\u0002\u0000\u0007\u0000\u0002\u0001\u0007\u0001\u0002"+
		"\u0002\u0007\u0002\u0002\u0003\u0007\u0003\u0002\u0004\u0007\u0004\u0002"+
		"\u0005\u0007\u0005\u0002\u0006\u0007\u0006\u0002\u0007\u0007\u0007\u0002"+
		"\b\u0007\b\u0002\t\u0007\t\u0002\n\u0007\n\u0002\u000b\u0007\u000b\u0002"+
		"\f\u0007\f\u0002\r\u0007\r\u0002\u000e\u0007\u000e\u0002\u000f\u0007\u000f"+
		"\u0002\u0010\u0007\u0010\u0002\u0011\u0007\u0011\u0002\u0012\u0007\u0012"+
		"\u0002\u0013\u0007\u0013\u0002\u0014\u0007\u0014\u0002\u0015\u0007\u0015"+
		"\u0002\u0016\u0007\u0016\u0002\u0017\u0007\u0017\u0002\u0018\u0007\u0018"+
		"\u0002\u0019\u0007\u0019\u0002\u001a\u0007\u001a\u0002\u001b\u0007\u001b"+
		"\u0002\u001c\u0007\u001c\u0002\u001d\u0007\u001d\u0002\u001e\u0007\u001e"+
		"\u0002\u001f\u0007\u001f\u0002 \u0007 \u0002!\u0007!\u0002\"\u0007\"\u0002"+
		"#\u0007#\u0002$\u0007$\u0002%\u0007%\u0002&\u0007&\u0002\'\u0007\'\u0002"+
		"(\u0007(\u0002)\u0007)\u0002*\u0007*\u0002+\u0007+\u0002,\u0007,\u0002"+
		"-\u0007-\u0002.\u0007.\u0002/\u0007/\u00020\u00070\u00021\u00071\u0002"+
		"2\u00072\u00023\u00073\u00024\u00074\u00025\u00075\u00026\u00076\u0002"+
		"7\u00077\u00028\u00078\u00029\u00079\u0002:\u0007:\u0002;\u0007;\u0002"+
		"<\u0007<\u0002=\u0007=\u0002>\u0007>\u0002?\u0007?\u0002@\u0007@\u0002"+
		"A\u0007A\u0002B\u0007B\u0002C\u0007C\u0002D\u0007D\u0002E\u0007E\u0002"+
		"F\u0007F\u0002G\u0007G\u0002H\u0007H\u0002I\u0007I\u0002J\u0007J\u0002"+
		"K\u0007K\u0002L\u0007L\u0002M\u0007M\u0002N\u0007N\u0002O\u0007O\u0002"+
		"P\u0007P\u0002Q\u0007Q\u0002R\u0007R\u0002S\u0007S\u0002T\u0007T\u0002"+
		"U\u0007U\u0002V\u0007V\u0002W\u0007W\u0001\u0000\u0001\u0000\u0003\u0000"+
		"\u00b3\b\u0000\u0005\u0000\u00b5\b\u0000\n\u0000\f\u0000\u00b8\t\u0000"+
		"\u0001\u0000\u0001\u0000\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001"+
		"\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001"+
		"\u0001\u0001\u0001\u0001\u0001\u0001\u0003\u0001\u00c9\b\u0001\u0001\u0002"+
		"\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0005\u0002"+
		"\u00d1\b\u0002\n\u0002\f\u0002\u00d4\t\u0002\u0001\u0002\u0001\u0002\u0003"+
		"\u0002\u00d8\b\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0003"+
		"\u0002\u00de\b\u0002\u0001\u0002\u0003\u0002\u00e1\b\u0002\u0001\u0002"+
		"\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0003\u0001\u0003\u0001\u0003"+
		"\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0005\u0003\u00ee\b\u0003"+
		"\n\u0003\f\u0003\u00f1\t\u0003\u0001\u0003\u0001\u0003\u0003\u0003\u00f5"+
		"\b\u0003\u0001\u0003\u0001\u0003\u0003\u0003\u00f9\b\u0003\u0001\u0003"+
		"\u0003\u0003\u00fc\b\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
		"\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0005\u0003\u0005\u0106\b\u0005"+
		"\u0001\u0005\u0003\u0005\u0109\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0005\u0006\u0114\b\u0006\n\u0006\f\u0006\u0117\t\u0006\u0001\u0006\u0001"+
		"\u0006\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001"+
		"\u0007\u0005\u0007\u0121\b\u0007\n\u0007\f\u0007\u0124\t\u0007\u0001\b"+
		"\u0001\b\u0001\b\u0001\b\u0001\b\u0001\b\u0001\b\u0001\t\u0001\t\u0001"+
		"\t\u0001\t\u0001\t\u0001\n\u0001\n\u0001\n\u0005\n\u0135\b\n\n\n\f\n\u0138"+
		"\t\n\u0001\u000b\u0001\u000b\u0001\u000b\u0001\u000b\u0001\f\u0001\f\u0001"+
		"\r\u0001\r\u0001\r\u0001\r\u0005\r\u0144\b\r\n\r\f\r\u0147\t\r\u0001\r"+
		"\u0001\r\u0003\r\u014b\b\r\u0001\u000e\u0001\u000e\u0001\u000f\u0001\u000f"+
		"\u0001\u000f\u0001\u0010\u0001\u0010\u0001\u0010\u0005\u0010\u0155\b\u0010"+
		"\n\u0010\f\u0010\u0158\t\u0010\u0001\u0010\u0001\u0010\u0001\u0011\u0001"+
		"\u0011\u0005\u0011\u015e\b\u0011\n\u0011\f\u0011\u0161\t\u0011\u0001\u0011"+
		"\u0001\u0011\u0001\u0012\u0001\u0012\u0005\u0012\u0167\b\u0012\n\u0012"+
		"\f\u0012\u016a\t\u0012\u0001\u0012\u0001\u0012\u0001\u0013\u0001\u0013"+
		"\u0001\u0013\u0005\u0013\u0171\b\u0013\n\u0013\f\u0013\u0174\t\u0013\u0001"+
		"\u0013\u0001\u0013\u0001\u0014\u0001\u0014\u0005\u0014\u017a\b\u0014\n"+
		"\u0014\f\u0014\u017d\t\u0014\u0001\u0014\u0001\u0014\u0001\u0015\u0001"+
		"\u0015\u0001\u0015\u0005\u0015\u0184\b\u0015\n\u0015\f\u0015\u0187\t\u0015"+
		"\u0001\u0015\u0001\u0015\u0001\u0016\u0001\u0016\u0005\u0016\u018d\b\u0016"+
		"\n\u0016\f\u0016\u0190\t\u0016\u0001\u0016\u0001\u0016\u0001\u0017\u0001"+
		"\u0017\u0005\u0017\u0196\b\u0017\n\u0017\f\u0017\u0199\t\u0017\u0001\u0017"+
		"\u0003\u0017\u019c\b\u0017\u0001\u0017\u0003\u0017\u019f\b\u0017\u0001"+
		"\u0018\u0001\u0018\u0001\u0018\u0001\u0019\u0004\u0019\u01a5\b\u0019\u000b"+
		"\u0019\f\u0019\u01a6\u0001\u001a\u0001\u001a\u0001\u001b\u0001\u001b\u0003"+
		"\u001b\u01ad\b\u001b\u0001\u001b\u0001\u001b\u0001\u001b\u0003\u001b\u01b2"+
		"\b\u001b\u0001\u001b\u0003\u001b\u01b5\b\u001b\u0001\u001b\u0001\u001b"+
		"\u0001\u001c\u0001\u001c\u0001\u001c\u0004\u001c\u01bc\b\u001c\u000b\u001c"+
		"\f\u001c\u01bd\u0001\u001d\u0001\u001d\u0001\u001d\u0001\u001d\u0001\u001d"+
		"\u0001\u001d\u0001\u001d\u0001\u001d\u0001\u001d\u0001\u001d\u0003\u001d"+
		"\u01ca\b\u001d\u0001\u001e\u0001\u001e\u0001\u001e\u0001\u001e\u0001\u001e"+
		"\u0001\u001e\u0003\u001e\u01d2\b\u001e\u0001\u001f\u0001\u001f\u0001\u001f"+
		"\u0001\u001f\u0001 \u0001 \u0001!\u0001!\u0001\"\u0001\"\u0001\"\u0001"+
		"\"\u0001\"\u0005\"\u01e1\b\"\n\"\f\"\u01e4\t\"\u0001\"\u0003\"\u01e7\b"+
		"\"\u0001\"\u0001\"\u0001\"\u0001#\u0001#\u0001#\u0001#\u0001#\u0001$\u0001"+
		"$\u0001$\u0001%\u0001%\u0001%\u0001%\u0003%\u01f8\b%\u0001%\u0001%\u0001"+
		"%\u0001%\u0001%\u0001&\u0001&\u0001&\u0001&\u0001&\u0001&\u0001&\u0001"+
		"&\u0001&\u0001&\u0001&\u0001&\u0003&\u020b\b&\u0001\'\u0001\'\u0005\'"+
		"\u020f\b\'\n\'\f\'\u0212\t\'\u0001\'\u0001\'\u0001(\u0001(\u0001)\u0001"+
		")\u0001*\u0001*\u0003*\u021c\b*\u0001+\u0001+\u0003+\u0220\b+\u0001,\u0003"+
		",\u0223\b,\u0001,\u0001,\u0003,\u0227\b,\u0001-\u0001-\u0001-\u0001-\u0004"+
		"-\u022d\b-\u000b-\f-\u022e\u0003-\u0231\b-\u0001-\u0001-\u0001.\u0001"+
		".\u0001.\u0001.\u0005.\u0239\b.\n.\f.\u023c\t.\u0001.\u0001.\u0001.\u0001"+
		"/\u0001/\u00010\u00010\u00010\u00010\u00010\u00010\u00010\u00030\u024a"+
		"\b0\u00011\u00011\u00051\u024e\b1\n1\f1\u0251\t1\u00011\u00011\u00012"+
		"\u00012\u00012\u00052\u0258\b2\n2\f2\u025b\t2\u00012\u00012\u00013\u0001"+
		"3\u00053\u0261\b3\n3\f3\u0264\t3\u00013\u00013\u00014\u00014\u00014\u0005"+
		"4\u026b\b4\n4\f4\u026e\t4\u00014\u00014\u00015\u00015\u00016\u00016\u0001"+
		"6\u00056\u0277\b6\n6\f6\u027a\t6\u00017\u00017\u00018\u00018\u00018\u0001"+
		"8\u00018\u00018\u00018\u00018\u00018\u00058\u0287\b8\n8\f8\u028a\t8\u0001"+
		"9\u00039\u028d\b9\u00019\u00019\u00019\u00039\u0292\b9\u00019\u00039\u0295"+
		"\b9\u0001:\u0001:\u0001;\u0001;\u0001;\u0001;\u0001;\u0001;\u0001;\u0005"+
		";\u02a0\b;\n;\f;\u02a3\t;\u0001<\u0001<\u0003<\u02a7\b<\u0001<\u0001<"+
		"\u0001<\u0001<\u0001<\u0001<\u0001<\u0001<\u0003<\u02b1\b<\u0003<\u02b3"+
		"\b<\u0003<\u02b5\b<\u0001=\u0001=\u0001=\u0001=\u0001=\u0001=\u0001=\u0001"+
		"=\u0001=\u0001=\u0003=\u02c1\b=\u0003=\u02c3\b=\u0001>\u0001>\u0001>\u0001"+
		">\u0005>\u02c9\b>\n>\f>\u02cc\t>\u0001>\u0001>\u0001>\u0001>\u0001>\u0003"+
		">\u02d3\b>\u0001?\u0001?\u0001?\u0001?\u0001@\u0001@\u0001@\u0001@\u0001"+
		"@\u0001@\u0001@\u0001@\u0001@\u0001@\u0001@\u0001@\u0001@\u0001@\u0001"+
		"@\u0001@\u0001@\u0001@\u0001@\u0001@\u0001@\u0001@\u0005@\u02ef\b@\n@"+
		"\f@\u02f2\t@\u0001A\u0001A\u0001A\u0003A\u02f7\bA\u0001B\u0001B\u0001"+
		"B\u0001B\u0001B\u0001B\u0001B\u0001B\u0003B\u0301\bB\u0001C\u0001C\u0001"+
		"D\u0001D\u0001E\u0001E\u0001F\u0001F\u0001G\u0001G\u0001H\u0001H\u0001"+
		"I\u0001I\u0001J\u0001J\u0001J\u0001J\u0005J\u0315\bJ\nJ\fJ\u0318\tJ\u0003"+
		"J\u031a\bJ\u0001J\u0001J\u0001K\u0001K\u0001L\u0001L\u0001M\u0001M\u0003"+
		"M\u0324\bM\u0001N\u0001N\u0001N\u0001N\u0003N\u032a\bN\u0001N\u0001N\u0001"+
		"O\u0001O\u0001P\u0001P\u0001Q\u0001Q\u0001Q\u0005Q\u0335\bQ\nQ\fQ\u0338"+
		"\tQ\u0001R\u0001R\u0001R\u0003R\u033d\bR\u0001R\u0001R\u0001R\u0001R\u0001"+
		"R\u0001R\u0001R\u0001R\u0003R\u0347\bR\u0001S\u0001S\u0001T\u0001T\u0001"+
		"U\u0001U\u0001V\u0001V\u0003V\u0351\bV\u0001W\u0001W\u0001W\u000e\u0115"+
		"\u0122\u0156\u015f\u0168\u0172\u017b\u0185\u018e\u0210\u024f\u0259\u0262"+
		"\u026c\u0003pv\u0080X\u0000\u0002\u0004\u0006\b\n\f\u000e\u0010\u0012"+
		"\u0014\u0016\u0018\u001a\u001c\u001e \"$&(*,.02468:<>@BDFHJLNPRTVXZ\\"+
		"^`bdfhjlnprtvxz|~\u0080\u0082\u0084\u0086\u0088\u008a\u008c\u008e\u0090"+
		"\u0092\u0094\u0096\u0098\u009a\u009c\u009e\u00a0\u00a2\u00a4\u00a6\u00a8"+
		"\u00aa\u00ac\u00ae\u0000\n\u0001\u000045\u0002\u000077OO\u0001\u0001K"+
		"K\u0001\u0000/2\u0001\u0000WX\u0001\u0000RS\u0002\u00008@BG\u0001\u0000"+
		"]^\u0001\u0000[\\\u0005\u0000\u0001\f\u000e\u001e *4Gaa\u037a\u0000\u00b6"+
		"\u0001\u0000\u0000\u0000\u0002\u00c8\u0001\u0000\u0000\u0000\u0004\u00ca"+
		"\u0001\u0000\u0000\u0000\u0006\u00e6\u0001\u0000\u0000\u0000\b\u0101\u0001"+
		"\u0000\u0000\u0000\n\u0105\u0001\u0000\u0000\u0000\f\u010d\u0001\u0000"+
		"\u0000\u0000\u000e\u011a\u0001\u0000\u0000\u0000\u0010\u0125\u0001\u0000"+
		"\u0000\u0000\u0012\u012c\u0001\u0000\u0000\u0000\u0014\u0131\u0001\u0000"+
		"\u0000\u0000\u0016\u0139\u0001\u0000\u0000\u0000\u0018\u013d\u0001\u0000"+
		"\u0000\u0000\u001a\u014a\u0001\u0000\u0000\u0000\u001c\u014c\u0001\u0000"+
		"\u0000\u0000\u001e\u014e\u0001\u0000\u0000\u0000 \u0151\u0001\u0000\u0000"+
		"\u0000\"\u015b\u0001\u0000\u0000\u0000$\u0164\u0001\u0000\u0000\u0000"+
		"&\u016d\u0001\u0000\u0000\u0000(\u0177\u0001\u0000\u0000\u0000*\u0180"+
		"\u0001\u0000\u0000\u0000,\u018a\u0001\u0000\u0000\u0000.\u0193\u0001\u0000"+
		"\u0000\u00000\u01a0\u0001\u0000\u0000\u00002\u01a4\u0001\u0000\u0000\u0000"+
		"4\u01a8\u0001\u0000\u0000\u00006\u01aa\u0001\u0000\u0000\u00008\u01bb"+
		"\u0001\u0000\u0000\u0000:\u01c9\u0001\u0000\u0000\u0000<\u01d1\u0001\u0000"+
		"\u0000\u0000>\u01d3\u0001\u0000\u0000\u0000@\u01d7\u0001\u0000\u0000\u0000"+
		"B\u01d9\u0001\u0000\u0000\u0000D\u01db\u0001\u0000\u0000\u0000F\u01eb"+
		"\u0001\u0000\u0000\u0000H\u01f0\u0001\u0000\u0000\u0000J\u01f7\u0001\u0000"+
		"\u0000\u0000L\u020a\u0001\u0000\u0000\u0000N\u020c\u0001\u0000\u0000\u0000"+
		"P\u0215\u0001\u0000\u0000\u0000R\u0217\u0001\u0000\u0000\u0000T\u0219"+
		"\u0001\u0000\u0000\u0000V\u021d\u0001\u0000\u0000\u0000X\u0222\u0001\u0000"+
		"\u0000\u0000Z\u0228\u0001\u0000\u0000\u0000\\\u0234\u0001\u0000\u0000"+
		"\u0000^\u0240\u0001\u0000\u0000\u0000`\u0249\u0001\u0000\u0000\u0000b"+
		"\u024b\u0001\u0000\u0000\u0000d\u0254\u0001\u0000\u0000\u0000f\u025e\u0001"+
		"\u0000\u0000\u0000h\u0267\u0001\u0000\u0000\u0000j\u0271\u0001\u0000\u0000"+
		"\u0000l\u0273\u0001\u0000\u0000\u0000n\u027b\u0001\u0000\u0000\u0000p"+
		"\u027d\u0001\u0000\u0000\u0000r\u028c\u0001\u0000\u0000\u0000t\u0296\u0001"+
		"\u0000\u0000\u0000v\u0298\u0001\u0000\u0000\u0000x\u02a4\u0001\u0000\u0000"+
		"\u0000z\u02c2\u0001\u0000\u0000\u0000|\u02d2\u0001\u0000\u0000\u0000~"+
		"\u02d4\u0001\u0000\u0000\u0000\u0080\u02d8\u0001\u0000\u0000\u0000\u0082"+
		"\u02f6\u0001\u0000\u0000\u0000\u0084\u0300\u0001\u0000\u0000\u0000\u0086"+
		"\u0302\u0001\u0000\u0000\u0000\u0088\u0304\u0001\u0000\u0000\u0000\u008a"+
		"\u0306\u0001\u0000\u0000\u0000\u008c\u0308\u0001\u0000\u0000\u0000\u008e"+
		"\u030a\u0001\u0000\u0000\u0000\u0090\u030c\u0001\u0000\u0000\u0000\u0092"+
		"\u030e\u0001\u0000\u0000\u0000\u0094\u0310\u0001\u0000\u0000\u0000\u0096"+
		"\u031d\u0001\u0000\u0000\u0000\u0098\u031f\u0001\u0000\u0000\u0000\u009a"+
		"\u0321\u0001\u0000\u0000\u0000\u009c\u0325\u0001\u0000\u0000\u0000\u009e"+
		"\u032d\u0001\u0000\u0000\u0000\u00a0\u032f\u0001\u0000\u0000\u0000\u00a2"+
		"\u0331\u0001\u0000\u0000\u0000\u00a4\u0346\u0001\u0000\u0000\u0000\u00a6"+
		"\u0348\u0001\u0000\u0000\u0000\u00a8\u034a\u0001\u0000\u0000\u0000\u00aa"+
		"\u034c\u0001\u0000\u0000\u0000\u00ac\u0350\u0001\u0000\u0000\u0000\u00ae"+
		"\u0352\u0001\u0000\u0000\u0000\u00b0\u00b2\u0003\u0002\u0001\u0000\u00b1"+
		"\u00b3\u0005K\u0000\u0000\u00b2\u00b1\u0001\u0000\u0000\u0000\u00b2\u00b3"+
		"\u0001\u0000\u0000\u0000\u00b3\u00b5\u0001\u0000\u0000\u0000\u00b4\u00b0"+
		"\u0001\u0000\u0000\u0000\u00b5\u00b8\u0001\u0000\u0000\u0000\u00b6\u00b4"+
		"\u0001\u0000\u0000\u0000\u00b6\u00b7\u0001\u0000\u0000\u0000\u00b7\u00b9"+
		"\u0001\u0000\u0000\u0000\u00b8\u00b6\u0001\u0000\u0000\u0000\u00b9\u00ba"+
		"\u0005\u0000\u0000\u0001\u00ba\u0001\u0001\u0000\u0000\u0000\u00bb\u00c9"+
		"\u0003\n\u0005\u0000\u00bc\u00c9\u0003\u0006\u0003\u0000\u00bd\u00c9\u0003"+
		"\b\u0004\u0000\u00be\u00c9\u0003\u0010\b\u0000\u00bf\u00c9\u0003\f\u0006"+
		"\u0000\u00c0\u00c9\u0003,\u0016\u0000\u00c1\u00c9\u0003\"\u0011\u0000"+
		"\u00c2\u00c9\u0003(\u0014\u0000\u00c3\u00c9\u0003$\u0012\u0000\u00c4\u00c9"+
		"\u0003&\u0013\u0000\u00c5\u00c9\u0003*\u0015\u0000\u00c6\u00c9\u0003 "+
		"\u0010\u0000\u00c7\u00c9\u0003X,\u0000\u00c8\u00bb\u0001\u0000\u0000\u0000"+
		"\u00c8\u00bc\u0001\u0000\u0000\u0000\u00c8\u00bd\u0001\u0000\u0000\u0000"+
		"\u00c8\u00be\u0001\u0000\u0000\u0000\u00c8\u00bf\u0001\u0000\u0000\u0000"+
		"\u00c8\u00c0\u0001\u0000\u0000\u0000\u00c8\u00c1\u0001\u0000\u0000\u0000"+
		"\u00c8\u00c2\u0001\u0000\u0000\u0000\u00c8\u00c3\u0001\u0000\u0000\u0000"+
		"\u00c8\u00c4\u0001\u0000\u0000\u0000\u00c8\u00c5\u0001\u0000\u0000\u0000"+
		"\u00c8\u00c6\u0001\u0000\u0000\u0000\u00c8\u00c7\u0001\u0000\u0000\u0000"+
		"\u00c9\u0003\u0001\u0000\u0000\u0000\u00ca\u00cb\u0005\u000f\u0000\u0000"+
		"\u00cb\u00d7\u0003\u00aaU\u0000\u00cc\u00cd\u0005H\u0000\u0000\u00cd\u00d2"+
		"\u0003.\u0017\u0000\u00ce\u00cf\u0005J\u0000\u0000\u00cf\u00d1\u0003."+
		"\u0017\u0000\u00d0\u00ce\u0001\u0000\u0000\u0000\u00d1\u00d4\u0001\u0000"+
		"\u0000\u0000\u00d2\u00d0\u0001\u0000\u0000\u0000\u00d2\u00d3\u0001\u0000"+
		"\u0000\u0000\u00d3\u00d5\u0001\u0000\u0000\u0000\u00d4\u00d2\u0001\u0000"+
		"\u0000\u0000\u00d5\u00d6\u0005I\u0000\u0000\u00d6\u00d8\u0001\u0000\u0000"+
		"\u0000\u00d7\u00cc\u0001\u0000\u0000\u0000\u00d7\u00d8\u0001\u0000\u0000"+
		"\u0000\u00d8\u00d9\u0001\u0000\u0000\u0000\u00d9\u00da\u0005\u0010\u0000"+
		"\u0000\u00da\u00db\u0003\u0098L\u0000\u00db\u00dd\u0005\"\u0000\u0000"+
		"\u00dc\u00de\u0005 \u0000\u0000\u00dd\u00dc\u0001\u0000\u0000\u0000\u00dd"+
		"\u00de\u0001\u0000\u0000\u0000\u00de\u00e0\u0001\u0000\u0000\u0000\u00df"+
		"\u00e1\u00032\u0019\u0000\u00e0\u00df\u0001\u0000\u0000\u0000\u00e0\u00e1"+
		"\u0001\u0000\u0000\u0000\u00e1\u00e2\u0001\u0000\u0000\u0000\u00e2\u00e3"+
		"\u0003Z-\u0000\u00e3\u00e4\u0001\u0000\u0000\u0000\u00e4\u00e5\u0005K"+
		"\u0000\u0000\u00e5\u0005\u0001\u0000\u0000\u0000\u00e6\u00e7\u0005\u0001"+
		"\u0000\u0000\u00e7\u00e8\u0005\u000e\u0000\u0000\u00e8\u00f4\u0003\u008a"+
		"E\u0000\u00e9\u00ea\u0005H\u0000\u0000\u00ea\u00ef\u0003.\u0017\u0000"+
		"\u00eb\u00ec\u0005J\u0000\u0000\u00ec\u00ee\u0003.\u0017\u0000\u00ed\u00eb"+
		"\u0001\u0000\u0000\u0000\u00ee\u00f1\u0001\u0000\u0000\u0000\u00ef\u00ed"+
		"\u0001\u0000\u0000\u0000\u00ef\u00f0\u0001\u0000\u0000\u0000\u00f0\u00f2"+
		"\u0001\u0000\u0000\u0000\u00f1\u00ef\u0001\u0000\u0000\u0000\u00f2\u00f3"+
		"\u0005I\u0000\u0000\u00f3\u00f5\u0001\u0000\u0000\u0000\u00f4\u00e9\u0001"+
		"\u0000\u0000\u0000\u00f4\u00f5\u0001\u0000\u0000\u0000\u00f5\u00f6\u0001"+
		"\u0000\u0000\u0000\u00f6\u00f8\u0005\"\u0000\u0000\u00f7\u00f9\u0005 "+
		"\u0000\u0000\u00f8\u00f7\u0001\u0000\u0000\u0000\u00f8\u00f9\u0001\u0000"+
		"\u0000\u0000\u00f9\u00fb\u0001\u0000\u0000\u0000\u00fa\u00fc\u00032\u0019"+
		"\u0000\u00fb\u00fa\u0001\u0000\u0000\u0000\u00fb\u00fc\u0001\u0000\u0000"+
		"\u0000\u00fc\u00fd\u0001\u0000\u0000\u0000\u00fd\u00fe\u0003Z-\u0000\u00fe"+
		"\u00ff\u0001\u0000\u0000\u0000\u00ff\u0100\u0005K\u0000\u0000\u0100\u0007"+
		"\u0001\u0000\u0000\u0000\u0101\u0102\u0005\u0001\u0000\u0000\u0102\u0103"+
		"\u0003\u0004\u0002\u0000\u0103\t\u0001\u0000\u0000\u0000\u0104\u0106\u0005"+
		" \u0000\u0000\u0105\u0104\u0001\u0000\u0000\u0000\u0105\u0106\u0001\u0000"+
		"\u0000\u0000\u0106\u0108\u0001\u0000\u0000\u0000\u0107\u0109\u00032\u0019"+
		"\u0000\u0108\u0107\u0001\u0000\u0000\u0000\u0108\u0109\u0001\u0000\u0000"+
		"\u0000\u0109\u010a\u0001\u0000\u0000\u0000\u010a\u010b\u0003Z-\u0000\u010b"+
		"\u010c\u0005K\u0000\u0000\u010c\u000b\u0001\u0000\u0000\u0000\u010d\u010e"+
		"\u0005\u0001\u0000\u0000\u010e\u010f\u0005\u0002\u0000\u0000\u010f\u0110"+
		"\u0003\u0018\f\u0000\u0110\u0111\u0005\"\u0000\u0000\u0111\u0115\u0005"+
		"\u0005\u0000\u0000\u0112\u0114\t\u0000\u0000\u0000\u0113\u0112\u0001\u0000"+
		"\u0000\u0000\u0114\u0117\u0001\u0000\u0000\u0000\u0115\u0116\u0001\u0000"+
		"\u0000\u0000\u0115\u0113\u0001\u0000\u0000\u0000\u0116\u0118\u0001\u0000"+
		"\u0000\u0000\u0117\u0115\u0001\u0000\u0000\u0000\u0118\u0119\u0005K\u0000"+
		"\u0000\u0119\r\u0001\u0000\u0000\u0000\u011a\u011b\u0005\u0001\u0000\u0000"+
		"\u011b\u011c\u0005\u0002\u0000\u0000\u011c\u011d\u0003\u0018\f\u0000\u011d"+
		"\u011e\u0005\"\u0000\u0000\u011e\u0122\u0005\u0005\u0000\u0000\u011f\u0121"+
		"\t\u0000\u0000\u0000\u0120\u011f\u0001\u0000\u0000\u0000\u0121\u0124\u0001"+
		"\u0000\u0000\u0000\u0122\u0123\u0001\u0000\u0000\u0000\u0122\u0120\u0001"+
		"\u0000\u0000\u0000\u0123\u000f\u0001\u0000\u0000\u0000\u0124\u0122\u0001"+
		"\u0000\u0000\u0000\u0125\u0126\u0005\u0001\u0000\u0000\u0126\u0127\u0005"+
		"\u0002\u0000\u0000\u0127\u0128\u0003\u0018\f\u0000\u0128\u0129\u0003\u001a"+
		"\r\u0000\u0129\u012a\u0003\u0012\t\u0000\u012a\u012b\u0005K\u0000\u0000"+
		"\u012b\u0011\u0001\u0000\u0000\u0000\u012c\u012d\u0005\u0003\u0000\u0000"+
		"\u012d\u012e\u0005H\u0000\u0000\u012e\u012f\u0003\u0014\n\u0000\u012f"+
		"\u0130\u0005I\u0000\u0000\u0130\u0013\u0001\u0000\u0000\u0000\u0131\u0136"+
		"\u0003\u0016\u000b\u0000\u0132\u0133\u0005J\u0000\u0000\u0133\u0135\u0003"+
		"\u0016\u000b\u0000\u0134\u0132\u0001\u0000\u0000\u0000\u0135\u0138\u0001"+
		"\u0000\u0000\u0000\u0136\u0134\u0001\u0000\u0000\u0000\u0136\u0137\u0001"+
		"\u0000\u0000\u0000\u0137\u0015\u0001\u0000\u0000\u0000\u0138\u0136\u0001"+
		"\u0000\u0000\u0000\u0139\u013a\u0005[\u0000\u0000\u013a\u013b\u0005P\u0000"+
		"\u0000\u013b\u013c\u0005[\u0000\u0000\u013c\u0017\u0001\u0000\u0000\u0000"+
		"\u013d\u013e\u0003\u00aaU\u0000\u013e\u0019\u0001\u0000\u0000\u0000\u013f"+
		"\u0140\u0005H\u0000\u0000\u0140\u0145\u0003\u001c\u000e\u0000\u0141\u0142"+
		"\u0005J\u0000\u0000\u0142\u0144\u0003\u001c\u000e\u0000\u0143\u0141\u0001"+
		"\u0000\u0000\u0000\u0144\u0147\u0001\u0000\u0000\u0000\u0145\u0143\u0001"+
		"\u0000\u0000\u0000\u0145\u0146\u0001\u0000\u0000\u0000\u0146\u0148\u0001"+
		"\u0000\u0000\u0000\u0147\u0145\u0001\u0000\u0000\u0000\u0148\u0149\u0005"+
		"I\u0000\u0000\u0149\u014b\u0001\u0000\u0000\u0000\u014a\u013f\u0001\u0000"+
		"\u0000\u0000\u014a\u014b\u0001\u0000\u0000\u0000\u014b\u001b\u0001\u0000"+
		"\u0000\u0000\u014c\u014d\u0003\u001e\u000f\u0000\u014d\u001d\u0001\u0000"+
		"\u0000\u0000\u014e\u014f\u0003\u0092I\u0000\u014f\u0150\u0003\u009aM\u0000"+
		"\u0150\u001f\u0001\u0000\u0000\u0000\u0151\u0152\u0005\r\u0000\u0000\u0152"+
		"\u0156\u0005\u0002\u0000\u0000\u0153\u0155\t\u0000\u0000\u0000\u0154\u0153"+
		"\u0001\u0000\u0000\u0000\u0155\u0158\u0001\u0000\u0000\u0000\u0156\u0157"+
		"\u0001\u0000\u0000\u0000\u0156\u0154\u0001\u0000\u0000\u0000\u0157\u0159"+
		"\u0001\u0000\u0000\u0000\u0158\u0156\u0001\u0000\u0000\u0000\u0159\u015a"+
		"\u0005K\u0000\u0000\u015a!\u0001\u0000\u0000\u0000\u015b\u015f\u0005\u0005"+
		"\u0000\u0000\u015c\u015e\t\u0000\u0000\u0000\u015d\u015c\u0001\u0000\u0000"+
		"\u0000\u015e\u0161\u0001\u0000\u0000\u0000\u015f\u0160\u0001\u0000\u0000"+
		"\u0000\u015f\u015d\u0001\u0000\u0000\u0000\u0160\u0162\u0001\u0000\u0000"+
		"\u0000\u0161\u015f\u0001\u0000\u0000\u0000\u0162\u0163\u0005K\u0000\u0000"+
		"\u0163#\u0001\u0000\u0000\u0000\u0164\u0168\u0005\u0007\u0000\u0000\u0165"+
		"\u0167\t\u0000\u0000\u0000\u0166\u0165\u0001\u0000\u0000\u0000\u0167\u016a"+
		"\u0001\u0000\u0000\u0000\u0168\u0169\u0001\u0000\u0000\u0000\u0168\u0166"+
		"\u0001\u0000\u0000\u0000\u0169\u016b\u0001\u0000\u0000\u0000\u016a\u0168"+
		"\u0001\u0000\u0000\u0000\u016b\u016c\u0005K\u0000\u0000\u016c%\u0001\u0000"+
		"\u0000\u0000\u016d\u016e\u0005\b\u0000\u0000\u016e\u0172\u0005\u000b\u0000"+
		"\u0000\u016f\u0171\t\u0000\u0000\u0000\u0170\u016f\u0001\u0000\u0000\u0000"+
		"\u0171\u0174\u0001\u0000\u0000\u0000\u0172\u0173\u0001\u0000\u0000\u0000"+
		"\u0172\u0170\u0001\u0000\u0000\u0000\u0173\u0175\u0001\u0000\u0000\u0000"+
		"\u0174\u0172\u0001\u0000\u0000\u0000\u0175\u0176\u0005K\u0000\u0000\u0176"+
		"\'\u0001\u0000\u0000\u0000\u0177\u017b\u0005\u0006\u0000\u0000\u0178\u017a"+
		"\t\u0000\u0000\u0000\u0179\u0178\u0001\u0000\u0000\u0000\u017a\u017d\u0001"+
		"\u0000\u0000\u0000\u017b\u017c\u0001\u0000\u0000\u0000\u017b\u0179\u0001"+
		"\u0000\u0000\u0000\u017c\u017e\u0001\u0000\u0000\u0000\u017d\u017b\u0001"+
		"\u0000\u0000\u0000\u017e\u017f\u0005K\u0000\u0000\u017f)\u0001\u0000\u0000"+
		"\u0000\u0180\u0181\u0005\t\u0000\u0000\u0181\u0185\u0005\n\u0000\u0000"+
		"\u0182\u0184\t\u0000\u0000\u0000\u0183\u0182\u0001\u0000\u0000\u0000\u0184"+
		"\u0187\u0001\u0000\u0000\u0000\u0185\u0186\u0001\u0000\u0000\u0000\u0185"+
		"\u0183\u0001\u0000\u0000\u0000\u0186\u0188\u0001\u0000\u0000\u0000\u0187"+
		"\u0185\u0001\u0000\u0000\u0000\u0188\u0189\u0005K\u0000\u0000\u0189+\u0001"+
		"\u0000\u0000\u0000\u018a\u018e\u0005\u0004\u0000\u0000\u018b\u018d\t\u0000"+
		"\u0000\u0000\u018c\u018b\u0001\u0000\u0000\u0000\u018d\u0190\u0001\u0000"+
		"\u0000\u0000\u018e\u018f\u0001\u0000\u0000\u0000\u018e\u018c\u0001\u0000"+
		"\u0000\u0000\u018f\u0191\u0001\u0000\u0000\u0000\u0190\u018e\u0001\u0000"+
		"\u0000\u0000\u0191\u0192\u0005K\u0000\u0000\u0192-\u0001\u0000\u0000\u0000"+
		"\u0193\u0197\u0003\u0088D\u0000\u0194\u0196\u0007\u0000\u0000\u0000\u0195"+
		"\u0194\u0001\u0000\u0000\u0000\u0196\u0199\u0001\u0000\u0000\u0000\u0197"+
		"\u0195\u0001\u0000\u0000\u0000\u0197\u0198\u0001\u0000\u0000\u0000\u0198"+
		"\u019b\u0001\u0000\u0000\u0000\u0199\u0197\u0001\u0000\u0000\u0000\u019a"+
		"\u019c\u0003\u0098L\u0000\u019b\u019a\u0001\u0000\u0000\u0000\u019b\u019c"+
		"\u0001\u0000\u0000\u0000\u019c\u019e\u0001\u0000\u0000\u0000\u019d\u019f"+
		"\u00030\u0018\u0000\u019e\u019d\u0001\u0000\u0000\u0000\u019e\u019f\u0001"+
		"\u0000\u0000\u0000\u019f/\u0001\u0000\u0000\u0000\u01a0\u01a1\u0007\u0001"+
		"\u0000\u0000\u01a1\u01a2\u0003n7\u0000\u01a21\u0001\u0000\u0000\u0000"+
		"\u01a3\u01a5\u00034\u001a\u0000\u01a4\u01a3\u0001\u0000\u0000\u0000\u01a5"+
		"\u01a6\u0001\u0000\u0000\u0000\u01a6\u01a4\u0001\u0000\u0000\u0000\u01a6"+
		"\u01a7\u0001\u0000\u0000\u0000\u01a73\u0001\u0000\u0000\u0000\u01a8\u01a9"+
		"\u00036\u001b\u0000\u01a95\u0001\u0000\u0000\u0000\u01aa\u01ac\u0003\u00aa"+
		"U\u0000\u01ab\u01ad\u00056\u0000\u0000\u01ac\u01ab\u0001\u0000\u0000\u0000"+
		"\u01ac\u01ad\u0001\u0000\u0000\u0000\u01ad\u01ae\u0001\u0000\u0000\u0000"+
		"\u01ae\u01b1\u0003\u0098L\u0000\u01af\u01b0\u0005*\u0000\u0000\u01b0\u01b2"+
		"\u0005+\u0000\u0000\u01b1\u01af\u0001\u0000\u0000\u0000\u01b1\u01b2\u0001"+
		"\u0000\u0000\u0000\u01b2\u01b4\u0001\u0000\u0000\u0000\u01b3\u01b5\u0003"+
		"0\u0018\u0000\u01b4\u01b3\u0001\u0000\u0000\u0000\u01b4\u01b5\u0001\u0000"+
		"\u0000\u0000\u01b5\u01b6\u0001\u0000\u0000\u0000\u01b6\u01b7\u0005K\u0000"+
		"\u0000\u01b77\u0001\u0000\u0000\u0000\u01b8\u01b9\u0003:\u001d\u0000\u01b9"+
		"\u01ba\u0007\u0002\u0000\u0000\u01ba\u01bc\u0001\u0000\u0000\u0000\u01bb"+
		"\u01b8\u0001\u0000\u0000\u0000\u01bc\u01bd\u0001\u0000\u0000\u0000\u01bd"+
		"\u01bb\u0001\u0000\u0000\u0000\u01bd\u01be\u0001\u0000\u0000\u0000\u01be"+
		"9\u0001\u0000\u0000\u0000\u01bf\u01ca\u0003<\u001e\u0000\u01c0\u01ca\u0003"+
		">\u001f\u0000\u01c1\u01ca\u0003@ \u0000\u01c2\u01ca\u0003B!\u0000\u01c3"+
		"\u01ca\u0003D\"\u0000\u01c4\u01ca\u0003J%\u0000\u01c5\u01ca\u0003T*\u0000"+
		"\u01c6\u01ca\u0003V+\u0000\u01c7\u01ca\u0003^/\u0000\u01c8\u01ca\u0003"+
		"X,\u0000\u01c9\u01bf\u0001\u0000\u0000\u0000\u01c9\u01c0\u0001\u0000\u0000"+
		"\u0000\u01c9\u01c1\u0001\u0000\u0000\u0000\u01c9\u01c2\u0001\u0000\u0000"+
		"\u0000\u01c9\u01c3\u0001\u0000\u0000\u0000\u01c9\u01c4\u0001\u0000\u0000"+
		"\u0000\u01c9\u01c5\u0001\u0000\u0000\u0000\u01c9\u01c6\u0001\u0000\u0000"+
		"\u0000\u01c9\u01c7\u0001\u0000\u0000\u0000\u01c9\u01c8\u0001\u0000\u0000"+
		"\u0000\u01ca;\u0001\u0000\u0000\u0000\u01cb\u01cc\u0005$\u0000\u0000\u01cc"+
		"\u01d2\u0005%\u0000\u0000\u01cd\u01ce\u0005\u0011\u0000\u0000\u01ce\u01d2"+
		"\u0005%\u0000\u0000\u01cf\u01d2\u0005&\u0000\u0000\u01d0\u01d2\u0005\'"+
		"\u0000\u0000\u01d1\u01cb\u0001\u0000\u0000\u0000\u01d1\u01cd\u0001\u0000"+
		"\u0000\u0000\u01d1\u01cf\u0001\u0000\u0000\u0000\u01d1\u01d0\u0001\u0000"+
		"\u0000\u0000\u01d2=\u0001\u0000\u0000\u0000\u01d3\u01d4\u0003\u00a2Q\u0000"+
		"\u01d4\u01d5\u0005O\u0000\u0000\u01d5\u01d6\u0003n7\u0000\u01d6?\u0001"+
		"\u0000\u0000\u0000\u01d7\u01d8\u0005\u001d\u0000\u0000\u01d8A\u0001\u0000"+
		"\u0000\u0000\u01d9\u01da\u0005\u001c\u0000\u0000\u01daC\u0001\u0000\u0000"+
		"\u0000\u01db\u01dc\u0005\u0018\u0000\u0000\u01dc\u01dd\u0003j5\u0000\u01dd"+
		"\u01de\u0005\u0015\u0000\u0000\u01de\u01e2\u00038\u001c\u0000\u01df\u01e1"+
		"\u0003F#\u0000\u01e0\u01df\u0001\u0000\u0000\u0000\u01e1\u01e4\u0001\u0000"+
		"\u0000\u0000\u01e2\u01e0\u0001\u0000\u0000\u0000\u01e2\u01e3\u0001\u0000"+
		"\u0000\u0000\u01e3\u01e6\u0001\u0000\u0000\u0000\u01e4\u01e2\u0001\u0000"+
		"\u0000\u0000\u01e5\u01e7\u0003H$\u0000\u01e6\u01e5\u0001\u0000\u0000\u0000"+
		"\u01e6\u01e7\u0001\u0000\u0000\u0000\u01e7\u01e8\u0001\u0000\u0000\u0000"+
		"\u01e8\u01e9\u0005\u0012\u0000\u0000\u01e9\u01ea\u0005\u0018\u0000\u0000"+
		"\u01eaE\u0001\u0000\u0000\u0000\u01eb\u01ec\u0005\u0017\u0000\u0000\u01ec"+
		"\u01ed\u0003j5\u0000\u01ed\u01ee\u0005\u0015\u0000\u0000\u01ee\u01ef\u0003"+
		"8\u001c\u0000\u01efG\u0001\u0000\u0000\u0000\u01f0\u01f1\u0005\u0016\u0000"+
		"\u0000\u01f1\u01f2\u00038\u001c\u0000\u01f2I\u0001\u0000\u0000\u0000\u01f3"+
		"\u01f4\u0005\u001b\u0000\u0000\u01f4\u01f8\u0003j5\u0000\u01f5\u01f6\u0005"+
		"\u001a\u0000\u0000\u01f6\u01f8\u0003L&\u0000\u01f7\u01f3\u0001\u0000\u0000"+
		"\u0000\u01f7\u01f5\u0001\u0000\u0000\u0000\u01f7\u01f8\u0001\u0000\u0000"+
		"\u0000\u01f8\u01f9\u0001\u0000\u0000\u0000\u01f9\u01fa\u0005\u0019\u0000"+
		"\u0000\u01fa\u01fb\u00038\u001c\u0000\u01fb\u01fc\u0005\u0012\u0000\u0000"+
		"\u01fc\u01fd\u0005\u0019\u0000\u0000\u01fdK\u0001\u0000\u0000\u0000\u01fe"+
		"\u01ff\u0003\u008eG\u0000\u01ff\u0200\u00054\u0000\u0000\u0200\u0201\u0003"+
		"P(\u0000\u0201\u0202\u0005N\u0000\u0000\u0202\u0203\u0003R)\u0000\u0203"+
		"\u020b\u0001\u0000\u0000\u0000\u0204\u0205\u0003\u0090H\u0000\u0205\u0206"+
		"\u00054\u0000\u0000\u0206\u0207\u0005H\u0000\u0000\u0207\u0208\u0003N"+
		"\'\u0000\u0208\u0209\u0005I\u0000\u0000\u0209\u020b\u0001\u0000\u0000"+
		"\u0000\u020a\u01fe\u0001\u0000\u0000\u0000\u020a\u0204\u0001\u0000\u0000"+
		"\u0000\u020bM\u0001\u0000\u0000\u0000\u020c\u0210\u0005\u0005\u0000\u0000"+
		"\u020d\u020f\t\u0000\u0000\u0000\u020e\u020d\u0001\u0000\u0000\u0000\u020f"+
		"\u0212\u0001\u0000\u0000\u0000\u0210\u0211\u0001\u0000\u0000\u0000\u0210"+
		"\u020e\u0001\u0000\u0000\u0000\u0211\u0213\u0001\u0000\u0000\u0000\u0212"+
		"\u0210\u0001\u0000\u0000\u0000\u0213\u0214\u0005K\u0000\u0000\u0214O\u0001"+
		"\u0000\u0000\u0000\u0215\u0216\u0003\u0080@\u0000\u0216Q\u0001\u0000\u0000"+
		"\u0000\u0217\u0218\u0003\u0080@\u0000\u0218S\u0001\u0000\u0000\u0000\u0219"+
		"\u021b\u0005\u001e\u0000\u0000\u021a\u021c\u0003\u008cF\u0000\u021b\u021a"+
		"\u0001\u0000\u0000\u0000\u021b\u021c\u0001\u0000\u0000\u0000\u021cU\u0001"+
		"\u0000\u0000\u0000\u021d\u021f\u0005\u0010\u0000\u0000\u021e\u0220\u0003"+
		"n7\u0000\u021f\u021e\u0001\u0000\u0000\u0000\u021f\u0220\u0001\u0000\u0000"+
		"\u0000\u0220W\u0001\u0000\u0000\u0000\u0221\u0223\u0005!\u0000\u0000\u0222"+
		"\u0221\u0001\u0000\u0000\u0000\u0222\u0223\u0001\u0000\u0000\u0000\u0223"+
		"\u0224\u0001\u0000\u0000\u0000\u0224\u0226\u0003\u0086C\u0000\u0225\u0227"+
		"\u0003\u0094J\u0000\u0226\u0225\u0001\u0000\u0000\u0000\u0226\u0227\u0001"+
		"\u0000\u0000\u0000\u0227Y\u0001\u0000\u0000\u0000\u0228\u0229\u0005\u0011"+
		"\u0000\u0000\u0229\u0230\u00038\u001c\u0000\u022a\u022c\u0005\u0013\u0000"+
		"\u0000\u022b\u022d\u0003\\.\u0000\u022c\u022b\u0001\u0000\u0000\u0000"+
		"\u022d\u022e\u0001\u0000\u0000\u0000\u022e\u022c\u0001\u0000\u0000\u0000"+
		"\u022e\u022f\u0001\u0000\u0000\u0000\u022f\u0231\u0001\u0000\u0000\u0000"+
		"\u0230\u022a\u0001\u0000\u0000\u0000\u0230\u0231\u0001\u0000\u0000\u0000"+
		"\u0231\u0232\u0001\u0000\u0000\u0000\u0232\u0233\u0005\u0012\u0000\u0000"+
		"\u0233[\u0001\u0000\u0000\u0000\u0234\u0235\u0005\u0014\u0000\u0000\u0235"+
		"\u023a\u0003\u008cF\u0000\u0236\u0237\u0005)\u0000\u0000\u0237\u0239\u0003"+
		"\u008cF\u0000\u0238\u0236\u0001\u0000\u0000\u0000\u0239\u023c\u0001\u0000"+
		"\u0000\u0000\u023a\u0238\u0001\u0000\u0000\u0000\u023a\u023b\u0001\u0000"+
		"\u0000\u0000\u023b\u023d\u0001\u0000\u0000\u0000\u023c\u023a\u0001\u0000"+
		"\u0000\u0000\u023d\u023e\u0005\u0015\u0000\u0000\u023e\u023f\u00038\u001c"+
		"\u0000\u023f]\u0001\u0000\u0000\u0000\u0240\u0241\u0003`0\u0000\u0241"+
		"_\u0001\u0000\u0000\u0000\u0242\u024a\u0003h4\u0000\u0243\u024a\u0003"+
		"N\'\u0000\u0244\u024a\u0003b1\u0000\u0245\u024a\u0003d2\u0000\u0246\u024a"+
		"\u0003f3\u0000\u0247\u024a\u0003\u000e\u0007\u0000\u0248\u024a\u0003 "+
		"\u0010\u0000\u0249\u0242\u0001\u0000\u0000\u0000\u0249\u0243\u0001\u0000"+
		"\u0000\u0000\u0249\u0244\u0001\u0000\u0000\u0000\u0249\u0245\u0001\u0000"+
		"\u0000\u0000\u0249\u0246\u0001\u0000\u0000\u0000\u0249\u0247\u0001\u0000"+
		"\u0000\u0000\u0249\u0248\u0001\u0000\u0000\u0000\u024aa\u0001\u0000\u0000"+
		"\u0000\u024b\u024f\u0005\u0007\u0000\u0000\u024c\u024e\t\u0000\u0000\u0000"+
		"\u024d\u024c\u0001\u0000\u0000\u0000\u024e\u0251\u0001\u0000\u0000\u0000"+
		"\u024f\u0250\u0001\u0000\u0000\u0000\u024f\u024d\u0001\u0000\u0000\u0000"+
		"\u0250\u0252\u0001\u0000\u0000\u0000\u0251\u024f\u0001\u0000\u0000\u0000"+
		"\u0252\u0253\u0005K\u0000\u0000\u0253c\u0001\u0000\u0000\u0000\u0254\u0255"+
		"\u0005\b\u0000\u0000\u0255\u0259\u0005\u000b\u0000\u0000\u0256\u0258\t"+
		"\u0000\u0000\u0000\u0257\u0256\u0001\u0000\u0000\u0000\u0258\u025b\u0001"+
		"\u0000\u0000\u0000\u0259\u025a\u0001\u0000\u0000\u0000\u0259\u0257\u0001"+
		"\u0000\u0000\u0000\u025a\u025c\u0001\u0000\u0000\u0000\u025b\u0259\u0001"+
		"\u0000\u0000\u0000\u025c\u025d\u0005K\u0000\u0000\u025de\u0001\u0000\u0000"+
		"\u0000\u025e\u0262\u0005\u0006\u0000\u0000\u025f\u0261\t\u0000\u0000\u0000"+
		"\u0260\u025f\u0001\u0000\u0000\u0000\u0261\u0264\u0001\u0000\u0000\u0000"+
		"\u0262\u0263\u0001\u0000\u0000\u0000\u0262\u0260\u0001\u0000\u0000\u0000"+
		"\u0263\u0265\u0001\u0000\u0000\u0000\u0264\u0262\u0001\u0000\u0000\u0000"+
		"\u0265\u0266\u0005K\u0000\u0000\u0266g\u0001\u0000\u0000\u0000\u0267\u0268"+
		"\u0005\t\u0000\u0000\u0268\u026c\u0005\n\u0000\u0000\u0269\u026b\t\u0000"+
		"\u0000\u0000\u026a\u0269\u0001\u0000\u0000\u0000\u026b\u026e\u0001\u0000"+
		"\u0000\u0000\u026c\u026d\u0001\u0000\u0000\u0000\u026c\u026a\u0001\u0000"+
		"\u0000\u0000\u026d\u026f\u0001\u0000\u0000\u0000\u026e\u026c\u0001\u0000"+
		"\u0000\u0000\u026f\u0270\u0005K\u0000\u0000\u0270i\u0001\u0000\u0000\u0000"+
		"\u0271\u0272\u0003n7\u0000\u0272k\u0001\u0000\u0000\u0000\u0273\u0278"+
		"\u0003n7\u0000\u0274\u0275\u0005J\u0000\u0000\u0275\u0277\u0003n7\u0000"+
		"\u0276\u0274\u0001\u0000\u0000\u0000\u0277\u027a\u0001\u0000\u0000\u0000"+
		"\u0278\u0276\u0001\u0000\u0000\u0000\u0278\u0279\u0001\u0000\u0000\u0000"+
		"\u0279m\u0001\u0000\u0000\u0000\u027a\u0278\u0001\u0000\u0000\u0000\u027b"+
		"\u027c\u0003p8\u0000\u027co\u0001\u0000\u0000\u0000\u027d\u027e\u0006"+
		"8\uffff\uffff\u0000\u027e\u027f\u0003r9\u0000\u027f\u0288\u0001\u0000"+
		"\u0000\u0000\u0280\u0281\n\u0002\u0000\u0000\u0281\u0282\u0005(\u0000"+
		"\u0000\u0282\u0287\u0003p8\u0003\u0283\u0284\n\u0001\u0000\u0000\u0284"+
		"\u0285\u0005)\u0000\u0000\u0285\u0287\u0003p8\u0002\u0286\u0280\u0001"+
		"\u0000\u0000\u0000\u0286\u0283\u0001\u0000\u0000\u0000\u0287\u028a\u0001"+
		"\u0000\u0000\u0000\u0288\u0286\u0001\u0000\u0000\u0000\u0288\u0289\u0001"+
		"\u0000\u0000\u0000\u0289q\u0001\u0000\u0000\u0000\u028a\u0288\u0001\u0000"+
		"\u0000\u0000\u028b\u028d\u0005*\u0000\u0000\u028c\u028b\u0001\u0000\u0000"+
		"\u0000\u028c\u028d\u0001\u0000\u0000\u0000\u028d\u028e\u0001\u0000\u0000"+
		"\u0000\u028e\u0294\u0003t:\u0000\u028f\u0291\u0005#\u0000\u0000\u0290"+
		"\u0292\u0005*\u0000\u0000\u0291\u0290\u0001\u0000\u0000\u0000\u0291\u0292"+
		"\u0001\u0000\u0000\u0000\u0292\u0293\u0001\u0000\u0000\u0000\u0293\u0295"+
		"\u0005+\u0000\u0000\u0294\u028f\u0001\u0000\u0000\u0000\u0294\u0295\u0001"+
		"\u0000\u0000\u0000\u0295s\u0001\u0000\u0000\u0000\u0296\u0297\u0003v;"+
		"\u0000\u0297u\u0001\u0000\u0000\u0000\u0298\u0299\u0006;\uffff\uffff\u0000"+
		"\u0299\u029a\u0003x<\u0000\u029a\u02a1\u0001\u0000\u0000\u0000\u029b\u029c"+
		"\n\u0002\u0000\u0000\u029c\u029d\u0003z=\u0000\u029d\u029e\u0003v;\u0003"+
		"\u029e\u02a0\u0001\u0000\u0000\u0000\u029f\u029b\u0001\u0000\u0000\u0000"+
		"\u02a0\u02a3\u0001\u0000\u0000\u0000\u02a1\u029f\u0001\u0000\u0000\u0000"+
		"\u02a1\u02a2\u0001\u0000\u0000\u0000\u02a2w\u0001\u0000\u0000\u0000\u02a3"+
		"\u02a1\u0001\u0000\u0000\u0000\u02a4\u02b4\u0003\u0080@\u0000\u02a5\u02a7"+
		"\u0005*\u0000\u0000\u02a6\u02a5\u0001\u0000\u0000\u0000\u02a6\u02a7\u0001"+
		"\u0000\u0000\u0000\u02a7\u02b2\u0001\u0000\u0000\u0000\u02a8\u02a9\u0005"+
		"4\u0000\u0000\u02a9\u02b3\u0003|>\u0000\u02aa\u02ab\u0005.\u0000\u0000"+
		"\u02ab\u02b3\u0003~?\u0000\u02ac\u02ad\u0007\u0003\u0000\u0000\u02ad\u02b0"+
		"\u0003\u0080@\u0000\u02ae\u02af\u00053\u0000\u0000\u02af\u02b1\u0003\u0080"+
		"@\u0000\u02b0\u02ae\u0001\u0000\u0000\u0000\u02b0\u02b1\u0001\u0000\u0000"+
		"\u0000\u02b1\u02b3\u0001\u0000\u0000\u0000\u02b2\u02a8\u0001\u0000\u0000"+
		"\u0000\u02b2\u02aa\u0001\u0000\u0000\u0000\u02b2\u02ac\u0001\u0000\u0000"+
		"\u0000\u02b3\u02b5\u0001\u0000\u0000\u0000\u02b4\u02a6\u0001\u0000\u0000"+
		"\u0000\u02b4\u02b5\u0001\u0000\u0000\u0000\u02b5y\u0001\u0000\u0000\u0000"+
		"\u02b6\u02c3\u0005P\u0000\u0000\u02b7\u02c3\u0005Q\u0000\u0000\u02b8\u02b9"+
		"\u0005W\u0000\u0000\u02b9\u02c3\u0005X\u0000\u0000\u02ba\u02bb\u0005Y"+
		"\u0000\u0000\u02bb\u02c3\u0005P\u0000\u0000\u02bc\u02bd\u0005Z\u0000\u0000"+
		"\u02bd\u02c3\u0005P\u0000\u0000\u02be\u02c0\u0007\u0004\u0000\u0000\u02bf"+
		"\u02c1\u0005P\u0000\u0000\u02c0\u02bf\u0001\u0000\u0000\u0000\u02c0\u02c1"+
		"\u0001\u0000\u0000\u0000\u02c1\u02c3\u0001\u0000\u0000\u0000\u02c2\u02b6"+
		"\u0001\u0000\u0000\u0000\u02c2\u02b7\u0001\u0000\u0000\u0000\u02c2\u02b8"+
		"\u0001\u0000\u0000\u0000\u02c2\u02ba\u0001\u0000\u0000\u0000\u02c2\u02bc"+
		"\u0001\u0000\u0000\u0000\u02c2\u02be\u0001\u0000\u0000\u0000\u02c3{\u0001"+
		"\u0000\u0000\u0000\u02c4\u02c5\u0005H\u0000\u0000\u02c5\u02ca\u0003\u0080"+
		"@\u0000\u02c6\u02c7\u0005J\u0000\u0000\u02c7\u02c9\u0003\u0080@\u0000"+
		"\u02c8\u02c6\u0001\u0000\u0000\u0000\u02c9\u02cc\u0001\u0000\u0000\u0000"+
		"\u02ca\u02c8\u0001\u0000\u0000\u0000\u02ca\u02cb\u0001\u0000\u0000\u0000"+
		"\u02cb\u02cd\u0001\u0000\u0000\u0000\u02cc\u02ca\u0001\u0000\u0000\u0000"+
		"\u02cd\u02ce\u0005I\u0000\u0000\u02ce\u02d3\u0001\u0000\u0000\u0000\u02cf"+
		"\u02d3\u0003\u00a4R\u0000\u02d0\u02d3\u0003\u00a0P\u0000\u02d1\u02d3\u0003"+
		"\u00a2Q\u0000\u02d2\u02c4\u0001\u0000\u0000\u0000\u02d2\u02cf\u0001\u0000"+
		"\u0000\u0000\u02d2\u02d0\u0001\u0000\u0000\u0000\u02d2\u02d1\u0001\u0000"+
		"\u0000\u0000\u02d3}\u0001\u0000\u0000\u0000\u02d4\u02d5\u0003\u0080@\u0000"+
		"\u02d5\u02d6\u0005(\u0000\u0000\u02d6\u02d7\u0003\u0080@\u0000\u02d7\u007f"+
		"\u0001\u0000\u0000\u0000\u02d8\u02d9\u0006@\uffff\uffff\u0000\u02d9\u02da"+
		"\u0003\u0084B\u0000\u02da\u02f0\u0001\u0000\u0000\u0000\u02db\u02dc\n"+
		"\u0007\u0000\u0000\u02dc\u02dd\u0005T\u0000\u0000\u02dd\u02ef\u0003\u0080"+
		"@\b\u02de\u02df\n\u0006\u0000\u0000\u02df\u02e0\u0005U\u0000\u0000\u02e0"+
		"\u02ef\u0003\u0080@\u0007\u02e1\u02e2\n\u0005\u0000\u0000\u02e2\u02e3"+
		"\u0005R\u0000\u0000\u02e3\u02ef\u0003\u0080@\u0006\u02e4\u02e5\n\u0004"+
		"\u0000\u0000\u02e5\u02e6\u0005S\u0000\u0000\u02e6\u02ef\u0003\u0080@\u0005"+
		"\u02e7\u02e8\n\u0003\u0000\u0000\u02e8\u02e9\u0005V\u0000\u0000\u02e9"+
		"\u02ef\u0003\u0080@\u0004\u02ea\u02eb\n\u0002\u0000\u0000\u02eb\u02ec"+
		"\u0005V\u0000\u0000\u02ec\u02ed\u0005V\u0000\u0000\u02ed\u02ef\u0003\u0080"+
		"@\u0003\u02ee\u02db\u0001\u0000\u0000\u0000\u02ee\u02de\u0001\u0000\u0000"+
		"\u0000\u02ee\u02e1\u0001\u0000\u0000\u0000\u02ee\u02e4\u0001\u0000\u0000"+
		"\u0000\u02ee\u02e7\u0001\u0000\u0000\u0000\u02ee\u02ea\u0001\u0000\u0000"+
		"\u0000\u02ef\u02f2\u0001\u0000\u0000\u0000\u02f0\u02ee\u0001\u0000\u0000"+
		"\u0000\u02f0\u02f1\u0001\u0000\u0000\u0000\u02f1\u0081\u0001\u0000\u0000"+
		"\u0000\u02f2\u02f0\u0001\u0000\u0000\u0000\u02f3\u02f4\u0007\u0005\u0000"+
		"\u0000\u02f4\u02f7\u0003\u0082A\u0000\u02f5\u02f7\u0003\u0084B\u0000\u02f6"+
		"\u02f3\u0001\u0000\u0000\u0000\u02f6\u02f5\u0001\u0000\u0000\u0000\u02f7"+
		"\u0083\u0001\u0000\u0000\u0000\u02f8\u0301\u0003\u00a0P\u0000\u02f9\u0301"+
		"\u0003\u00a4R\u0000\u02fa\u0301\u0003\u00a2Q\u0000\u02fb\u02fc\u0005H"+
		"\u0000\u0000\u02fc\u02fd\u0003l6\u0000\u02fd\u02fe\u0005I\u0000\u0000"+
		"\u02fe\u0301\u0001\u0000\u0000\u0000\u02ff\u0301\u0003\u00a8T\u0000\u0300"+
		"\u02f8\u0001\u0000\u0000\u0000\u0300\u02f9\u0001\u0000\u0000\u0000\u0300"+
		"\u02fa\u0001\u0000\u0000\u0000\u0300\u02fb\u0001\u0000\u0000\u0000\u0300"+
		"\u02ff\u0001\u0000\u0000\u0000\u0301\u0085\u0001\u0000\u0000\u0000\u0302"+
		"\u0303\u0003\u00aaU\u0000\u0303\u0087\u0001\u0000\u0000\u0000\u0304\u0305"+
		"\u0003\u00aaU\u0000\u0305\u0089\u0001\u0000\u0000\u0000\u0306\u0307\u0003"+
		"\u00aaU\u0000\u0307\u008b\u0001\u0000\u0000\u0000\u0308\u0309\u0003\u00aa"+
		"U\u0000\u0309\u008d\u0001\u0000\u0000\u0000\u030a\u030b\u0003\u00aaU\u0000"+
		"\u030b\u008f\u0001\u0000\u0000\u0000\u030c\u030d\u0003\u00aaU\u0000\u030d"+
		"\u0091\u0001\u0000\u0000\u0000\u030e\u030f\u0003\u00aaU\u0000\u030f\u0093"+
		"\u0001\u0000\u0000\u0000\u0310\u0319\u0005H\u0000\u0000\u0311\u0316\u0003"+
		"\u0096K\u0000\u0312\u0313\u0005J\u0000\u0000\u0313\u0315\u0003\u0096K"+
		"\u0000\u0314\u0312\u0001\u0000\u0000\u0000\u0315\u0318\u0001\u0000\u0000"+
		"\u0000\u0316\u0314\u0001\u0000\u0000\u0000\u0316\u0317\u0001\u0000\u0000"+
		"\u0000\u0317\u031a\u0001\u0000\u0000\u0000\u0318\u0316\u0001\u0000\u0000"+
		"\u0000\u0319\u0311\u0001\u0000\u0000\u0000\u0319\u031a\u0001\u0000\u0000"+
		"\u0000\u031a\u031b\u0001\u0000\u0000\u0000\u031b\u031c\u0005I\u0000\u0000"+
		"\u031c\u0095\u0001\u0000\u0000\u0000\u031d\u031e\u0003n7\u0000\u031e\u0097"+
		"\u0001\u0000\u0000\u0000\u031f\u0320\u0003\u009aM\u0000\u0320\u0099\u0001"+
		"\u0000\u0000\u0000\u0321\u0323\u0003\u009eO\u0000\u0322\u0324\u0003\u009c"+
		"N\u0000\u0323\u0322\u0001\u0000\u0000\u0000\u0323\u0324\u0001\u0000\u0000"+
		"\u0000\u0324\u009b\u0001\u0000\u0000\u0000\u0325\u0326\u0005H\u0000\u0000"+
		"\u0326\u0329\u0003\u00a6S\u0000\u0327\u0328\u0005J\u0000\u0000\u0328\u032a"+
		"\u0003\u00a6S\u0000\u0329\u0327\u0001\u0000\u0000\u0000\u0329\u032a\u0001"+
		"\u0000\u0000\u0000\u032a\u032b\u0001\u0000\u0000\u0000\u032b\u032c\u0005"+
		"I\u0000\u0000\u032c\u009d\u0001\u0000\u0000\u0000\u032d\u032e\u0007\u0006"+
		"\u0000\u0000\u032e\u009f\u0001\u0000\u0000\u0000\u032f\u0330\u0005`\u0000"+
		"\u0000\u0330\u00a1\u0001\u0000\u0000\u0000\u0331\u0336\u0003\u00acV\u0000"+
		"\u0332\u0333\u0005M\u0000\u0000\u0333\u0335\u0003\u00acV\u0000\u0334\u0332"+
		"\u0001\u0000\u0000\u0000\u0335\u0338\u0001\u0000\u0000\u0000\u0336\u0334"+
		"\u0001\u0000\u0000\u0000\u0336\u0337\u0001\u0000\u0000\u0000\u0337\u00a3"+
		"\u0001\u0000\u0000\u0000\u0338\u0336\u0001\u0000\u0000\u0000\u0339\u033c"+
		"\u0005E\u0000\u0000\u033a\u033d\u0003\u00a8T\u0000\u033b\u033d\u0003\u00a0"+
		"P\u0000\u033c\u033a\u0001\u0000\u0000\u0000\u033c\u033b\u0001\u0000\u0000"+
		"\u0000\u033d\u0347\u0001\u0000\u0000\u0000\u033e\u0347\u0003\u00a6S\u0000"+
		"\u033f\u0340\u0005D\u0000\u0000\u0340\u0347\u0003\u00a8T\u0000\u0341\u0347"+
		"\u0003\u00a8T\u0000\u0342\u0347\u0005+\u0000\u0000\u0343\u0347\u0005,"+
		"\u0000\u0000\u0344\u0347\u0005-\u0000\u0000\u0345\u0347\u00057\u0000\u0000"+
		"\u0346\u0339\u0001\u0000\u0000\u0000\u0346\u033e\u0001\u0000\u0000\u0000"+
		"\u0346\u033f\u0001\u0000\u0000\u0000\u0346\u0341\u0001\u0000\u0000\u0000"+
		"\u0346\u0342\u0001\u0000\u0000\u0000\u0346\u0343\u0001\u0000\u0000\u0000"+
		"\u0346\u0344\u0001\u0000\u0000\u0000\u0346\u0345\u0001\u0000\u0000\u0000"+
		"\u0347\u00a5\u0001\u0000\u0000\u0000\u0348\u0349\u0007\u0007\u0000\u0000"+
		"\u0349\u00a7\u0001\u0000\u0000\u0000\u034a\u034b\u0007\b\u0000\u0000\u034b"+
		"\u00a9\u0001\u0000\u0000\u0000\u034c\u034d\u0003\u00acV\u0000\u034d\u00ab"+
		"\u0001\u0000\u0000\u0000\u034e\u0351\u0003\u00aeW\u0000\u034f\u0351\u0005"+
		"_\u0000\u0000\u0350\u034e\u0001\u0000\u0000\u0000\u0350\u034f\u0001\u0000"+
		"\u0000\u0000\u0351\u00ad\u0001\u0000\u0000\u0000\u0352\u0353\u0007\t\u0000"+
		"\u0000\u0353\u00af\u0001\u0000\u0000\u0000O\u00b2\u00b6\u00c8\u00d2\u00d7"+
		"\u00dd\u00e0\u00ef\u00f4\u00f8\u00fb\u0105\u0108\u0115\u0122\u0136\u0145"+
		"\u014a\u0156\u015f\u0168\u0172\u017b\u0185\u018e\u0197\u019b\u019e\u01a6"+
		"\u01ac\u01b1\u01b4\u01bd\u01c9\u01d1\u01e2\u01e6\u01f7\u020a\u0210\u021b"+
		"\u021f\u0222\u0226\u022e\u0230\u023a\u0249\u024f\u0259\u0262\u026c\u0278"+
		"\u0286\u0288\u028c\u0291\u0294\u02a1\u02a6\u02b0\u02b2\u02b4\u02c0\u02c2"+
		"\u02ca\u02d2\u02ee\u02f0\u02f6\u0300\u0316\u0319\u0323\u0329\u0336\u033c"+
		"\u0346\u0350";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}
