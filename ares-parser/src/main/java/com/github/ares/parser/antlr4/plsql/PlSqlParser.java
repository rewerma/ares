// Generated from /Users/rewerma/Develop/git_workspace/ares/ares-parser/src/main/java/com/github/ares/parser/antlr4/plsql/PlSqlParser.g4 by ANTLR 4.13.1
package com.github.ares.parser.antlr4.plsql;

import com.github.ares.org.antlr.v4.runtime.FailedPredicateException;
import com.github.ares.org.antlr.v4.runtime.NoViableAltException;
import com.github.ares.org.antlr.v4.runtime.Parser;
import com.github.ares.org.antlr.v4.runtime.ParserRuleContext;
import com.github.ares.org.antlr.v4.runtime.RecognitionException;
import com.github.ares.org.antlr.v4.runtime.RuleContext;
import com.github.ares.org.antlr.v4.runtime.RuntimeMetaData;
import com.github.ares.org.antlr.v4.runtime.Token;
import com.github.ares.org.antlr.v4.runtime.TokenStream;
import com.github.ares.org.antlr.v4.runtime.Vocabulary;
import com.github.ares.org.antlr.v4.runtime.VocabularyImpl;
import com.github.ares.org.antlr.v4.runtime.atn.ATN;
import com.github.ares.org.antlr.v4.runtime.atn.ATNDeserializer;
import com.github.ares.org.antlr.v4.runtime.atn.ParserATNSimulator;
import com.github.ares.org.antlr.v4.runtime.atn.PredictionContextCache;
import com.github.ares.org.antlr.v4.runtime.dfa.DFA;
import com.github.ares.org.antlr.v4.runtime.tree.ParseTreeListener;
import com.github.ares.org.antlr.v4.runtime.tree.ParseTreeVisitor;
import com.github.ares.org.antlr.v4.runtime.tree.TerminalNode;

import java.util.List;

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
		DO=31, DECLARE=32, CALL=33, AS=34, IS=35, AND=36, OR=37, NOT=38, NULL_=39, 
		TRUE=40, FALSE=41, BETWEEN=42, LIKE=43, LIKEC=44, LIKE2=45, LIKE4=46, 
		ESCAPE=47, IN=48, OUT=49, CONSTANT=50, DEFAULT=51, INT=52, BYTE=53, SMALLINT=54, 
		BIGINT=55, NUMBER=56, DECIMAL=57, DOUBLE=58, FLOAT=59, VARCHAR=60, VARCHAR2=61, 
		STRING=62, BOOLEAN=63, DATE=64, TIMESTAMP=65, BINARY=66, BLOB=67, LEFT_PAREN=68, 
		RIGHT_PAREN=69, COMMA=70, SEMICOLON=71, COLON=72, PERIOD=73, DOUBLE_PERIOD=74, 
		ASSIGN_OP=75, EQUALS_OP=76, NOT_EQUAL_OP=77, PLUS_SIGN=78, MINUS_SIGN=79, 
		ASTERISK=80, SOLIDUS=81, BAR=82, LESS_THAN_OP=83, GREATER_THAN_OP=84, 
		EXCLAMATION_OPERATOR_PART=85, CARRET_OPERATOR_PART=86, CHAR_STRING=87, 
		NATIONAL_CHAR_STRING_LIT=88, UNSIGNED_INTEGER=89, APPROXIMATE_NUM_LIT=90, 
		DELIMITED_ID=91, BINDVAR=92, REGULAR_ID=93, SINGLE_LINE_COMMENT=94, MULTI_LINE_COMMENT=95, 
		SPACES=96;
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
		RULE_seq_of_statements = 28, RULE_statement = 29, RULE_assignment_statement = 30, 
		RULE_continue_statement = 31, RULE_exit_statement = 32, RULE_if_statement = 33, 
		RULE_elsif_part = 34, RULE_else_part = 35, RULE_loop_statement = 36, RULE_cursor_loop_param = 37, 
		RULE_select_statement = 38, RULE_lower_bound = 39, RULE_upper_bound = 40, 
		RULE_raise_statement = 41, RULE_return_statement = 42, RULE_call_statement = 43, 
		RULE_body = 44, RULE_exception_handler = 45, RULE_sql_statement = 46, 
		RULE_data_manipulation_language_statements = 47, RULE_update_statement = 48, 
		RULE_delete_statement = 49, RULE_insert_statement = 50, RULE_merge_statement = 51, 
		RULE_condition = 52, RULE_expressions = 53, RULE_expression = 54, RULE_logical_expression = 55, 
		RULE_unary_logical_expression = 56, RULE_multiset_expression = 57, RULE_relational_expression = 58, 
		RULE_compound_expression = 59, RULE_relational_operator = 60, RULE_in_elements = 61, 
		RULE_between_elements = 62, RULE_concatenation = 63, RULE_unary_expression = 64, 
		RULE_atom = 65, RULE_routine_name = 66, RULE_parameter_name = 67, RULE_procedure_name = 68, 
		RULE_exception_name = 69, RULE_index_name = 70, RULE_record_name = 71, 
		RULE_column_name = 72, RULE_function_argument = 73, RULE_argument = 74, 
		RULE_type_spec = 75, RULE_datatype = 76, RULE_precision_part = 77, RULE_native_datatype_element = 78, 
		RULE_bind_variable = 79, RULE_general_element = 80, RULE_constant = 81, 
		RULE_numeric = 82, RULE_quoted_string = 83, RULE_identifier = 84, RULE_id_expression = 85, 
		RULE_regular_id = 86;
	private static String[] makeRuleNames() {
		return new String[] {
			"sql_script", "unit_statement", "function_body", "create_procedure_body", 
			"create_function_body", "anonymous_body", "create_table_as", "create_table_as2", 
			"create_table", "create_with", "create_options", "option_", "table_name", 
			"relational_table", "relational_property", "column_definition", "truncate_table_block", 
			"select_block", "update_block", "delete_block", "insert_block", "merge_block", 
			"set_bleck", "parameter", "default_value_part", "seq_of_declare_specs", 
			"declare_spec", "variable_declaration", "seq_of_statements", "statement", 
			"assignment_statement", "continue_statement", "exit_statement", "if_statement", 
			"elsif_part", "else_part", "loop_statement", "cursor_loop_param", "select_statement", 
			"lower_bound", "upper_bound", "raise_statement", "return_statement", 
			"call_statement", "body", "exception_handler", "sql_statement", "data_manipulation_language_statements", 
			"update_statement", "delete_statement", "insert_statement", "merge_statement", 
			"condition", "expressions", "expression", "logical_expression", "unary_logical_expression", 
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
			"'IS'", "'AND'", "'OR'", "'NOT'", "'NULL'", "'TRUE'", "'FALSE'", "'BETWEEN'", 
			"'LIKE'", "'LIKEC'", "'LIKE2'", "'LIKE4'", "'ESCAPE'", "'IN'", "'OUT'", 
			"'CONSTANT'", "'DEFAULT'", "'INT'", "'BYTE'", "'SMALLINT'", "'BIGINT'", 
			"'NUMBER'", "'DECIMAL'", "'DOUBLE'", "'FLOAT'", "'VARCHAR'", "'VARCHAR2'", 
			"'STRING'", "'BOOLEAN'", "'DATE'", "'TIMESTAMP'", "'BINARY'", "'BLOB'", 
			"'('", "')'", "','", "';'", "':'", "'.'", "'..'", "':='", "'='", null, 
			"'+'", "'-'", "'*'", "'/'", "'|'", "'<'", "'>'", "'!'", "'^'"
		};
	}
	private static final String[] _LITERAL_NAMES = makeLiteralNames();
	private static String[] makeSymbolicNames() {
		return new String[] {
			null, "CREATE", "TABLE", "WITH", "SET", "SELECT", "INSERT", "UPDATE", 
			"DELETE", "MERGE", "INTO", "FROM", "WHERE", "TRUNCATE", "PROCEDURE", 
			"FUNCTION", "RETURN", "BEGIN", "END", "EXCEPTION", "WHEN", "THEN", "ELSE", 
			"ELSIF", "IF", "LOOP", "FOR", "WHILE", "EXIT", "CONTINUE", "RAISE", "DO", 
			"DECLARE", "CALL", "AS", "IS", "AND", "OR", "NOT", "NULL_", "TRUE", "FALSE", 
			"BETWEEN", "LIKE", "LIKEC", "LIKE2", "LIKE4", "ESCAPE", "IN", "OUT", 
			"CONSTANT", "DEFAULT", "INT", "BYTE", "SMALLINT", "BIGINT", "NUMBER", 
			"DECIMAL", "DOUBLE", "FLOAT", "VARCHAR", "VARCHAR2", "STRING", "BOOLEAN", 
			"DATE", "TIMESTAMP", "BINARY", "BLOB", "LEFT_PAREN", "RIGHT_PAREN", "COMMA", 
			"SEMICOLON", "COLON", "PERIOD", "DOUBLE_PERIOD", "ASSIGN_OP", "EQUALS_OP", 
			"NOT_EQUAL_OP", "PLUS_SIGN", "MINUS_SIGN", "ASTERISK", "SOLIDUS", "BAR", 
			"LESS_THAN_OP", "GREATER_THAN_OP", "EXCLAMATION_OPERATOR_PART", "CARRET_OPERATOR_PART", 
			"CHAR_STRING", "NATIONAL_CHAR_STRING_LIT", "UNSIGNED_INTEGER", "APPROXIMATE_NUM_LIT", 
			"DELIMITED_ID", "BINDVAR", "REGULAR_ID", "SINGLE_LINE_COMMENT", "MULTI_LINE_COMMENT", 
			"SPACES"
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
			setState(180);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while ((((_la) & ~0x3f) == 0 && ((1L << _la) & -280927368380418L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & 671088655L) != 0)) {
				{
				{
				{
				setState(174);
				unit_statement();
				}
				setState(176);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SEMICOLON) {
					{
					setState(175);
					match(SEMICOLON);
					}
				}

				}
				}
				setState(182);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(183);
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
			setState(198);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,2,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(185);
				anonymous_body();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(186);
				create_procedure_body();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(187);
				create_function_body();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(188);
				create_table();
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(189);
				create_table_as();
				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(190);
				set_bleck();
				}
				break;
			case 7:
				enterOuterAlt(_localctx, 7);
				{
				setState(191);
				select_block();
				}
				break;
			case 8:
				enterOuterAlt(_localctx, 8);
				{
				setState(192);
				insert_block();
				}
				break;
			case 9:
				enterOuterAlt(_localctx, 9);
				{
				setState(193);
				update_block();
				}
				break;
			case 10:
				enterOuterAlt(_localctx, 10);
				{
				setState(194);
				delete_block();
				}
				break;
			case 11:
				enterOuterAlt(_localctx, 11);
				{
				setState(195);
				merge_block();
				}
				break;
			case 12:
				enterOuterAlt(_localctx, 12);
				{
				setState(196);
				truncate_table_block();
				}
				break;
			case 13:
				enterOuterAlt(_localctx, 13);
				{
				setState(197);
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
			setState(200);
			match(FUNCTION);
			setState(201);
			identifier();
			setState(213);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==LEFT_PAREN) {
				{
				setState(202);
				match(LEFT_PAREN);
				setState(203);
				parameter();
				setState(208);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(204);
					match(COMMA);
					setState(205);
					parameter();
					}
					}
					setState(210);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(211);
				match(RIGHT_PAREN);
				}
			}

			setState(215);
			match(RETURN);
			setState(216);
			type_spec();
			setState(217);
			match(AS);
			{
			setState(219);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,5,_ctx) ) {
			case 1:
				{
				setState(218);
				match(DECLARE);
				}
				break;
			}
			setState(222);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,6,_ctx) ) {
			case 1:
				{
				setState(221);
				seq_of_declare_specs();
				}
				break;
			}
			setState(224);
			body();
			}
			setState(226);
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
			setState(228);
			match(CREATE);
			setState(229);
			match(PROCEDURE);
			setState(230);
			procedure_name();
			setState(242);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==LEFT_PAREN) {
				{
				setState(231);
				match(LEFT_PAREN);
				setState(232);
				parameter();
				setState(237);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(233);
					match(COMMA);
					setState(234);
					parameter();
					}
					}
					setState(239);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(240);
				match(RIGHT_PAREN);
				}
			}

			setState(244);
			match(AS);
			{
			setState(246);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,9,_ctx) ) {
			case 1:
				{
				setState(245);
				match(DECLARE);
				}
				break;
			}
			setState(249);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,10,_ctx) ) {
			case 1:
				{
				setState(248);
				seq_of_declare_specs();
				}
				break;
			}
			setState(251);
			body();
			}
			setState(253);
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
			setState(255);
			match(CREATE);
			setState(256);
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
			setState(259);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,11,_ctx) ) {
			case 1:
				{
				setState(258);
				match(DECLARE);
				}
				break;
			}
			setState(262);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,12,_ctx) ) {
			case 1:
				{
				setState(261);
				seq_of_declare_specs();
				}
				break;
			}
			setState(264);
			body();
			setState(265);
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
			setState(267);
			match(CREATE);
			setState(268);
			match(TABLE);
			setState(269);
			table_name();
			setState(270);
			match(AS);
			setState(271);
			match(SELECT);
			setState(275);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,13,_ctx);
			while ( _alt!=1 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1+1 ) {
					{
					{
					setState(272);
					matchWildcard();
					}
					} 
				}
				setState(277);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,13,_ctx);
			}
			setState(278);
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
			setState(280);
			match(CREATE);
			setState(281);
			match(TABLE);
			setState(282);
			table_name();
			setState(283);
			match(AS);
			setState(284);
			match(SELECT);
			setState(288);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,14,_ctx);
			while ( _alt!=1 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1+1 ) {
					{
					{
					setState(285);
					matchWildcard();
					}
					} 
				}
				setState(290);
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
			setState(291);
			match(CREATE);
			setState(292);
			match(TABLE);
			setState(293);
			table_name();
			{
			setState(294);
			relational_table();
			}
			{
			setState(295);
			create_with();
			}
			setState(296);
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
			setState(298);
			match(WITH);
			setState(299);
			match(LEFT_PAREN);
			setState(300);
			create_options();
			setState(301);
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
			setState(303);
			option_();
			setState(308);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(304);
				match(COMMA);
				setState(305);
				option_();
				}
				}
				setState(310);
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
			setState(311);
			match(CHAR_STRING);
			setState(312);
			match(EQUALS_OP);
			setState(313);
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
			setState(315);
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
			setState(328);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==LEFT_PAREN) {
				{
				setState(317);
				match(LEFT_PAREN);
				setState(318);
				relational_property();
				setState(323);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(319);
					match(COMMA);
					setState(320);
					relational_property();
					}
					}
					setState(325);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(326);
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
			setState(330);
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
			setState(332);
			column_name();
			setState(333);
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
			setState(335);
			match(TRUNCATE);
			setState(336);
			match(TABLE);
			setState(340);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,18,_ctx);
			while ( _alt!=1 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1+1 ) {
					{
					{
					setState(337);
					matchWildcard();
					}
					} 
				}
				setState(342);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,18,_ctx);
			}
			setState(343);
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
			setState(345);
			match(SELECT);
			setState(349);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,19,_ctx);
			while ( _alt!=1 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1+1 ) {
					{
					{
					setState(346);
					matchWildcard();
					}
					} 
				}
				setState(351);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,19,_ctx);
			}
			setState(352);
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
			setState(354);
			match(UPDATE);
			setState(358);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,20,_ctx);
			while ( _alt!=1 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1+1 ) {
					{
					{
					setState(355);
					matchWildcard();
					}
					} 
				}
				setState(360);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,20,_ctx);
			}
			setState(361);
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
			setState(363);
			match(DELETE);
			setState(364);
			match(FROM);
			setState(368);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,21,_ctx);
			while ( _alt!=1 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1+1 ) {
					{
					{
					setState(365);
					matchWildcard();
					}
					} 
				}
				setState(370);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,21,_ctx);
			}
			setState(371);
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
			setState(373);
			match(INSERT);
			setState(377);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,22,_ctx);
			while ( _alt!=1 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1+1 ) {
					{
					{
					setState(374);
					matchWildcard();
					}
					} 
				}
				setState(379);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,22,_ctx);
			}
			setState(380);
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
			setState(382);
			match(MERGE);
			setState(383);
			match(INTO);
			setState(387);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,23,_ctx);
			while ( _alt!=1 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1+1 ) {
					{
					{
					setState(384);
					matchWildcard();
					}
					} 
				}
				setState(389);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,23,_ctx);
			}
			setState(390);
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
			setState(392);
			match(SET);
			setState(396);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,24,_ctx);
			while ( _alt!=1 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1+1 ) {
					{
					{
					setState(393);
					matchWildcard();
					}
					} 
				}
				setState(398);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,24,_ctx);
			}
			setState(399);
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
			setState(401);
			parameter_name();
			setState(405);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==IN || _la==OUT) {
				{
				{
				setState(402);
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
				setState(407);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(409);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (((((_la - 52)) & ~0x3f) == 0 && ((1L << (_la - 52)) & 65023L) != 0)) {
				{
				setState(408);
				type_spec();
				}
			}

			setState(412);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==DEFAULT || _la==ASSIGN_OP) {
				{
				setState(411);
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
			setState(414);
			_la = _input.LA(1);
			if ( !(_la==DEFAULT || _la==ASSIGN_OP) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			setState(415);
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
			setState(418); 
			_errHandler.sync(this);
			_alt = 1;
			do {
				switch (_alt) {
				case 1:
					{
					{
					setState(417);
					declare_spec();
					}
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(420); 
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
			setState(422);
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
			setState(424);
			identifier();
			setState(426);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==CONSTANT) {
				{
				setState(425);
				match(CONSTANT);
				}
			}

			setState(428);
			type_spec();
			setState(431);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==NOT) {
				{
				setState(429);
				match(NOT);
				setState(430);
				match(NULL_);
				}
			}

			setState(434);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==DEFAULT || _la==ASSIGN_OP) {
				{
				setState(433);
				default_value_part();
				}
			}

			setState(436);
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
			setState(441); 
			_errHandler.sync(this);
			_alt = 1;
			do {
				switch (_alt) {
				case 1:
					{
					{
					setState(438);
					statement();
					setState(439);
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
				setState(443); 
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
			setState(454);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,33,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(445);
				assignment_statement();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(446);
				continue_statement();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(447);
				exit_statement();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(448);
				if_statement();
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(449);
				loop_statement();
				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(450);
				raise_statement();
				}
				break;
			case 7:
				enterOuterAlt(_localctx, 7);
				{
				setState(451);
				return_statement();
				}
				break;
			case 8:
				enterOuterAlt(_localctx, 8);
				{
				setState(452);
				sql_statement();
				}
				break;
			case 9:
				enterOuterAlt(_localctx, 9);
				{
				setState(453);
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
		enterRule(_localctx, 60, RULE_assignment_statement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(456);
			general_element();
			setState(457);
			match(ASSIGN_OP);
			setState(458);
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
		enterRule(_localctx, 62, RULE_continue_statement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(460);
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
		enterRule(_localctx, 64, RULE_exit_statement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(462);
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
		enterRule(_localctx, 66, RULE_if_statement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(464);
			match(IF);
			setState(465);
			condition();
			setState(466);
			match(THEN);
			setState(467);
			seq_of_statements();
			setState(471);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==ELSIF) {
				{
				{
				setState(468);
				elsif_part();
				}
				}
				setState(473);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(475);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ELSE) {
				{
				setState(474);
				else_part();
				}
			}

			setState(477);
			match(END);
			setState(478);
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
		enterRule(_localctx, 68, RULE_elsif_part);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(480);
			match(ELSIF);
			setState(481);
			condition();
			setState(482);
			match(THEN);
			setState(483);
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
		enterRule(_localctx, 70, RULE_else_part);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(485);
			match(ELSE);
			setState(486);
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
		enterRule(_localctx, 72, RULE_loop_statement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(492);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case WHILE:
				{
				setState(488);
				match(WHILE);
				setState(489);
				condition();
				}
				break;
			case FOR:
				{
				setState(490);
				match(FOR);
				setState(491);
				cursor_loop_param();
				}
				break;
			case LOOP:
				break;
			default:
				break;
			}
			setState(494);
			match(LOOP);
			setState(495);
			seq_of_statements();
			setState(496);
			match(END);
			setState(497);
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
		enterRule(_localctx, 74, RULE_cursor_loop_param);
		try {
			setState(511);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,37,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(499);
				index_name();
				setState(500);
				match(IN);
				setState(501);
				lower_bound();
				setState(502);
				match(DOUBLE_PERIOD);
				setState(503);
				upper_bound();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(505);
				record_name();
				setState(506);
				match(IN);
				setState(507);
				match(LEFT_PAREN);
				setState(508);
				select_statement();
				setState(509);
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
		enterRule(_localctx, 76, RULE_select_statement);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(513);
			match(SELECT);
			setState(517);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,38,_ctx);
			while ( _alt!=1 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1+1 ) {
					{
					{
					setState(514);
					matchWildcard();
					}
					} 
				}
				setState(519);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,38,_ctx);
			}
			setState(520);
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
		enterRule(_localctx, 78, RULE_lower_bound);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(522);
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
		enterRule(_localctx, 80, RULE_upper_bound);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(524);
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
		enterRule(_localctx, 82, RULE_raise_statement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(526);
			match(RAISE);
			setState(528);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & -280927368388610L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & 671088655L) != 0)) {
				{
				setState(527);
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
		enterRule(_localctx, 84, RULE_return_statement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(530);
			match(RETURN);
			setState(532);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & -277079077691394L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & 1065353247L) != 0)) {
				{
				setState(531);
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
		enterRule(_localctx, 86, RULE_call_statement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(535);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,41,_ctx) ) {
			case 1:
				{
				setState(534);
				match(CALL);
				}
				break;
			}
			setState(537);
			routine_name();
			setState(539);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==LEFT_PAREN) {
				{
				setState(538);
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
		enterRule(_localctx, 88, RULE_body);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(541);
			match(BEGIN);
			setState(542);
			seq_of_statements();
			setState(549);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==EXCEPTION) {
				{
				setState(543);
				match(EXCEPTION);
				setState(545); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(544);
					exception_handler();
					}
					}
					setState(547); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==WHEN );
				}
			}

			setState(551);
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
		enterRule(_localctx, 90, RULE_exception_handler);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(553);
			match(WHEN);
			setState(554);
			exception_name();
			setState(559);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==OR) {
				{
				{
				setState(555);
				match(OR);
				setState(556);
				exception_name();
				}
				}
				setState(561);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(562);
			match(THEN);
			setState(563);
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
		enterRule(_localctx, 92, RULE_sql_statement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(565);
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
		enterRule(_localctx, 94, RULE_data_manipulation_language_statements);
		try {
			setState(574);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case MERGE:
				enterOuterAlt(_localctx, 1);
				{
				setState(567);
				merge_statement();
				}
				break;
			case SELECT:
				enterOuterAlt(_localctx, 2);
				{
				setState(568);
				select_statement();
				}
				break;
			case UPDATE:
				enterOuterAlt(_localctx, 3);
				{
				setState(569);
				update_statement();
				}
				break;
			case DELETE:
				enterOuterAlt(_localctx, 4);
				{
				setState(570);
				delete_statement();
				}
				break;
			case INSERT:
				enterOuterAlt(_localctx, 5);
				{
				setState(571);
				insert_statement();
				}
				break;
			case CREATE:
				enterOuterAlt(_localctx, 6);
				{
				setState(572);
				create_table_as2();
				}
				break;
			case TRUNCATE:
				enterOuterAlt(_localctx, 7);
				{
				setState(573);
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
		enterRule(_localctx, 96, RULE_update_statement);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(576);
			match(UPDATE);
			setState(580);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,47,_ctx);
			while ( _alt!=1 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1+1 ) {
					{
					{
					setState(577);
					matchWildcard();
					}
					} 
				}
				setState(582);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,47,_ctx);
			}
			setState(583);
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
		enterRule(_localctx, 98, RULE_delete_statement);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(585);
			match(DELETE);
			setState(586);
			match(FROM);
			setState(590);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,48,_ctx);
			while ( _alt!=1 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1+1 ) {
					{
					{
					setState(587);
					matchWildcard();
					}
					} 
				}
				setState(592);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,48,_ctx);
			}
			setState(593);
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
		enterRule(_localctx, 100, RULE_insert_statement);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(595);
			match(INSERT);
			setState(599);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,49,_ctx);
			while ( _alt!=1 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1+1 ) {
					{
					{
					setState(596);
					matchWildcard();
					}
					} 
				}
				setState(601);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,49,_ctx);
			}
			setState(602);
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
		enterRule(_localctx, 102, RULE_merge_statement);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(604);
			match(MERGE);
			setState(605);
			match(INTO);
			setState(609);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,50,_ctx);
			while ( _alt!=1 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1+1 ) {
					{
					{
					setState(606);
					matchWildcard();
					}
					} 
				}
				setState(611);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,50,_ctx);
			}
			setState(612);
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
		enterRule(_localctx, 104, RULE_condition);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(614);
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
		enterRule(_localctx, 106, RULE_expressions);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(616);
			expression();
			setState(621);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(617);
				match(COMMA);
				setState(618);
				expression();
				}
				}
				setState(623);
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
		enterRule(_localctx, 108, RULE_expression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(624);
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
		int _startState = 110;
		enterRecursionRule(_localctx, 110, RULE_logical_expression, _p);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			{
			setState(627);
			unary_logical_expression();
			}
			_ctx.stop = _input.LT(-1);
			setState(637);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,53,_ctx);
			while ( _alt!=2 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(635);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,52,_ctx) ) {
					case 1:
						{
						_localctx = new Logical_expressionContext(_parentctx, _parentState);
						pushNewRecursionContext(_localctx, _startState, RULE_logical_expression);
						setState(629);
						if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
						setState(630);
						match(AND);
						setState(631);
						logical_expression(3);
						}
						break;
					case 2:
						{
						_localctx = new Logical_expressionContext(_parentctx, _parentState);
						pushNewRecursionContext(_localctx, _startState, RULE_logical_expression);
						setState(632);
						if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
						setState(633);
						match(OR);
						setState(634);
						logical_expression(2);
						}
						break;
					}
					} 
				}
				setState(639);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,53,_ctx);
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
		enterRule(_localctx, 112, RULE_unary_logical_expression);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(641);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,54,_ctx) ) {
			case 1:
				{
				setState(640);
				match(NOT);
				}
				break;
			}
			setState(643);
			multiset_expression();
			setState(649);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,56,_ctx) ) {
			case 1:
				{
				setState(644);
				match(IS);
				setState(646);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(645);
					match(NOT);
					}
				}

				setState(648);
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
		enterRule(_localctx, 114, RULE_multiset_expression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(651);
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
		int _startState = 116;
		enterRecursionRule(_localctx, 116, RULE_relational_expression, _p);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			{
			setState(654);
			compound_expression();
			}
			_ctx.stop = _input.LT(-1);
			setState(662);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,57,_ctx);
			while ( _alt!=2 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					{
					_localctx = new Relational_expressionContext(_parentctx, _parentState);
					pushNewRecursionContext(_localctx, _startState, RULE_relational_expression);
					setState(656);
					if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
					setState(657);
					relational_operator();
					setState(658);
					relational_expression(3);
					}
					} 
				}
				setState(664);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,57,_ctx);
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
		enterRule(_localctx, 118, RULE_compound_expression);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(665);
			concatenation(0);
			setState(681);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,61,_ctx) ) {
			case 1:
				{
				setState(667);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(666);
					match(NOT);
					}
				}

				setState(679);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case IN:
					{
					setState(669);
					match(IN);
					setState(670);
					in_elements();
					}
					break;
				case BETWEEN:
					{
					setState(671);
					match(BETWEEN);
					setState(672);
					between_elements();
					}
					break;
				case LIKE:
				case LIKEC:
				case LIKE2:
				case LIKE4:
					{
					setState(673);
					((Compound_expressionContext)_localctx).like_type = _input.LT(1);
					_la = _input.LA(1);
					if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & 131941395333120L) != 0)) ) {
						((Compound_expressionContext)_localctx).like_type = (Token)_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					setState(674);
					concatenation(0);
					setState(677);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,59,_ctx) ) {
					case 1:
						{
						setState(675);
						match(ESCAPE);
						setState(676);
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
		enterRule(_localctx, 120, RULE_relational_operator);
		int _la;
		try {
			setState(695);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,63,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(683);
				match(EQUALS_OP);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(684);
				match(NOT_EQUAL_OP);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(685);
				match(LESS_THAN_OP);
				setState(686);
				match(GREATER_THAN_OP);
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(687);
				match(EXCLAMATION_OPERATOR_PART);
				setState(688);
				match(EQUALS_OP);
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(689);
				match(CARRET_OPERATOR_PART);
				setState(690);
				match(EQUALS_OP);
				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(691);
				_la = _input.LA(1);
				if ( !(_la==LESS_THAN_OP || _la==GREATER_THAN_OP) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(693);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==EQUALS_OP) {
					{
					setState(692);
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
		enterRule(_localctx, 122, RULE_in_elements);
		int _la;
		try {
			setState(711);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,65,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(697);
				match(LEFT_PAREN);
				setState(698);
				concatenation(0);
				setState(703);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(699);
					match(COMMA);
					setState(700);
					concatenation(0);
					}
					}
					setState(705);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(706);
				match(RIGHT_PAREN);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(708);
				constant();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(709);
				bind_variable();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(710);
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
		enterRule(_localctx, 124, RULE_between_elements);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(713);
			concatenation(0);
			setState(714);
			match(AND);
			setState(715);
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
		int _startState = 126;
		enterRecursionRule(_localctx, 126, RULE_concatenation, _p);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			{
			setState(718);
			atom();
			}
			_ctx.stop = _input.LT(-1);
			setState(741);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,67,_ctx);
			while ( _alt!=2 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(739);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,66,_ctx) ) {
					case 1:
						{
						_localctx = new ConcatenationContext(_parentctx, _parentState);
						pushNewRecursionContext(_localctx, _startState, RULE_concatenation);
						setState(720);
						if (!(precpred(_ctx, 7))) throw new FailedPredicateException(this, "precpred(_ctx, 7)");
						setState(721);
						((ConcatenationContext)_localctx).op = match(ASTERISK);
						setState(722);
						concatenation(8);
						}
						break;
					case 2:
						{
						_localctx = new ConcatenationContext(_parentctx, _parentState);
						pushNewRecursionContext(_localctx, _startState, RULE_concatenation);
						setState(723);
						if (!(precpred(_ctx, 6))) throw new FailedPredicateException(this, "precpred(_ctx, 6)");
						setState(724);
						((ConcatenationContext)_localctx).op = match(SOLIDUS);
						setState(725);
						concatenation(7);
						}
						break;
					case 3:
						{
						_localctx = new ConcatenationContext(_parentctx, _parentState);
						pushNewRecursionContext(_localctx, _startState, RULE_concatenation);
						setState(726);
						if (!(precpred(_ctx, 5))) throw new FailedPredicateException(this, "precpred(_ctx, 5)");
						setState(727);
						((ConcatenationContext)_localctx).op = match(PLUS_SIGN);
						setState(728);
						concatenation(6);
						}
						break;
					case 4:
						{
						_localctx = new ConcatenationContext(_parentctx, _parentState);
						pushNewRecursionContext(_localctx, _startState, RULE_concatenation);
						setState(729);
						if (!(precpred(_ctx, 4))) throw new FailedPredicateException(this, "precpred(_ctx, 4)");
						setState(730);
						((ConcatenationContext)_localctx).op = match(MINUS_SIGN);
						setState(731);
						concatenation(5);
						}
						break;
					case 5:
						{
						_localctx = new ConcatenationContext(_parentctx, _parentState);
						pushNewRecursionContext(_localctx, _startState, RULE_concatenation);
						setState(732);
						if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
						setState(733);
						((ConcatenationContext)_localctx).op = match(BAR);
						setState(734);
						concatenation(4);
						}
						break;
					case 6:
						{
						_localctx = new ConcatenationContext(_parentctx, _parentState);
						pushNewRecursionContext(_localctx, _startState, RULE_concatenation);
						setState(735);
						if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
						setState(736);
						match(BAR);
						setState(737);
						match(BAR);
						setState(738);
						concatenation(3);
						}
						break;
					}
					} 
				}
				setState(743);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,67,_ctx);
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
		enterRule(_localctx, 128, RULE_unary_expression);
		int _la;
		try {
			setState(747);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case PLUS_SIGN:
			case MINUS_SIGN:
				enterOuterAlt(_localctx, 1);
				{
				setState(744);
				_la = _input.LA(1);
				if ( !(_la==PLUS_SIGN || _la==MINUS_SIGN) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(745);
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
				setState(746);
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
		enterRule(_localctx, 130, RULE_atom);
		try {
			setState(757);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,69,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(749);
				bind_variable();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(750);
				constant();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(751);
				general_element();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(752);
				match(LEFT_PAREN);
				setState(753);
				expressions();
				setState(754);
				match(RIGHT_PAREN);
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(756);
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
		enterRule(_localctx, 132, RULE_routine_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(759);
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
		enterRule(_localctx, 134, RULE_parameter_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(761);
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
		enterRule(_localctx, 136, RULE_procedure_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(763);
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
		enterRule(_localctx, 138, RULE_exception_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(765);
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
		enterRule(_localctx, 140, RULE_index_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(767);
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
		enterRule(_localctx, 142, RULE_record_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(769);
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
		enterRule(_localctx, 144, RULE_column_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(771);
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
		enterRule(_localctx, 146, RULE_function_argument);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(773);
			match(LEFT_PAREN);
			setState(782);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & -277079077691394L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & 1065353247L) != 0)) {
				{
				setState(774);
				argument();
				setState(779);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(775);
					match(COMMA);
					setState(776);
					argument();
					}
					}
					setState(781);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(784);
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
		enterRule(_localctx, 148, RULE_argument);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(786);
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
		enterRule(_localctx, 150, RULE_type_spec);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(788);
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
		enterRule(_localctx, 152, RULE_datatype);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(790);
			native_datatype_element();
			setState(792);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==LEFT_PAREN) {
				{
				setState(791);
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
		enterRule(_localctx, 154, RULE_precision_part);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(794);
			match(LEFT_PAREN);
			setState(795);
			numeric();
			setState(798);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==COMMA) {
				{
				setState(796);
				match(COMMA);
				setState(797);
				numeric();
				}
			}

			setState(800);
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
		enterRule(_localctx, 156, RULE_native_datatype_element);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(802);
			_la = _input.LA(1);
			if ( !(((((_la - 52)) & ~0x3f) == 0 && ((1L << (_la - 52)) & 65023L) != 0)) ) {
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
		enterRule(_localctx, 158, RULE_bind_variable);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(804);
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
		enterRule(_localctx, 160, RULE_general_element);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(806);
			id_expression();
			setState(811);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,74,_ctx);
			while ( _alt!=2 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(807);
					match(PERIOD);
					setState(808);
					id_expression();
					}
					} 
				}
				setState(813);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,74,_ctx);
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
		enterRule(_localctx, 162, RULE_constant);
		try {
			setState(827);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case TIMESTAMP:
				enterOuterAlt(_localctx, 1);
				{
				setState(814);
				match(TIMESTAMP);
				setState(817);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case CHAR_STRING:
				case NATIONAL_CHAR_STRING_LIT:
					{
					setState(815);
					quoted_string();
					}
					break;
				case BINDVAR:
					{
					setState(816);
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
				setState(819);
				numeric();
				}
				break;
			case DATE:
				enterOuterAlt(_localctx, 3);
				{
				setState(820);
				match(DATE);
				setState(821);
				quoted_string();
				}
				break;
			case CHAR_STRING:
			case NATIONAL_CHAR_STRING_LIT:
				enterOuterAlt(_localctx, 4);
				{
				setState(822);
				quoted_string();
				}
				break;
			case NULL_:
				enterOuterAlt(_localctx, 5);
				{
				setState(823);
				match(NULL_);
				}
				break;
			case TRUE:
				enterOuterAlt(_localctx, 6);
				{
				setState(824);
				match(TRUE);
				}
				break;
			case FALSE:
				enterOuterAlt(_localctx, 7);
				{
				setState(825);
				match(FALSE);
				}
				break;
			case DEFAULT:
				enterOuterAlt(_localctx, 8);
				{
				setState(826);
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
		enterRule(_localctx, 164, RULE_numeric);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(829);
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
		enterRule(_localctx, 166, RULE_quoted_string);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(831);
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
		enterRule(_localctx, 168, RULE_identifier);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(833);
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
		enterRule(_localctx, 170, RULE_id_expression);
		try {
			setState(837);
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
				setState(835);
				regular_id();
				}
				break;
			case DELIMITED_ID:
				enterOuterAlt(_localctx, 2);
				{
				setState(836);
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
		enterRule(_localctx, 172, RULE_regular_id);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(839);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & -280927368388610L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & 536870927L) != 0)) ) {
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
		case 55:
			return logical_expression_sempred((Logical_expressionContext)_localctx, predIndex);
		case 58:
			return relational_expression_sempred((Relational_expressionContext)_localctx, predIndex);
		case 63:
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
		"\u0004\u0001`\u034a\u0002\u0000\u0007\u0000\u0002\u0001\u0007\u0001\u0002"+
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
		"U\u0007U\u0002V\u0007V\u0001\u0000\u0001\u0000\u0003\u0000\u00b1\b\u0000"+
		"\u0005\u0000\u00b3\b\u0000\n\u0000\f\u0000\u00b6\t\u0000\u0001\u0000\u0001"+
		"\u0000\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001"+
		"\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001"+
		"\u0001\u0001\u0001\u0003\u0001\u00c7\b\u0001\u0001\u0002\u0001\u0002\u0001"+
		"\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0005\u0002\u00cf\b\u0002\n"+
		"\u0002\f\u0002\u00d2\t\u0002\u0001\u0002\u0001\u0002\u0003\u0002\u00d6"+
		"\b\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0003\u0002\u00dc"+
		"\b\u0002\u0001\u0002\u0003\u0002\u00df\b\u0002\u0001\u0002\u0001\u0002"+
		"\u0001\u0002\u0001\u0002\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
		"\u0001\u0003\u0001\u0003\u0001\u0003\u0005\u0003\u00ec\b\u0003\n\u0003"+
		"\f\u0003\u00ef\t\u0003\u0001\u0003\u0001\u0003\u0003\u0003\u00f3\b\u0003"+
		"\u0001\u0003\u0001\u0003\u0003\u0003\u00f7\b\u0003\u0001\u0003\u0003\u0003"+
		"\u00fa\b\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0004"+
		"\u0001\u0004\u0001\u0004\u0001\u0005\u0003\u0005\u0104\b\u0005\u0001\u0005"+
		"\u0003\u0005\u0107\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0005\u0006"+
		"\u0112\b\u0006\n\u0006\f\u0006\u0115\t\u0006\u0001\u0006\u0001\u0006\u0001"+
		"\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0005"+
		"\u0007\u011f\b\u0007\n\u0007\f\u0007\u0122\t\u0007\u0001\b\u0001\b\u0001"+
		"\b\u0001\b\u0001\b\u0001\b\u0001\b\u0001\t\u0001\t\u0001\t\u0001\t\u0001"+
		"\t\u0001\n\u0001\n\u0001\n\u0005\n\u0133\b\n\n\n\f\n\u0136\t\n\u0001\u000b"+
		"\u0001\u000b\u0001\u000b\u0001\u000b\u0001\f\u0001\f\u0001\r\u0001\r\u0001"+
		"\r\u0001\r\u0005\r\u0142\b\r\n\r\f\r\u0145\t\r\u0001\r\u0001\r\u0003\r"+
		"\u0149\b\r\u0001\u000e\u0001\u000e\u0001\u000f\u0001\u000f\u0001\u000f"+
		"\u0001\u0010\u0001\u0010\u0001\u0010\u0005\u0010\u0153\b\u0010\n\u0010"+
		"\f\u0010\u0156\t\u0010\u0001\u0010\u0001\u0010\u0001\u0011\u0001\u0011"+
		"\u0005\u0011\u015c\b\u0011\n\u0011\f\u0011\u015f\t\u0011\u0001\u0011\u0001"+
		"\u0011\u0001\u0012\u0001\u0012\u0005\u0012\u0165\b\u0012\n\u0012\f\u0012"+
		"\u0168\t\u0012\u0001\u0012\u0001\u0012\u0001\u0013\u0001\u0013\u0001\u0013"+
		"\u0005\u0013\u016f\b\u0013\n\u0013\f\u0013\u0172\t\u0013\u0001\u0013\u0001"+
		"\u0013\u0001\u0014\u0001\u0014\u0005\u0014\u0178\b\u0014\n\u0014\f\u0014"+
		"\u017b\t\u0014\u0001\u0014\u0001\u0014\u0001\u0015\u0001\u0015\u0001\u0015"+
		"\u0005\u0015\u0182\b\u0015\n\u0015\f\u0015\u0185\t\u0015\u0001\u0015\u0001"+
		"\u0015\u0001\u0016\u0001\u0016\u0005\u0016\u018b\b\u0016\n\u0016\f\u0016"+
		"\u018e\t\u0016\u0001\u0016\u0001\u0016\u0001\u0017\u0001\u0017\u0005\u0017"+
		"\u0194\b\u0017\n\u0017\f\u0017\u0197\t\u0017\u0001\u0017\u0003\u0017\u019a"+
		"\b\u0017\u0001\u0017\u0003\u0017\u019d\b\u0017\u0001\u0018\u0001\u0018"+
		"\u0001\u0018\u0001\u0019\u0004\u0019\u01a3\b\u0019\u000b\u0019\f\u0019"+
		"\u01a4\u0001\u001a\u0001\u001a\u0001\u001b\u0001\u001b\u0003\u001b\u01ab"+
		"\b\u001b\u0001\u001b\u0001\u001b\u0001\u001b\u0003\u001b\u01b0\b\u001b"+
		"\u0001\u001b\u0003\u001b\u01b3\b\u001b\u0001\u001b\u0001\u001b\u0001\u001c"+
		"\u0001\u001c\u0001\u001c\u0004\u001c\u01ba\b\u001c\u000b\u001c\f\u001c"+
		"\u01bb\u0001\u001d\u0001\u001d\u0001\u001d\u0001\u001d\u0001\u001d\u0001"+
		"\u001d\u0001\u001d\u0001\u001d\u0001\u001d\u0003\u001d\u01c7\b\u001d\u0001"+
		"\u001e\u0001\u001e\u0001\u001e\u0001\u001e\u0001\u001f\u0001\u001f\u0001"+
		" \u0001 \u0001!\u0001!\u0001!\u0001!\u0001!\u0005!\u01d6\b!\n!\f!\u01d9"+
		"\t!\u0001!\u0003!\u01dc\b!\u0001!\u0001!\u0001!\u0001\"\u0001\"\u0001"+
		"\"\u0001\"\u0001\"\u0001#\u0001#\u0001#\u0001$\u0001$\u0001$\u0001$\u0003"+
		"$\u01ed\b$\u0001$\u0001$\u0001$\u0001$\u0001$\u0001%\u0001%\u0001%\u0001"+
		"%\u0001%\u0001%\u0001%\u0001%\u0001%\u0001%\u0001%\u0001%\u0003%\u0200"+
		"\b%\u0001&\u0001&\u0005&\u0204\b&\n&\f&\u0207\t&\u0001&\u0001&\u0001\'"+
		"\u0001\'\u0001(\u0001(\u0001)\u0001)\u0003)\u0211\b)\u0001*\u0001*\u0003"+
		"*\u0215\b*\u0001+\u0003+\u0218\b+\u0001+\u0001+\u0003+\u021c\b+\u0001"+
		",\u0001,\u0001,\u0001,\u0004,\u0222\b,\u000b,\f,\u0223\u0003,\u0226\b"+
		",\u0001,\u0001,\u0001-\u0001-\u0001-\u0001-\u0005-\u022e\b-\n-\f-\u0231"+
		"\t-\u0001-\u0001-\u0001-\u0001.\u0001.\u0001/\u0001/\u0001/\u0001/\u0001"+
		"/\u0001/\u0001/\u0003/\u023f\b/\u00010\u00010\u00050\u0243\b0\n0\f0\u0246"+
		"\t0\u00010\u00010\u00011\u00011\u00011\u00051\u024d\b1\n1\f1\u0250\t1"+
		"\u00011\u00011\u00012\u00012\u00052\u0256\b2\n2\f2\u0259\t2\u00012\u0001"+
		"2\u00013\u00013\u00013\u00053\u0260\b3\n3\f3\u0263\t3\u00013\u00013\u0001"+
		"4\u00014\u00015\u00015\u00015\u00055\u026c\b5\n5\f5\u026f\t5\u00016\u0001"+
		"6\u00017\u00017\u00017\u00017\u00017\u00017\u00017\u00017\u00017\u0005"+
		"7\u027c\b7\n7\f7\u027f\t7\u00018\u00038\u0282\b8\u00018\u00018\u00018"+
		"\u00038\u0287\b8\u00018\u00038\u028a\b8\u00019\u00019\u0001:\u0001:\u0001"+
		":\u0001:\u0001:\u0001:\u0001:\u0005:\u0295\b:\n:\f:\u0298\t:\u0001;\u0001"+
		";\u0003;\u029c\b;\u0001;\u0001;\u0001;\u0001;\u0001;\u0001;\u0001;\u0001"+
		";\u0003;\u02a6\b;\u0003;\u02a8\b;\u0003;\u02aa\b;\u0001<\u0001<\u0001"+
		"<\u0001<\u0001<\u0001<\u0001<\u0001<\u0001<\u0001<\u0003<\u02b6\b<\u0003"+
		"<\u02b8\b<\u0001=\u0001=\u0001=\u0001=\u0005=\u02be\b=\n=\f=\u02c1\t="+
		"\u0001=\u0001=\u0001=\u0001=\u0001=\u0003=\u02c8\b=\u0001>\u0001>\u0001"+
		">\u0001>\u0001?\u0001?\u0001?\u0001?\u0001?\u0001?\u0001?\u0001?\u0001"+
		"?\u0001?\u0001?\u0001?\u0001?\u0001?\u0001?\u0001?\u0001?\u0001?\u0001"+
		"?\u0001?\u0001?\u0001?\u0005?\u02e4\b?\n?\f?\u02e7\t?\u0001@\u0001@\u0001"+
		"@\u0003@\u02ec\b@\u0001A\u0001A\u0001A\u0001A\u0001A\u0001A\u0001A\u0001"+
		"A\u0003A\u02f6\bA\u0001B\u0001B\u0001C\u0001C\u0001D\u0001D\u0001E\u0001"+
		"E\u0001F\u0001F\u0001G\u0001G\u0001H\u0001H\u0001I\u0001I\u0001I\u0001"+
		"I\u0005I\u030a\bI\nI\fI\u030d\tI\u0003I\u030f\bI\u0001I\u0001I\u0001J"+
		"\u0001J\u0001K\u0001K\u0001L\u0001L\u0003L\u0319\bL\u0001M\u0001M\u0001"+
		"M\u0001M\u0003M\u031f\bM\u0001M\u0001M\u0001N\u0001N\u0001O\u0001O\u0001"+
		"P\u0001P\u0001P\u0005P\u032a\bP\nP\fP\u032d\tP\u0001Q\u0001Q\u0001Q\u0003"+
		"Q\u0332\bQ\u0001Q\u0001Q\u0001Q\u0001Q\u0001Q\u0001Q\u0001Q\u0001Q\u0003"+
		"Q\u033c\bQ\u0001R\u0001R\u0001S\u0001S\u0001T\u0001T\u0001U\u0001U\u0003"+
		"U\u0346\bU\u0001V\u0001V\u0001V\u000e\u0113\u0120\u0154\u015d\u0166\u0170"+
		"\u0179\u0183\u018c\u0205\u0244\u024e\u0257\u0261\u0003nt~W\u0000\u0002"+
		"\u0004\u0006\b\n\f\u000e\u0010\u0012\u0014\u0016\u0018\u001a\u001c\u001e"+
		" \"$&(*,.02468:<>@BDFHJLNPRTVXZ\\^`bdfhjlnprtvxz|~\u0080\u0082\u0084\u0086"+
		"\u0088\u008a\u008c\u008e\u0090\u0092\u0094\u0096\u0098\u009a\u009c\u009e"+
		"\u00a0\u00a2\u00a4\u00a6\u00a8\u00aa\u00ac\u0000\n\u0001\u000001\u0002"+
		"\u000033KK\u0001\u0001GG\u0001\u0000+.\u0001\u0000ST\u0001\u0000NO\u0002"+
		"\u00004<>C\u0001\u0000YZ\u0001\u0000WX\u0005\u0000\u0001\f\u000e\u001e"+
		" &0C]]\u036c\u0000\u00b4\u0001\u0000\u0000\u0000\u0002\u00c6\u0001\u0000"+
		"\u0000\u0000\u0004\u00c8\u0001\u0000\u0000\u0000\u0006\u00e4\u0001\u0000"+
		"\u0000\u0000\b\u00ff\u0001\u0000\u0000\u0000\n\u0103\u0001\u0000\u0000"+
		"\u0000\f\u010b\u0001\u0000\u0000\u0000\u000e\u0118\u0001\u0000\u0000\u0000"+
		"\u0010\u0123\u0001\u0000\u0000\u0000\u0012\u012a\u0001\u0000\u0000\u0000"+
		"\u0014\u012f\u0001\u0000\u0000\u0000\u0016\u0137\u0001\u0000\u0000\u0000"+
		"\u0018\u013b\u0001\u0000\u0000\u0000\u001a\u0148\u0001\u0000\u0000\u0000"+
		"\u001c\u014a\u0001\u0000\u0000\u0000\u001e\u014c\u0001\u0000\u0000\u0000"+
		" \u014f\u0001\u0000\u0000\u0000\"\u0159\u0001\u0000\u0000\u0000$\u0162"+
		"\u0001\u0000\u0000\u0000&\u016b\u0001\u0000\u0000\u0000(\u0175\u0001\u0000"+
		"\u0000\u0000*\u017e\u0001\u0000\u0000\u0000,\u0188\u0001\u0000\u0000\u0000"+
		".\u0191\u0001\u0000\u0000\u00000\u019e\u0001\u0000\u0000\u00002\u01a2"+
		"\u0001\u0000\u0000\u00004\u01a6\u0001\u0000\u0000\u00006\u01a8\u0001\u0000"+
		"\u0000\u00008\u01b9\u0001\u0000\u0000\u0000:\u01c6\u0001\u0000\u0000\u0000"+
		"<\u01c8\u0001\u0000\u0000\u0000>\u01cc\u0001\u0000\u0000\u0000@\u01ce"+
		"\u0001\u0000\u0000\u0000B\u01d0\u0001\u0000\u0000\u0000D\u01e0\u0001\u0000"+
		"\u0000\u0000F\u01e5\u0001\u0000\u0000\u0000H\u01ec\u0001\u0000\u0000\u0000"+
		"J\u01ff\u0001\u0000\u0000\u0000L\u0201\u0001\u0000\u0000\u0000N\u020a"+
		"\u0001\u0000\u0000\u0000P\u020c\u0001\u0000\u0000\u0000R\u020e\u0001\u0000"+
		"\u0000\u0000T\u0212\u0001\u0000\u0000\u0000V\u0217\u0001\u0000\u0000\u0000"+
		"X\u021d\u0001\u0000\u0000\u0000Z\u0229\u0001\u0000\u0000\u0000\\\u0235"+
		"\u0001\u0000\u0000\u0000^\u023e\u0001\u0000\u0000\u0000`\u0240\u0001\u0000"+
		"\u0000\u0000b\u0249\u0001\u0000\u0000\u0000d\u0253\u0001\u0000\u0000\u0000"+
		"f\u025c\u0001\u0000\u0000\u0000h\u0266\u0001\u0000\u0000\u0000j\u0268"+
		"\u0001\u0000\u0000\u0000l\u0270\u0001\u0000\u0000\u0000n\u0272\u0001\u0000"+
		"\u0000\u0000p\u0281\u0001\u0000\u0000\u0000r\u028b\u0001\u0000\u0000\u0000"+
		"t\u028d\u0001\u0000\u0000\u0000v\u0299\u0001\u0000\u0000\u0000x\u02b7"+
		"\u0001\u0000\u0000\u0000z\u02c7\u0001\u0000\u0000\u0000|\u02c9\u0001\u0000"+
		"\u0000\u0000~\u02cd\u0001\u0000\u0000\u0000\u0080\u02eb\u0001\u0000\u0000"+
		"\u0000\u0082\u02f5\u0001\u0000\u0000\u0000\u0084\u02f7\u0001\u0000\u0000"+
		"\u0000\u0086\u02f9\u0001\u0000\u0000\u0000\u0088\u02fb\u0001\u0000\u0000"+
		"\u0000\u008a\u02fd\u0001\u0000\u0000\u0000\u008c\u02ff\u0001\u0000\u0000"+
		"\u0000\u008e\u0301\u0001\u0000\u0000\u0000\u0090\u0303\u0001\u0000\u0000"+
		"\u0000\u0092\u0305\u0001\u0000\u0000\u0000\u0094\u0312\u0001\u0000\u0000"+
		"\u0000\u0096\u0314\u0001\u0000\u0000\u0000\u0098\u0316\u0001\u0000\u0000"+
		"\u0000\u009a\u031a\u0001\u0000\u0000\u0000\u009c\u0322\u0001\u0000\u0000"+
		"\u0000\u009e\u0324\u0001\u0000\u0000\u0000\u00a0\u0326\u0001\u0000\u0000"+
		"\u0000\u00a2\u033b\u0001\u0000\u0000\u0000\u00a4\u033d\u0001\u0000\u0000"+
		"\u0000\u00a6\u033f\u0001\u0000\u0000\u0000\u00a8\u0341\u0001\u0000\u0000"+
		"\u0000\u00aa\u0345\u0001\u0000\u0000\u0000\u00ac\u0347\u0001\u0000\u0000"+
		"\u0000\u00ae\u00b0\u0003\u0002\u0001\u0000\u00af\u00b1\u0005G\u0000\u0000"+
		"\u00b0\u00af\u0001\u0000\u0000\u0000\u00b0\u00b1\u0001\u0000\u0000\u0000"+
		"\u00b1\u00b3\u0001\u0000\u0000\u0000\u00b2\u00ae\u0001\u0000\u0000\u0000"+
		"\u00b3\u00b6\u0001\u0000\u0000\u0000\u00b4\u00b2\u0001\u0000\u0000\u0000"+
		"\u00b4\u00b5\u0001\u0000\u0000\u0000\u00b5\u00b7\u0001\u0000\u0000\u0000"+
		"\u00b6\u00b4\u0001\u0000\u0000\u0000\u00b7\u00b8\u0005\u0000\u0000\u0001"+
		"\u00b8\u0001\u0001\u0000\u0000\u0000\u00b9\u00c7\u0003\n\u0005\u0000\u00ba"+
		"\u00c7\u0003\u0006\u0003\u0000\u00bb\u00c7\u0003\b\u0004\u0000\u00bc\u00c7"+
		"\u0003\u0010\b\u0000\u00bd\u00c7\u0003\f\u0006\u0000\u00be\u00c7\u0003"+
		",\u0016\u0000\u00bf\u00c7\u0003\"\u0011\u0000\u00c0\u00c7\u0003(\u0014"+
		"\u0000\u00c1\u00c7\u0003$\u0012\u0000\u00c2\u00c7\u0003&\u0013\u0000\u00c3"+
		"\u00c7\u0003*\u0015\u0000\u00c4\u00c7\u0003 \u0010\u0000\u00c5\u00c7\u0003"+
		"V+\u0000\u00c6\u00b9\u0001\u0000\u0000\u0000\u00c6\u00ba\u0001\u0000\u0000"+
		"\u0000\u00c6\u00bb\u0001\u0000\u0000\u0000\u00c6\u00bc\u0001\u0000\u0000"+
		"\u0000\u00c6\u00bd\u0001\u0000\u0000\u0000\u00c6\u00be\u0001\u0000\u0000"+
		"\u0000\u00c6\u00bf\u0001\u0000\u0000\u0000\u00c6\u00c0\u0001\u0000\u0000"+
		"\u0000\u00c6\u00c1\u0001\u0000\u0000\u0000\u00c6\u00c2\u0001\u0000\u0000"+
		"\u0000\u00c6\u00c3\u0001\u0000\u0000\u0000\u00c6\u00c4\u0001\u0000\u0000"+
		"\u0000\u00c6\u00c5\u0001\u0000\u0000\u0000\u00c7\u0003\u0001\u0000\u0000"+
		"\u0000\u00c8\u00c9\u0005\u000f\u0000\u0000\u00c9\u00d5\u0003\u00a8T\u0000"+
		"\u00ca\u00cb\u0005D\u0000\u0000\u00cb\u00d0\u0003.\u0017\u0000\u00cc\u00cd"+
		"\u0005F\u0000\u0000\u00cd\u00cf\u0003.\u0017\u0000\u00ce\u00cc\u0001\u0000"+
		"\u0000\u0000\u00cf\u00d2\u0001\u0000\u0000\u0000\u00d0\u00ce\u0001\u0000"+
		"\u0000\u0000\u00d0\u00d1\u0001\u0000\u0000\u0000\u00d1\u00d3\u0001\u0000"+
		"\u0000\u0000\u00d2\u00d0\u0001\u0000\u0000\u0000\u00d3\u00d4\u0005E\u0000"+
		"\u0000\u00d4\u00d6\u0001\u0000\u0000\u0000\u00d5\u00ca\u0001\u0000\u0000"+
		"\u0000\u00d5\u00d6\u0001\u0000\u0000\u0000\u00d6\u00d7\u0001\u0000\u0000"+
		"\u0000\u00d7\u00d8\u0005\u0010\u0000\u0000\u00d8\u00d9\u0003\u0096K\u0000"+
		"\u00d9\u00db\u0005\"\u0000\u0000\u00da\u00dc\u0005 \u0000\u0000\u00db"+
		"\u00da\u0001\u0000\u0000\u0000\u00db\u00dc\u0001\u0000\u0000\u0000\u00dc"+
		"\u00de\u0001\u0000\u0000\u0000\u00dd\u00df\u00032\u0019\u0000\u00de\u00dd"+
		"\u0001\u0000\u0000\u0000\u00de\u00df\u0001\u0000\u0000\u0000\u00df\u00e0"+
		"\u0001\u0000\u0000\u0000\u00e0\u00e1\u0003X,\u0000\u00e1\u00e2\u0001\u0000"+
		"\u0000\u0000\u00e2\u00e3\u0005G\u0000\u0000\u00e3\u0005\u0001\u0000\u0000"+
		"\u0000\u00e4\u00e5\u0005\u0001\u0000\u0000\u00e5\u00e6\u0005\u000e\u0000"+
		"\u0000\u00e6\u00f2\u0003\u0088D\u0000\u00e7\u00e8\u0005D\u0000\u0000\u00e8"+
		"\u00ed\u0003.\u0017\u0000\u00e9\u00ea\u0005F\u0000\u0000\u00ea\u00ec\u0003"+
		".\u0017\u0000\u00eb\u00e9\u0001\u0000\u0000\u0000\u00ec\u00ef\u0001\u0000"+
		"\u0000\u0000\u00ed\u00eb\u0001\u0000\u0000\u0000\u00ed\u00ee\u0001\u0000"+
		"\u0000\u0000\u00ee\u00f0\u0001\u0000\u0000\u0000\u00ef\u00ed\u0001\u0000"+
		"\u0000\u0000\u00f0\u00f1\u0005E\u0000\u0000\u00f1\u00f3\u0001\u0000\u0000"+
		"\u0000\u00f2\u00e7\u0001\u0000\u0000\u0000\u00f2\u00f3\u0001\u0000\u0000"+
		"\u0000\u00f3\u00f4\u0001\u0000\u0000\u0000\u00f4\u00f6\u0005\"\u0000\u0000"+
		"\u00f5\u00f7\u0005 \u0000\u0000\u00f6\u00f5\u0001\u0000\u0000\u0000\u00f6"+
		"\u00f7\u0001\u0000\u0000\u0000\u00f7\u00f9\u0001\u0000\u0000\u0000\u00f8"+
		"\u00fa\u00032\u0019\u0000\u00f9\u00f8\u0001\u0000\u0000\u0000\u00f9\u00fa"+
		"\u0001\u0000\u0000\u0000\u00fa\u00fb\u0001\u0000\u0000\u0000\u00fb\u00fc"+
		"\u0003X,\u0000\u00fc\u00fd\u0001\u0000\u0000\u0000\u00fd\u00fe\u0005G"+
		"\u0000\u0000\u00fe\u0007\u0001\u0000\u0000\u0000\u00ff\u0100\u0005\u0001"+
		"\u0000\u0000\u0100\u0101\u0003\u0004\u0002\u0000\u0101\t\u0001\u0000\u0000"+
		"\u0000\u0102\u0104\u0005 \u0000\u0000\u0103\u0102\u0001\u0000\u0000\u0000"+
		"\u0103\u0104\u0001\u0000\u0000\u0000\u0104\u0106\u0001\u0000\u0000\u0000"+
		"\u0105\u0107\u00032\u0019\u0000\u0106\u0105\u0001\u0000\u0000\u0000\u0106"+
		"\u0107\u0001\u0000\u0000\u0000\u0107\u0108\u0001\u0000\u0000\u0000\u0108"+
		"\u0109\u0003X,\u0000\u0109\u010a\u0005G\u0000\u0000\u010a\u000b\u0001"+
		"\u0000\u0000\u0000\u010b\u010c\u0005\u0001\u0000\u0000\u010c\u010d\u0005"+
		"\u0002\u0000\u0000\u010d\u010e\u0003\u0018\f\u0000\u010e\u010f\u0005\""+
		"\u0000\u0000\u010f\u0113\u0005\u0005\u0000\u0000\u0110\u0112\t\u0000\u0000"+
		"\u0000\u0111\u0110\u0001\u0000\u0000\u0000\u0112\u0115\u0001\u0000\u0000"+
		"\u0000\u0113\u0114\u0001\u0000\u0000\u0000\u0113\u0111\u0001\u0000\u0000"+
		"\u0000\u0114\u0116\u0001\u0000\u0000\u0000\u0115\u0113\u0001\u0000\u0000"+
		"\u0000\u0116\u0117\u0005G\u0000\u0000\u0117\r\u0001\u0000\u0000\u0000"+
		"\u0118\u0119\u0005\u0001\u0000\u0000\u0119\u011a\u0005\u0002\u0000\u0000"+
		"\u011a\u011b\u0003\u0018\f\u0000\u011b\u011c\u0005\"\u0000\u0000\u011c"+
		"\u0120\u0005\u0005\u0000\u0000\u011d\u011f\t\u0000\u0000\u0000\u011e\u011d"+
		"\u0001\u0000\u0000\u0000\u011f\u0122\u0001\u0000\u0000\u0000\u0120\u0121"+
		"\u0001\u0000\u0000\u0000\u0120\u011e\u0001\u0000\u0000\u0000\u0121\u000f"+
		"\u0001\u0000\u0000\u0000\u0122\u0120\u0001\u0000\u0000\u0000\u0123\u0124"+
		"\u0005\u0001\u0000\u0000\u0124\u0125\u0005\u0002\u0000\u0000\u0125\u0126"+
		"\u0003\u0018\f\u0000\u0126\u0127\u0003\u001a\r\u0000\u0127\u0128\u0003"+
		"\u0012\t\u0000\u0128\u0129\u0005G\u0000\u0000\u0129\u0011\u0001\u0000"+
		"\u0000\u0000\u012a\u012b\u0005\u0003\u0000\u0000\u012b\u012c\u0005D\u0000"+
		"\u0000\u012c\u012d\u0003\u0014\n\u0000\u012d\u012e\u0005E\u0000\u0000"+
		"\u012e\u0013\u0001\u0000\u0000\u0000\u012f\u0134\u0003\u0016\u000b\u0000"+
		"\u0130\u0131\u0005F\u0000\u0000\u0131\u0133\u0003\u0016\u000b\u0000\u0132"+
		"\u0130\u0001\u0000\u0000\u0000\u0133\u0136\u0001\u0000\u0000\u0000\u0134"+
		"\u0132\u0001\u0000\u0000\u0000\u0134\u0135\u0001\u0000\u0000\u0000\u0135"+
		"\u0015\u0001\u0000\u0000\u0000\u0136\u0134\u0001\u0000\u0000\u0000\u0137"+
		"\u0138\u0005W\u0000\u0000\u0138\u0139\u0005L\u0000\u0000\u0139\u013a\u0005"+
		"W\u0000\u0000\u013a\u0017\u0001\u0000\u0000\u0000\u013b\u013c\u0003\u00a8"+
		"T\u0000\u013c\u0019\u0001\u0000\u0000\u0000\u013d\u013e\u0005D\u0000\u0000"+
		"\u013e\u0143\u0003\u001c\u000e\u0000\u013f\u0140\u0005F\u0000\u0000\u0140"+
		"\u0142\u0003\u001c\u000e\u0000\u0141\u013f\u0001\u0000\u0000\u0000\u0142"+
		"\u0145\u0001\u0000\u0000\u0000\u0143\u0141\u0001\u0000\u0000\u0000\u0143"+
		"\u0144\u0001\u0000\u0000\u0000\u0144\u0146\u0001\u0000\u0000\u0000\u0145"+
		"\u0143\u0001\u0000\u0000\u0000\u0146\u0147\u0005E\u0000\u0000\u0147\u0149"+
		"\u0001\u0000\u0000\u0000\u0148\u013d\u0001\u0000\u0000\u0000\u0148\u0149"+
		"\u0001\u0000\u0000\u0000\u0149\u001b\u0001\u0000\u0000\u0000\u014a\u014b"+
		"\u0003\u001e\u000f\u0000\u014b\u001d\u0001\u0000\u0000\u0000\u014c\u014d"+
		"\u0003\u0090H\u0000\u014d\u014e\u0003\u0098L\u0000\u014e\u001f\u0001\u0000"+
		"\u0000\u0000\u014f\u0150\u0005\r\u0000\u0000\u0150\u0154\u0005\u0002\u0000"+
		"\u0000\u0151\u0153\t\u0000\u0000\u0000\u0152\u0151\u0001\u0000\u0000\u0000"+
		"\u0153\u0156\u0001\u0000\u0000\u0000\u0154\u0155\u0001\u0000\u0000\u0000"+
		"\u0154\u0152\u0001\u0000\u0000\u0000\u0155\u0157\u0001\u0000\u0000\u0000"+
		"\u0156\u0154\u0001\u0000\u0000\u0000\u0157\u0158\u0005G\u0000\u0000\u0158"+
		"!\u0001\u0000\u0000\u0000\u0159\u015d\u0005\u0005\u0000\u0000\u015a\u015c"+
		"\t\u0000\u0000\u0000\u015b\u015a\u0001\u0000\u0000\u0000\u015c\u015f\u0001"+
		"\u0000\u0000\u0000\u015d\u015e\u0001\u0000\u0000\u0000\u015d\u015b\u0001"+
		"\u0000\u0000\u0000\u015e\u0160\u0001\u0000\u0000\u0000\u015f\u015d\u0001"+
		"\u0000\u0000\u0000\u0160\u0161\u0005G\u0000\u0000\u0161#\u0001\u0000\u0000"+
		"\u0000\u0162\u0166\u0005\u0007\u0000\u0000\u0163\u0165\t\u0000\u0000\u0000"+
		"\u0164\u0163\u0001\u0000\u0000\u0000\u0165\u0168\u0001\u0000\u0000\u0000"+
		"\u0166\u0167\u0001\u0000\u0000\u0000\u0166\u0164\u0001\u0000\u0000\u0000"+
		"\u0167\u0169\u0001\u0000\u0000\u0000\u0168\u0166\u0001\u0000\u0000\u0000"+
		"\u0169\u016a\u0005G\u0000\u0000\u016a%\u0001\u0000\u0000\u0000\u016b\u016c"+
		"\u0005\b\u0000\u0000\u016c\u0170\u0005\u000b\u0000\u0000\u016d\u016f\t"+
		"\u0000\u0000\u0000\u016e\u016d\u0001\u0000\u0000\u0000\u016f\u0172\u0001"+
		"\u0000\u0000\u0000\u0170\u0171\u0001\u0000\u0000\u0000\u0170\u016e\u0001"+
		"\u0000\u0000\u0000\u0171\u0173\u0001\u0000\u0000\u0000\u0172\u0170\u0001"+
		"\u0000\u0000\u0000\u0173\u0174\u0005G\u0000\u0000\u0174\'\u0001\u0000"+
		"\u0000\u0000\u0175\u0179\u0005\u0006\u0000\u0000\u0176\u0178\t\u0000\u0000"+
		"\u0000\u0177\u0176\u0001\u0000\u0000\u0000\u0178\u017b\u0001\u0000\u0000"+
		"\u0000\u0179\u017a\u0001\u0000\u0000\u0000\u0179\u0177\u0001\u0000\u0000"+
		"\u0000\u017a\u017c\u0001\u0000\u0000\u0000\u017b\u0179\u0001\u0000\u0000"+
		"\u0000\u017c\u017d\u0005G\u0000\u0000\u017d)\u0001\u0000\u0000\u0000\u017e"+
		"\u017f\u0005\t\u0000\u0000\u017f\u0183\u0005\n\u0000\u0000\u0180\u0182"+
		"\t\u0000\u0000\u0000\u0181\u0180\u0001\u0000\u0000\u0000\u0182\u0185\u0001"+
		"\u0000\u0000\u0000\u0183\u0184\u0001\u0000\u0000\u0000\u0183\u0181\u0001"+
		"\u0000\u0000\u0000\u0184\u0186\u0001\u0000\u0000\u0000\u0185\u0183\u0001"+
		"\u0000\u0000\u0000\u0186\u0187\u0005G\u0000\u0000\u0187+\u0001\u0000\u0000"+
		"\u0000\u0188\u018c\u0005\u0004\u0000\u0000\u0189\u018b\t\u0000\u0000\u0000"+
		"\u018a\u0189\u0001\u0000\u0000\u0000\u018b\u018e\u0001\u0000\u0000\u0000"+
		"\u018c\u018d\u0001\u0000\u0000\u0000\u018c\u018a\u0001\u0000\u0000\u0000"+
		"\u018d\u018f\u0001\u0000\u0000\u0000\u018e\u018c\u0001\u0000\u0000\u0000"+
		"\u018f\u0190\u0005G\u0000\u0000\u0190-\u0001\u0000\u0000\u0000\u0191\u0195"+
		"\u0003\u0086C\u0000\u0192\u0194\u0007\u0000\u0000\u0000\u0193\u0192\u0001"+
		"\u0000\u0000\u0000\u0194\u0197\u0001\u0000\u0000\u0000\u0195\u0193\u0001"+
		"\u0000\u0000\u0000\u0195\u0196\u0001\u0000\u0000\u0000\u0196\u0199\u0001"+
		"\u0000\u0000\u0000\u0197\u0195\u0001\u0000\u0000\u0000\u0198\u019a\u0003"+
		"\u0096K\u0000\u0199\u0198\u0001\u0000\u0000\u0000\u0199\u019a\u0001\u0000"+
		"\u0000\u0000\u019a\u019c\u0001\u0000\u0000\u0000\u019b\u019d\u00030\u0018"+
		"\u0000\u019c\u019b\u0001\u0000\u0000\u0000\u019c\u019d\u0001\u0000\u0000"+
		"\u0000\u019d/\u0001\u0000\u0000\u0000\u019e\u019f\u0007\u0001\u0000\u0000"+
		"\u019f\u01a0\u0003l6\u0000\u01a01\u0001\u0000\u0000\u0000\u01a1\u01a3"+
		"\u00034\u001a\u0000\u01a2\u01a1\u0001\u0000\u0000\u0000\u01a3\u01a4\u0001"+
		"\u0000\u0000\u0000\u01a4\u01a2\u0001\u0000\u0000\u0000\u01a4\u01a5\u0001"+
		"\u0000\u0000\u0000\u01a53\u0001\u0000\u0000\u0000\u01a6\u01a7\u00036\u001b"+
		"\u0000\u01a75\u0001\u0000\u0000\u0000\u01a8\u01aa\u0003\u00a8T\u0000\u01a9"+
		"\u01ab\u00052\u0000\u0000\u01aa\u01a9\u0001\u0000\u0000\u0000\u01aa\u01ab"+
		"\u0001\u0000\u0000\u0000\u01ab\u01ac\u0001\u0000\u0000\u0000\u01ac\u01af"+
		"\u0003\u0096K\u0000\u01ad\u01ae\u0005&\u0000\u0000\u01ae\u01b0\u0005\'"+
		"\u0000\u0000\u01af\u01ad\u0001\u0000\u0000\u0000\u01af\u01b0\u0001\u0000"+
		"\u0000\u0000\u01b0\u01b2\u0001\u0000\u0000\u0000\u01b1\u01b3\u00030\u0018"+
		"\u0000\u01b2\u01b1\u0001\u0000\u0000\u0000\u01b2\u01b3\u0001\u0000\u0000"+
		"\u0000\u01b3\u01b4\u0001\u0000\u0000\u0000\u01b4\u01b5\u0005G\u0000\u0000"+
		"\u01b57\u0001\u0000\u0000\u0000\u01b6\u01b7\u0003:\u001d\u0000\u01b7\u01b8"+
		"\u0007\u0002\u0000\u0000\u01b8\u01ba\u0001\u0000\u0000\u0000\u01b9\u01b6"+
		"\u0001\u0000\u0000\u0000\u01ba\u01bb\u0001\u0000\u0000\u0000\u01bb\u01b9"+
		"\u0001\u0000\u0000\u0000\u01bb\u01bc\u0001\u0000\u0000\u0000\u01bc9\u0001"+
		"\u0000\u0000\u0000\u01bd\u01c7\u0003<\u001e\u0000\u01be\u01c7\u0003>\u001f"+
		"\u0000\u01bf\u01c7\u0003@ \u0000\u01c0\u01c7\u0003B!\u0000\u01c1\u01c7"+
		"\u0003H$\u0000\u01c2\u01c7\u0003R)\u0000\u01c3\u01c7\u0003T*\u0000\u01c4"+
		"\u01c7\u0003\\.\u0000\u01c5\u01c7\u0003V+\u0000\u01c6\u01bd\u0001\u0000"+
		"\u0000\u0000\u01c6\u01be\u0001\u0000\u0000\u0000\u01c6\u01bf\u0001\u0000"+
		"\u0000\u0000\u01c6\u01c0\u0001\u0000\u0000\u0000\u01c6\u01c1\u0001\u0000"+
		"\u0000\u0000\u01c6\u01c2\u0001\u0000\u0000\u0000\u01c6\u01c3\u0001\u0000"+
		"\u0000\u0000\u01c6\u01c4\u0001\u0000\u0000\u0000\u01c6\u01c5\u0001\u0000"+
		"\u0000\u0000\u01c7;\u0001\u0000\u0000\u0000\u01c8\u01c9\u0003\u00a0P\u0000"+
		"\u01c9\u01ca\u0005K\u0000\u0000\u01ca\u01cb\u0003l6\u0000\u01cb=\u0001"+
		"\u0000\u0000\u0000\u01cc\u01cd\u0005\u001d\u0000\u0000\u01cd?\u0001\u0000"+
		"\u0000\u0000\u01ce\u01cf\u0005\u001c\u0000\u0000\u01cfA\u0001\u0000\u0000"+
		"\u0000\u01d0\u01d1\u0005\u0018\u0000\u0000\u01d1\u01d2\u0003h4\u0000\u01d2"+
		"\u01d3\u0005\u0015\u0000\u0000\u01d3\u01d7\u00038\u001c\u0000\u01d4\u01d6"+
		"\u0003D\"\u0000\u01d5\u01d4\u0001\u0000\u0000\u0000\u01d6\u01d9\u0001"+
		"\u0000\u0000\u0000\u01d7\u01d5\u0001\u0000\u0000\u0000\u01d7\u01d8\u0001"+
		"\u0000\u0000\u0000\u01d8\u01db\u0001\u0000\u0000\u0000\u01d9\u01d7\u0001"+
		"\u0000\u0000\u0000\u01da\u01dc\u0003F#\u0000\u01db\u01da\u0001\u0000\u0000"+
		"\u0000\u01db\u01dc\u0001\u0000\u0000\u0000\u01dc\u01dd\u0001\u0000\u0000"+
		"\u0000\u01dd\u01de\u0005\u0012\u0000\u0000\u01de\u01df\u0005\u0018\u0000"+
		"\u0000\u01dfC\u0001\u0000\u0000\u0000\u01e0\u01e1\u0005\u0017\u0000\u0000"+
		"\u01e1\u01e2\u0003h4\u0000\u01e2\u01e3\u0005\u0015\u0000\u0000\u01e3\u01e4"+
		"\u00038\u001c\u0000\u01e4E\u0001\u0000\u0000\u0000\u01e5\u01e6\u0005\u0016"+
		"\u0000\u0000\u01e6\u01e7\u00038\u001c\u0000\u01e7G\u0001\u0000\u0000\u0000"+
		"\u01e8\u01e9\u0005\u001b\u0000\u0000\u01e9\u01ed\u0003h4\u0000\u01ea\u01eb"+
		"\u0005\u001a\u0000\u0000\u01eb\u01ed\u0003J%\u0000\u01ec\u01e8\u0001\u0000"+
		"\u0000\u0000\u01ec\u01ea\u0001\u0000\u0000\u0000\u01ec\u01ed\u0001\u0000"+
		"\u0000\u0000\u01ed\u01ee\u0001\u0000\u0000\u0000\u01ee\u01ef\u0005\u0019"+
		"\u0000\u0000\u01ef\u01f0\u00038\u001c\u0000\u01f0\u01f1\u0005\u0012\u0000"+
		"\u0000\u01f1\u01f2\u0005\u0019\u0000\u0000\u01f2I\u0001\u0000\u0000\u0000"+
		"\u01f3\u01f4\u0003\u008cF\u0000\u01f4\u01f5\u00050\u0000\u0000\u01f5\u01f6"+
		"\u0003N\'\u0000\u01f6\u01f7\u0005J\u0000\u0000\u01f7\u01f8\u0003P(\u0000"+
		"\u01f8\u0200\u0001\u0000\u0000\u0000\u01f9\u01fa\u0003\u008eG\u0000\u01fa"+
		"\u01fb\u00050\u0000\u0000\u01fb\u01fc\u0005D\u0000\u0000\u01fc\u01fd\u0003"+
		"L&\u0000\u01fd\u01fe\u0005E\u0000\u0000\u01fe\u0200\u0001\u0000\u0000"+
		"\u0000\u01ff\u01f3\u0001\u0000\u0000\u0000\u01ff\u01f9\u0001\u0000\u0000"+
		"\u0000\u0200K\u0001\u0000\u0000\u0000\u0201\u0205\u0005\u0005\u0000\u0000"+
		"\u0202\u0204\t\u0000\u0000\u0000\u0203\u0202\u0001\u0000\u0000\u0000\u0204"+
		"\u0207\u0001\u0000\u0000\u0000\u0205\u0206\u0001\u0000\u0000\u0000\u0205"+
		"\u0203\u0001\u0000\u0000\u0000\u0206\u0208\u0001\u0000\u0000\u0000\u0207"+
		"\u0205\u0001\u0000\u0000\u0000\u0208\u0209\u0005G\u0000\u0000\u0209M\u0001"+
		"\u0000\u0000\u0000\u020a\u020b\u0003~?\u0000\u020bO\u0001\u0000\u0000"+
		"\u0000\u020c\u020d\u0003~?\u0000\u020dQ\u0001\u0000\u0000\u0000\u020e"+
		"\u0210\u0005\u001e\u0000\u0000\u020f\u0211\u0003\u008aE\u0000\u0210\u020f"+
		"\u0001\u0000\u0000\u0000\u0210\u0211\u0001\u0000\u0000\u0000\u0211S\u0001"+
		"\u0000\u0000\u0000\u0212\u0214\u0005\u0010\u0000\u0000\u0213\u0215\u0003"+
		"l6\u0000\u0214\u0213\u0001\u0000\u0000\u0000\u0214\u0215\u0001\u0000\u0000"+
		"\u0000\u0215U\u0001\u0000\u0000\u0000\u0216\u0218\u0005!\u0000\u0000\u0217"+
		"\u0216\u0001\u0000\u0000\u0000\u0217\u0218\u0001\u0000\u0000\u0000\u0218"+
		"\u0219\u0001\u0000\u0000\u0000\u0219\u021b\u0003\u0084B\u0000\u021a\u021c"+
		"\u0003\u0092I\u0000\u021b\u021a\u0001\u0000\u0000\u0000\u021b\u021c\u0001"+
		"\u0000\u0000\u0000\u021cW\u0001\u0000\u0000\u0000\u021d\u021e\u0005\u0011"+
		"\u0000\u0000\u021e\u0225\u00038\u001c\u0000\u021f\u0221\u0005\u0013\u0000"+
		"\u0000\u0220\u0222\u0003Z-\u0000\u0221\u0220\u0001\u0000\u0000\u0000\u0222"+
		"\u0223\u0001\u0000\u0000\u0000\u0223\u0221\u0001\u0000\u0000\u0000\u0223"+
		"\u0224\u0001\u0000\u0000\u0000\u0224\u0226\u0001\u0000\u0000\u0000\u0225"+
		"\u021f\u0001\u0000\u0000\u0000\u0225\u0226\u0001\u0000\u0000\u0000\u0226"+
		"\u0227\u0001\u0000\u0000\u0000\u0227\u0228\u0005\u0012\u0000\u0000\u0228"+
		"Y\u0001\u0000\u0000\u0000\u0229\u022a\u0005\u0014\u0000\u0000\u022a\u022f"+
		"\u0003\u008aE\u0000\u022b\u022c\u0005%\u0000\u0000\u022c\u022e\u0003\u008a"+
		"E\u0000\u022d\u022b\u0001\u0000\u0000\u0000\u022e\u0231\u0001\u0000\u0000"+
		"\u0000\u022f\u022d\u0001\u0000\u0000\u0000\u022f\u0230\u0001\u0000\u0000"+
		"\u0000\u0230\u0232\u0001\u0000\u0000\u0000\u0231\u022f\u0001\u0000\u0000"+
		"\u0000\u0232\u0233\u0005\u0015\u0000\u0000\u0233\u0234\u00038\u001c\u0000"+
		"\u0234[\u0001\u0000\u0000\u0000\u0235\u0236\u0003^/\u0000\u0236]\u0001"+
		"\u0000\u0000\u0000\u0237\u023f\u0003f3\u0000\u0238\u023f\u0003L&\u0000"+
		"\u0239\u023f\u0003`0\u0000\u023a\u023f\u0003b1\u0000\u023b\u023f\u0003"+
		"d2\u0000\u023c\u023f\u0003\u000e\u0007\u0000\u023d\u023f\u0003 \u0010"+
		"\u0000\u023e\u0237\u0001\u0000\u0000\u0000\u023e\u0238\u0001\u0000\u0000"+
		"\u0000\u023e\u0239\u0001\u0000\u0000\u0000\u023e\u023a\u0001\u0000\u0000"+
		"\u0000\u023e\u023b\u0001\u0000\u0000\u0000\u023e\u023c\u0001\u0000\u0000"+
		"\u0000\u023e\u023d\u0001\u0000\u0000\u0000\u023f_\u0001\u0000\u0000\u0000"+
		"\u0240\u0244\u0005\u0007\u0000\u0000\u0241\u0243\t\u0000\u0000\u0000\u0242"+
		"\u0241\u0001\u0000\u0000\u0000\u0243\u0246\u0001\u0000\u0000\u0000\u0244"+
		"\u0245\u0001\u0000\u0000\u0000\u0244\u0242\u0001\u0000\u0000\u0000\u0245"+
		"\u0247\u0001\u0000\u0000\u0000\u0246\u0244\u0001\u0000\u0000\u0000\u0247"+
		"\u0248\u0005G\u0000\u0000\u0248a\u0001\u0000\u0000\u0000\u0249\u024a\u0005"+
		"\b\u0000\u0000\u024a\u024e\u0005\u000b\u0000\u0000\u024b\u024d\t\u0000"+
		"\u0000\u0000\u024c\u024b\u0001\u0000\u0000\u0000\u024d\u0250\u0001\u0000"+
		"\u0000\u0000\u024e\u024f\u0001\u0000\u0000\u0000\u024e\u024c\u0001\u0000"+
		"\u0000\u0000\u024f\u0251\u0001\u0000\u0000\u0000\u0250\u024e\u0001\u0000"+
		"\u0000\u0000\u0251\u0252\u0005G\u0000\u0000\u0252c\u0001\u0000\u0000\u0000"+
		"\u0253\u0257\u0005\u0006\u0000\u0000\u0254\u0256\t\u0000\u0000\u0000\u0255"+
		"\u0254\u0001\u0000\u0000\u0000\u0256\u0259\u0001\u0000\u0000\u0000\u0257"+
		"\u0258\u0001\u0000\u0000\u0000\u0257\u0255\u0001\u0000\u0000\u0000\u0258"+
		"\u025a\u0001\u0000\u0000\u0000\u0259\u0257\u0001\u0000\u0000\u0000\u025a"+
		"\u025b\u0005G\u0000\u0000\u025be\u0001\u0000\u0000\u0000\u025c\u025d\u0005"+
		"\t\u0000\u0000\u025d\u0261\u0005\n\u0000\u0000\u025e\u0260\t\u0000\u0000"+
		"\u0000\u025f\u025e\u0001\u0000\u0000\u0000\u0260\u0263\u0001\u0000\u0000"+
		"\u0000\u0261\u0262\u0001\u0000\u0000\u0000\u0261\u025f\u0001\u0000\u0000"+
		"\u0000\u0262\u0264\u0001\u0000\u0000\u0000\u0263\u0261\u0001\u0000\u0000"+
		"\u0000\u0264\u0265\u0005G\u0000\u0000\u0265g\u0001\u0000\u0000\u0000\u0266"+
		"\u0267\u0003l6\u0000\u0267i\u0001\u0000\u0000\u0000\u0268\u026d\u0003"+
		"l6\u0000\u0269\u026a\u0005F\u0000\u0000\u026a\u026c\u0003l6\u0000\u026b"+
		"\u0269\u0001\u0000\u0000\u0000\u026c\u026f\u0001\u0000\u0000\u0000\u026d"+
		"\u026b\u0001\u0000\u0000\u0000\u026d\u026e\u0001\u0000\u0000\u0000\u026e"+
		"k\u0001\u0000\u0000\u0000\u026f\u026d\u0001\u0000\u0000\u0000\u0270\u0271"+
		"\u0003n7\u0000\u0271m\u0001\u0000\u0000\u0000\u0272\u0273\u00067\uffff"+
		"\uffff\u0000\u0273\u0274\u0003p8\u0000\u0274\u027d\u0001\u0000\u0000\u0000"+
		"\u0275\u0276\n\u0002\u0000\u0000\u0276\u0277\u0005$\u0000\u0000\u0277"+
		"\u027c\u0003n7\u0003\u0278\u0279\n\u0001\u0000\u0000\u0279\u027a\u0005"+
		"%\u0000\u0000\u027a\u027c\u0003n7\u0002\u027b\u0275\u0001\u0000\u0000"+
		"\u0000\u027b\u0278\u0001\u0000\u0000\u0000\u027c\u027f\u0001\u0000\u0000"+
		"\u0000\u027d\u027b\u0001\u0000\u0000\u0000\u027d\u027e\u0001\u0000\u0000"+
		"\u0000\u027eo\u0001\u0000\u0000\u0000\u027f\u027d\u0001\u0000\u0000\u0000"+
		"\u0280\u0282\u0005&\u0000\u0000\u0281\u0280\u0001\u0000\u0000\u0000\u0281"+
		"\u0282\u0001\u0000\u0000\u0000\u0282\u0283\u0001\u0000\u0000\u0000\u0283"+
		"\u0289\u0003r9\u0000\u0284\u0286\u0005#\u0000\u0000\u0285\u0287\u0005"+
		"&\u0000\u0000\u0286\u0285\u0001\u0000\u0000\u0000\u0286\u0287\u0001\u0000"+
		"\u0000\u0000\u0287\u0288\u0001\u0000\u0000\u0000\u0288\u028a\u0005\'\u0000"+
		"\u0000\u0289\u0284\u0001\u0000\u0000\u0000\u0289\u028a\u0001\u0000\u0000"+
		"\u0000\u028aq\u0001\u0000\u0000\u0000\u028b\u028c\u0003t:\u0000\u028c"+
		"s\u0001\u0000\u0000\u0000\u028d\u028e\u0006:\uffff\uffff\u0000\u028e\u028f"+
		"\u0003v;\u0000\u028f\u0296\u0001\u0000\u0000\u0000\u0290\u0291\n\u0002"+
		"\u0000\u0000\u0291\u0292\u0003x<\u0000\u0292\u0293\u0003t:\u0003\u0293"+
		"\u0295\u0001\u0000\u0000\u0000\u0294\u0290\u0001\u0000\u0000\u0000\u0295"+
		"\u0298\u0001\u0000\u0000\u0000\u0296\u0294\u0001\u0000\u0000\u0000\u0296"+
		"\u0297\u0001\u0000\u0000\u0000\u0297u\u0001\u0000\u0000\u0000\u0298\u0296"+
		"\u0001\u0000\u0000\u0000\u0299\u02a9\u0003~?\u0000\u029a\u029c\u0005&"+
		"\u0000\u0000\u029b\u029a\u0001\u0000\u0000\u0000\u029b\u029c\u0001\u0000"+
		"\u0000\u0000\u029c\u02a7\u0001\u0000\u0000\u0000\u029d\u029e\u00050\u0000"+
		"\u0000\u029e\u02a8\u0003z=\u0000\u029f\u02a0\u0005*\u0000\u0000\u02a0"+
		"\u02a8\u0003|>\u0000\u02a1\u02a2\u0007\u0003\u0000\u0000\u02a2\u02a5\u0003"+
		"~?\u0000\u02a3\u02a4\u0005/\u0000\u0000\u02a4\u02a6\u0003~?\u0000\u02a5"+
		"\u02a3\u0001\u0000\u0000\u0000\u02a5\u02a6\u0001\u0000\u0000\u0000\u02a6"+
		"\u02a8\u0001\u0000\u0000\u0000\u02a7\u029d\u0001\u0000\u0000\u0000\u02a7"+
		"\u029f\u0001\u0000\u0000\u0000\u02a7\u02a1\u0001\u0000\u0000\u0000\u02a8"+
		"\u02aa\u0001\u0000\u0000\u0000\u02a9\u029b\u0001\u0000\u0000\u0000\u02a9"+
		"\u02aa\u0001\u0000\u0000\u0000\u02aaw\u0001\u0000\u0000\u0000\u02ab\u02b8"+
		"\u0005L\u0000\u0000\u02ac\u02b8\u0005M\u0000\u0000\u02ad\u02ae\u0005S"+
		"\u0000\u0000\u02ae\u02b8\u0005T\u0000\u0000\u02af\u02b0\u0005U\u0000\u0000"+
		"\u02b0\u02b8\u0005L\u0000\u0000\u02b1\u02b2\u0005V\u0000\u0000\u02b2\u02b8"+
		"\u0005L\u0000\u0000\u02b3\u02b5\u0007\u0004\u0000\u0000\u02b4\u02b6\u0005"+
		"L\u0000\u0000\u02b5\u02b4\u0001\u0000\u0000\u0000\u02b5\u02b6\u0001\u0000"+
		"\u0000\u0000\u02b6\u02b8\u0001\u0000\u0000\u0000\u02b7\u02ab\u0001\u0000"+
		"\u0000\u0000\u02b7\u02ac\u0001\u0000\u0000\u0000\u02b7\u02ad\u0001\u0000"+
		"\u0000\u0000\u02b7\u02af\u0001\u0000\u0000\u0000\u02b7\u02b1\u0001\u0000"+
		"\u0000\u0000\u02b7\u02b3\u0001\u0000\u0000\u0000\u02b8y\u0001\u0000\u0000"+
		"\u0000\u02b9\u02ba\u0005D\u0000\u0000\u02ba\u02bf\u0003~?\u0000\u02bb"+
		"\u02bc\u0005F\u0000\u0000\u02bc\u02be\u0003~?\u0000\u02bd\u02bb\u0001"+
		"\u0000\u0000\u0000\u02be\u02c1\u0001\u0000\u0000\u0000\u02bf\u02bd\u0001"+
		"\u0000\u0000\u0000\u02bf\u02c0\u0001\u0000\u0000\u0000\u02c0\u02c2\u0001"+
		"\u0000\u0000\u0000\u02c1\u02bf\u0001\u0000\u0000\u0000\u02c2\u02c3\u0005"+
		"E\u0000\u0000\u02c3\u02c8\u0001\u0000\u0000\u0000\u02c4\u02c8\u0003\u00a2"+
		"Q\u0000\u02c5\u02c8\u0003\u009eO\u0000\u02c6\u02c8\u0003\u00a0P\u0000"+
		"\u02c7\u02b9\u0001\u0000\u0000\u0000\u02c7\u02c4\u0001\u0000\u0000\u0000"+
		"\u02c7\u02c5\u0001\u0000\u0000\u0000\u02c7\u02c6\u0001\u0000\u0000\u0000"+
		"\u02c8{\u0001\u0000\u0000\u0000\u02c9\u02ca\u0003~?\u0000\u02ca\u02cb"+
		"\u0005$\u0000\u0000\u02cb\u02cc\u0003~?\u0000\u02cc}\u0001\u0000\u0000"+
		"\u0000\u02cd\u02ce\u0006?\uffff\uffff\u0000\u02ce\u02cf\u0003\u0082A\u0000"+
		"\u02cf\u02e5\u0001\u0000\u0000\u0000\u02d0\u02d1\n\u0007\u0000\u0000\u02d1"+
		"\u02d2\u0005P\u0000\u0000\u02d2\u02e4\u0003~?\b\u02d3\u02d4\n\u0006\u0000"+
		"\u0000\u02d4\u02d5\u0005Q\u0000\u0000\u02d5\u02e4\u0003~?\u0007\u02d6"+
		"\u02d7\n\u0005\u0000\u0000\u02d7\u02d8\u0005N\u0000\u0000\u02d8\u02e4"+
		"\u0003~?\u0006\u02d9\u02da\n\u0004\u0000\u0000\u02da\u02db\u0005O\u0000"+
		"\u0000\u02db\u02e4\u0003~?\u0005\u02dc\u02dd\n\u0003\u0000\u0000\u02dd"+
		"\u02de\u0005R\u0000\u0000\u02de\u02e4\u0003~?\u0004\u02df\u02e0\n\u0002"+
		"\u0000\u0000\u02e0\u02e1\u0005R\u0000\u0000\u02e1\u02e2\u0005R\u0000\u0000"+
		"\u02e2\u02e4\u0003~?\u0003\u02e3\u02d0\u0001\u0000\u0000\u0000\u02e3\u02d3"+
		"\u0001\u0000\u0000\u0000\u02e3\u02d6\u0001\u0000\u0000\u0000\u02e3\u02d9"+
		"\u0001\u0000\u0000\u0000\u02e3\u02dc\u0001\u0000\u0000\u0000\u02e3\u02df"+
		"\u0001\u0000\u0000\u0000\u02e4\u02e7\u0001\u0000\u0000\u0000\u02e5\u02e3"+
		"\u0001\u0000\u0000\u0000\u02e5\u02e6\u0001\u0000\u0000\u0000\u02e6\u007f"+
		"\u0001\u0000\u0000\u0000\u02e7\u02e5\u0001\u0000\u0000\u0000\u02e8\u02e9"+
		"\u0007\u0005\u0000\u0000\u02e9\u02ec\u0003\u0080@\u0000\u02ea\u02ec\u0003"+
		"\u0082A\u0000\u02eb\u02e8\u0001\u0000\u0000\u0000\u02eb\u02ea\u0001\u0000"+
		"\u0000\u0000\u02ec\u0081\u0001\u0000\u0000\u0000\u02ed\u02f6\u0003\u009e"+
		"O\u0000\u02ee\u02f6\u0003\u00a2Q\u0000\u02ef\u02f6\u0003\u00a0P\u0000"+
		"\u02f0\u02f1\u0005D\u0000\u0000\u02f1\u02f2\u0003j5\u0000\u02f2\u02f3"+
		"\u0005E\u0000\u0000\u02f3\u02f6\u0001\u0000\u0000\u0000\u02f4\u02f6\u0003"+
		"\u00a6S\u0000\u02f5\u02ed\u0001\u0000\u0000\u0000\u02f5\u02ee\u0001\u0000"+
		"\u0000\u0000\u02f5\u02ef\u0001\u0000\u0000\u0000\u02f5\u02f0\u0001\u0000"+
		"\u0000\u0000\u02f5\u02f4\u0001\u0000\u0000\u0000\u02f6\u0083\u0001\u0000"+
		"\u0000\u0000\u02f7\u02f8\u0003\u00a8T\u0000\u02f8\u0085\u0001\u0000\u0000"+
		"\u0000\u02f9\u02fa\u0003\u00a8T\u0000\u02fa\u0087\u0001\u0000\u0000\u0000"+
		"\u02fb\u02fc\u0003\u00a8T\u0000\u02fc\u0089\u0001\u0000\u0000\u0000\u02fd"+
		"\u02fe\u0003\u00a8T\u0000\u02fe\u008b\u0001\u0000\u0000\u0000\u02ff\u0300"+
		"\u0003\u00a8T\u0000\u0300\u008d\u0001\u0000\u0000\u0000\u0301\u0302\u0003"+
		"\u00a8T\u0000\u0302\u008f\u0001\u0000\u0000\u0000\u0303\u0304\u0003\u00a8"+
		"T\u0000\u0304\u0091\u0001\u0000\u0000\u0000\u0305\u030e\u0005D\u0000\u0000"+
		"\u0306\u030b\u0003\u0094J\u0000\u0307\u0308\u0005F\u0000\u0000\u0308\u030a"+
		"\u0003\u0094J\u0000\u0309\u0307\u0001\u0000\u0000\u0000\u030a\u030d\u0001"+
		"\u0000\u0000\u0000\u030b\u0309\u0001\u0000\u0000\u0000\u030b\u030c\u0001"+
		"\u0000\u0000\u0000\u030c\u030f\u0001\u0000\u0000\u0000\u030d\u030b\u0001"+
		"\u0000\u0000\u0000\u030e\u0306\u0001\u0000\u0000\u0000\u030e\u030f\u0001"+
		"\u0000\u0000\u0000\u030f\u0310\u0001\u0000\u0000\u0000\u0310\u0311\u0005"+
		"E\u0000\u0000\u0311\u0093\u0001\u0000\u0000\u0000\u0312\u0313\u0003l6"+
		"\u0000\u0313\u0095\u0001\u0000\u0000\u0000\u0314\u0315\u0003\u0098L\u0000"+
		"\u0315\u0097\u0001\u0000\u0000\u0000\u0316\u0318\u0003\u009cN\u0000\u0317"+
		"\u0319\u0003\u009aM\u0000\u0318\u0317\u0001\u0000\u0000\u0000\u0318\u0319"+
		"\u0001\u0000\u0000\u0000\u0319\u0099\u0001\u0000\u0000\u0000\u031a\u031b"+
		"\u0005D\u0000\u0000\u031b\u031e\u0003\u00a4R\u0000\u031c\u031d\u0005F"+
		"\u0000\u0000\u031d\u031f\u0003\u00a4R\u0000\u031e\u031c\u0001\u0000\u0000"+
		"\u0000\u031e\u031f\u0001\u0000\u0000\u0000\u031f\u0320\u0001\u0000\u0000"+
		"\u0000\u0320\u0321\u0005E\u0000\u0000\u0321\u009b\u0001\u0000\u0000\u0000"+
		"\u0322\u0323\u0007\u0006\u0000\u0000\u0323\u009d\u0001\u0000\u0000\u0000"+
		"\u0324\u0325\u0005\\\u0000\u0000\u0325\u009f\u0001\u0000\u0000\u0000\u0326"+
		"\u032b\u0003\u00aaU\u0000\u0327\u0328\u0005I\u0000\u0000\u0328\u032a\u0003"+
		"\u00aaU\u0000\u0329\u0327\u0001\u0000\u0000\u0000\u032a\u032d\u0001\u0000"+
		"\u0000\u0000\u032b\u0329\u0001\u0000\u0000\u0000\u032b\u032c\u0001\u0000"+
		"\u0000\u0000\u032c\u00a1\u0001\u0000\u0000\u0000\u032d\u032b\u0001\u0000"+
		"\u0000\u0000\u032e\u0331\u0005A\u0000\u0000\u032f\u0332\u0003\u00a6S\u0000"+
		"\u0330\u0332\u0003\u009eO\u0000\u0331\u032f\u0001\u0000\u0000\u0000\u0331"+
		"\u0330\u0001\u0000\u0000\u0000\u0332\u033c\u0001\u0000\u0000\u0000\u0333"+
		"\u033c\u0003\u00a4R\u0000\u0334\u0335\u0005@\u0000\u0000\u0335\u033c\u0003"+
		"\u00a6S\u0000\u0336\u033c\u0003\u00a6S\u0000\u0337\u033c\u0005\'\u0000"+
		"\u0000\u0338\u033c\u0005(\u0000\u0000\u0339\u033c\u0005)\u0000\u0000\u033a"+
		"\u033c\u00053\u0000\u0000\u033b\u032e\u0001\u0000\u0000\u0000\u033b\u0333"+
		"\u0001\u0000\u0000\u0000\u033b\u0334\u0001\u0000\u0000\u0000\u033b\u0336"+
		"\u0001\u0000\u0000\u0000\u033b\u0337\u0001\u0000\u0000\u0000\u033b\u0338"+
		"\u0001\u0000\u0000\u0000\u033b\u0339\u0001\u0000\u0000\u0000\u033b\u033a"+
		"\u0001\u0000\u0000\u0000\u033c\u00a3\u0001\u0000\u0000\u0000\u033d\u033e"+
		"\u0007\u0007\u0000\u0000\u033e\u00a5\u0001\u0000\u0000\u0000\u033f\u0340"+
		"\u0007\b\u0000\u0000\u0340\u00a7\u0001\u0000\u0000\u0000\u0341\u0342\u0003"+
		"\u00aaU\u0000\u0342\u00a9\u0001\u0000\u0000\u0000\u0343\u0346\u0003\u00ac"+
		"V\u0000\u0344\u0346\u0005[\u0000\u0000\u0345\u0343\u0001\u0000\u0000\u0000"+
		"\u0345\u0344\u0001\u0000\u0000\u0000\u0346\u00ab\u0001\u0000\u0000\u0000"+
		"\u0347\u0348\u0007\t\u0000\u0000\u0348\u00ad\u0001\u0000\u0000\u0000N"+
		"\u00b0\u00b4\u00c6\u00d0\u00d5\u00db\u00de\u00ed\u00f2\u00f6\u00f9\u0103"+
		"\u0106\u0113\u0120\u0134\u0143\u0148\u0154\u015d\u0166\u0170\u0179\u0183"+
		"\u018c\u0195\u0199\u019c\u01a4\u01aa\u01af\u01b2\u01bb\u01c6\u01d7\u01db"+
		"\u01ec\u01ff\u0205\u0210\u0214\u0217\u021b\u0223\u0225\u022f\u023e\u0244"+
		"\u024e\u0257\u0261\u026d\u027b\u027d\u0281\u0286\u0289\u0296\u029b\u02a5"+
		"\u02a7\u02a9\u02b5\u02b7\u02bf\u02c7\u02e3\u02e5\u02eb\u02f5\u030b\u030e"+
		"\u0318\u031e\u032b\u0331\u033b\u0345";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}
