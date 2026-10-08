// Generated from PlSqlParser.g4 by ANTLR 4.13.1
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
		DEF=1, IF=2, ELSE=3, ELSIF=4, ELSEIF=5, WHILE=6, FOR=7, IN=8, TRY=9, CATCH=10, 
		BREAK=11, CONTINUE=12, RAISE=13, SET=14, START=15, END=16, TRANSACTION=17, 
		COMMIT=18, ROLLBACK=19, SELECT=20, INSERT=21, UPDATE=22, DELETE=23, MERGE=24, 
		TRUNCATE=25, CREATE=26, DROP=27, ALTER=28, WITH=29, AND=30, OR=31, NOT=32, 
		IS=33, NULL_=34, TRUE=35, FALSE=36, BETWEEN=37, LIKE=38, ESCAPE=39, NULL_SAFE_EQUALS=40, 
		NOT_EQUAL_OP=41, LESS_THAN_OP=42, GREATER_THAN_OP=43, EQUALS_OP=44, PLUS_SIGN=45, 
		MINUS_SIGN=46, ASTERISK=47, SOLIDUS=48, PERCENT=49, CONCAT=50, BAR=51, 
		AMPERSAND=52, CARET=53, LEFT_PAREN=54, RIGHT_PAREN=55, LEFT_BRACE=56, 
		RIGHT_BRACE=57, LEFT_BRACKET=58, RIGHT_BRACKET=59, COMMA=60, SEMICOLON=61, 
		PERIOD=62, DOUBLE_PERIOD=63, CHAR_STRING=64, NATIONAL_CHAR_STRING_LIT=65, 
		UNSIGNED_INTEGER=66, APPROXIMATE_NUM_LIT=67, DELIMITED_ID=68, BACKTICK_ID=69, 
		BINDVAR=70, REGULAR_ID=71, SINGLE_LINE_COMMENT=72, MULTI_LINE_COMMENT=73, 
		SPACES=74;
	public static final int
		RULE_sql_script = 0, RULE_statement = 1, RULE_terminated_statement = 2, 
		RULE_body = 3, RULE_def_statement = 4, RULE_assignment_statement = 5, 
		RULE_call_statement = 6, RULE_set_statement = 7, RULE_transaction_statement = 8, 
		RULE_break_statement = 9, RULE_continue_statement = 10, RULE_raise_statement = 11, 
		RULE_if_statement = 12, RULE_elsif_clause = 13, RULE_else_clause = 14, 
		RULE_while_statement = 15, RULE_for_statement = 16, RULE_for_source = 17, 
		RULE_for_query = 18, RULE_for_query_item = 19, RULE_try_statement = 20, 
		RULE_sql_statement = 21, RULE_sql_prefix = 22, RULE_raw_token = 23, RULE_expression = 24, 
		RULE_or_expression = 25, RULE_and_expression = 26, RULE_not_expression = 27, 
		RULE_predicate = 28, RULE_predicate_tail = 29, RULE_relational_operator = 30, 
		RULE_concatenation = 31, RULE_additive = 32, RULE_multiplicative = 33, 
		RULE_unary = 34, RULE_atom = 35, RULE_func_args = 36, RULE_func_arg = 37, 
		RULE_func_top = 38, RULE_func_nested = 39, RULE_literal = 40, RULE_numeric = 41, 
		RULE_identifier = 42;
	private static String[] makeRuleNames() {
		return new String[] {
			"sql_script", "statement", "terminated_statement", "body", "def_statement", 
			"assignment_statement", "call_statement", "set_statement", "transaction_statement", 
			"break_statement", "continue_statement", "raise_statement", "if_statement", 
			"elsif_clause", "else_clause", "while_statement", "for_statement", "for_source", 
			"for_query", "for_query_item", "try_statement", "sql_statement", "sql_prefix", 
			"raw_token", "expression", "or_expression", "and_expression", "not_expression", 
			"predicate", "predicate_tail", "relational_operator", "concatenation", 
			"additive", "multiplicative", "unary", "atom", "func_args", "func_arg", 
			"func_top", "func_nested", "literal", "numeric", "identifier"
		};
	}
	public static final String[] ruleNames = makeRuleNames();

	private static String[] makeLiteralNames() {
		return new String[] {
			null, "'DEF'", "'IF'", "'ELSE'", "'ELSIF'", "'ELSEIF'", "'WHILE'", "'FOR'", 
			"'IN'", "'TRY'", "'CATCH'", "'BREAK'", "'CONTINUE'", "'RAISE'", "'SET'", 
			"'START'", "'END'", "'TRANSACTION'", "'COMMIT'", "'ROLLBACK'", "'SELECT'", 
			"'INSERT'", "'UPDATE'", "'DELETE'", "'MERGE'", "'TRUNCATE'", "'CREATE'", 
			"'DROP'", "'ALTER'", "'WITH'", "'AND'", "'OR'", "'NOT'", "'IS'", "'NULL'", 
			"'TRUE'", "'FALSE'", "'BETWEEN'", "'LIKE'", "'ESCAPE'", "'<=>'", null, 
			"'<'", "'>'", "'='", "'+'", "'-'", "'*'", "'/'", "'%'", "'||'", "'|'", 
			"'&'", "'^'", "'('", "')'", "'{'", "'}'", "'['", "']'", "','", "';'", 
			"'.'", "'..'"
		};
	}
	private static final String[] _LITERAL_NAMES = makeLiteralNames();
	private static String[] makeSymbolicNames() {
		return new String[] {
			null, "DEF", "IF", "ELSE", "ELSIF", "ELSEIF", "WHILE", "FOR", "IN", "TRY", 
			"CATCH", "BREAK", "CONTINUE", "RAISE", "SET", "START", "END", "TRANSACTION", 
			"COMMIT", "ROLLBACK", "SELECT", "INSERT", "UPDATE", "DELETE", "MERGE", 
			"TRUNCATE", "CREATE", "DROP", "ALTER", "WITH", "AND", "OR", "NOT", "IS", 
			"NULL_", "TRUE", "FALSE", "BETWEEN", "LIKE", "ESCAPE", "NULL_SAFE_EQUALS", 
			"NOT_EQUAL_OP", "LESS_THAN_OP", "GREATER_THAN_OP", "EQUALS_OP", "PLUS_SIGN", 
			"MINUS_SIGN", "ASTERISK", "SOLIDUS", "PERCENT", "CONCAT", "BAR", "AMPERSAND", 
			"CARET", "LEFT_PAREN", "RIGHT_PAREN", "LEFT_BRACE", "RIGHT_BRACE", "LEFT_BRACKET", 
			"RIGHT_BRACKET", "COMMA", "SEMICOLON", "PERIOD", "DOUBLE_PERIOD", "CHAR_STRING", 
			"NATIONAL_CHAR_STRING_LIT", "UNSIGNED_INTEGER", "APPROXIMATE_NUM_LIT", 
			"DELIMITED_ID", "BACKTICK_ID", "BINDVAR", "REGULAR_ID", "SINGLE_LINE_COMMENT", 
			"MULTI_LINE_COMMENT", "SPACES"
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
		public List<StatementContext> statement() {
			return getRuleContexts(StatementContext.class);
		}
		public StatementContext statement(int i) {
			return getRuleContext(StatementContext.class,i);
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
			setState(89);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while ((((_la) & ~0x3f) == 0 && ((1L << _la) & 1073609414L) != 0) || ((((_la - 68)) & ~0x3f) == 0 && ((1L << (_la - 68)) & 11L) != 0)) {
				{
				{
				setState(86);
				statement();
				}
				}
				setState(91);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(92);
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
	public static class StatementContext extends ParserRuleContext {
		public Terminated_statementContext terminated_statement() {
			return getRuleContext(Terminated_statementContext.class,0);
		}
		public If_statementContext if_statement() {
			return getRuleContext(If_statementContext.class,0);
		}
		public While_statementContext while_statement() {
			return getRuleContext(While_statementContext.class,0);
		}
		public For_statementContext for_statement() {
			return getRuleContext(For_statementContext.class,0);
		}
		public Try_statementContext try_statement() {
			return getRuleContext(Try_statementContext.class,0);
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
		enterRule(_localctx, 2, RULE_statement);
		try {
			setState(99);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case DEF:
			case BREAK:
			case CONTINUE:
			case RAISE:
			case SET:
			case START:
			case END:
			case COMMIT:
			case ROLLBACK:
			case SELECT:
			case INSERT:
			case UPDATE:
			case DELETE:
			case MERGE:
			case TRUNCATE:
			case CREATE:
			case DROP:
			case ALTER:
			case WITH:
			case DELIMITED_ID:
			case BACKTICK_ID:
			case REGULAR_ID:
				enterOuterAlt(_localctx, 1);
				{
				setState(94);
				terminated_statement();
				}
				break;
			case IF:
				enterOuterAlt(_localctx, 2);
				{
				setState(95);
				if_statement();
				}
				break;
			case WHILE:
				enterOuterAlt(_localctx, 3);
				{
				setState(96);
				while_statement();
				}
				break;
			case FOR:
				enterOuterAlt(_localctx, 4);
				{
				setState(97);
				for_statement();
				}
				break;
			case TRY:
				enterOuterAlt(_localctx, 5);
				{
				setState(98);
				try_statement();
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
	public static class Terminated_statementContext extends ParserRuleContext {
		public TerminalNode SEMICOLON() { return getToken(PlSqlParser.SEMICOLON, 0); }
		public Def_statementContext def_statement() {
			return getRuleContext(Def_statementContext.class,0);
		}
		public Assignment_statementContext assignment_statement() {
			return getRuleContext(Assignment_statementContext.class,0);
		}
		public Call_statementContext call_statement() {
			return getRuleContext(Call_statementContext.class,0);
		}
		public Transaction_statementContext transaction_statement() {
			return getRuleContext(Transaction_statementContext.class,0);
		}
		public Break_statementContext break_statement() {
			return getRuleContext(Break_statementContext.class,0);
		}
		public Continue_statementContext continue_statement() {
			return getRuleContext(Continue_statementContext.class,0);
		}
		public Raise_statementContext raise_statement() {
			return getRuleContext(Raise_statementContext.class,0);
		}
		public Set_statementContext set_statement() {
			return getRuleContext(Set_statementContext.class,0);
		}
		public Sql_statementContext sql_statement() {
			return getRuleContext(Sql_statementContext.class,0);
		}
		public Terminated_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_terminated_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterTerminated_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitTerminated_statement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitTerminated_statement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Terminated_statementContext terminated_statement() throws RecognitionException {
		Terminated_statementContext _localctx = new Terminated_statementContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_terminated_statement);
		try {
			setState(114);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case DEF:
			case BREAK:
			case CONTINUE:
			case RAISE:
			case START:
			case END:
			case COMMIT:
			case ROLLBACK:
			case DELIMITED_ID:
			case BACKTICK_ID:
			case REGULAR_ID:
				enterOuterAlt(_localctx, 1);
				{
				setState(108);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,2,_ctx) ) {
				case 1:
					{
					setState(101);
					def_statement();
					}
					break;
				case 2:
					{
					setState(102);
					assignment_statement();
					}
					break;
				case 3:
					{
					setState(103);
					call_statement();
					}
					break;
				case 4:
					{
					setState(104);
					transaction_statement();
					}
					break;
				case 5:
					{
					setState(105);
					break_statement();
					}
					break;
				case 6:
					{
					setState(106);
					continue_statement();
					}
					break;
				case 7:
					{
					setState(107);
					raise_statement();
					}
					break;
				}
				setState(110);
				match(SEMICOLON);
				}
				break;
			case SET:
				enterOuterAlt(_localctx, 2);
				{
				setState(112);
				set_statement();
				}
				break;
			case SELECT:
			case INSERT:
			case UPDATE:
			case DELETE:
			case MERGE:
			case TRUNCATE:
			case CREATE:
			case DROP:
			case ALTER:
			case WITH:
				enterOuterAlt(_localctx, 3);
				{
				setState(113);
				sql_statement();
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
	public static class BodyContext extends ParserRuleContext {
		public List<StatementContext> statement() {
			return getRuleContexts(StatementContext.class);
		}
		public StatementContext statement(int i) {
			return getRuleContext(StatementContext.class,i);
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
		enterRule(_localctx, 6, RULE_body);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(120);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,4,_ctx);
			while ( _alt!=2 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(116);
					if (!( _input.LA(1) != END || _input.LA(2) == TRANSACTION )) throw new FailedPredicateException(this, " _input.LA(1) != END || _input.LA(2) == TRANSACTION ");
					setState(117);
					statement();
					}
					} 
				}
				setState(122);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,4,_ctx);
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
	public static class Def_statementContext extends ParserRuleContext {
		public TerminalNode DEF() { return getToken(PlSqlParser.DEF, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode EQUALS_OP() { return getToken(PlSqlParser.EQUALS_OP, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public Def_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_def_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterDef_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitDef_statement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitDef_statement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Def_statementContext def_statement() throws RecognitionException {
		Def_statementContext _localctx = new Def_statementContext(_ctx, getState());
		enterRule(_localctx, 8, RULE_def_statement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(123);
			match(DEF);
			setState(124);
			identifier();
			setState(127);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==EQUALS_OP) {
				{
				setState(125);
				match(EQUALS_OP);
				setState(126);
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
	public static class Assignment_statementContext extends ParserRuleContext {
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public TerminalNode EQUALS_OP() { return getToken(PlSqlParser.EQUALS_OP, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public List<TerminalNode> PERIOD() { return getTokens(PlSqlParser.PERIOD); }
		public TerminalNode PERIOD(int i) {
			return getToken(PlSqlParser.PERIOD, i);
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
		enterRule(_localctx, 10, RULE_assignment_statement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(129);
			identifier();
			setState(134);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==PERIOD) {
				{
				{
				setState(130);
				match(PERIOD);
				setState(131);
				identifier();
				}
				}
				setState(136);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(137);
			match(EQUALS_OP);
			setState(138);
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
	public static class Call_statementContext extends ParserRuleContext {
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public TerminalNode LEFT_PAREN() { return getToken(PlSqlParser.LEFT_PAREN, 0); }
		public TerminalNode RIGHT_PAREN() { return getToken(PlSqlParser.RIGHT_PAREN, 0); }
		public List<TerminalNode> PERIOD() { return getTokens(PlSqlParser.PERIOD); }
		public TerminalNode PERIOD(int i) {
			return getToken(PlSqlParser.PERIOD, i);
		}
		public Func_argsContext func_args() {
			return getRuleContext(Func_argsContext.class,0);
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
		enterRule(_localctx, 12, RULE_call_statement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(140);
			identifier();
			setState(145);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==PERIOD) {
				{
				{
				setState(141);
				match(PERIOD);
				setState(142);
				identifier();
				}
				}
				setState(147);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(148);
			match(LEFT_PAREN);
			setState(150);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & -1188950301625810946L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & 2047L) != 0)) {
				{
				setState(149);
				func_args();
				}
			}

			setState(152);
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
	public static class Set_statementContext extends ParserRuleContext {
		public TerminalNode SET() { return getToken(PlSqlParser.SET, 0); }
		public TerminalNode SEMICOLON() { return getToken(PlSqlParser.SEMICOLON, 0); }
		public List<Raw_tokenContext> raw_token() {
			return getRuleContexts(Raw_tokenContext.class);
		}
		public Raw_tokenContext raw_token(int i) {
			return getRuleContext(Raw_tokenContext.class,i);
		}
		public Set_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_set_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterSet_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitSet_statement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitSet_statement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Set_statementContext set_statement() throws RecognitionException {
		Set_statementContext _localctx = new Set_statementContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_set_statement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(154);
			match(SET);
			setState(158);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while ((((_la) & ~0x3f) == 0 && ((1L << _la) & -2305843009213693954L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & 2047L) != 0)) {
				{
				{
				setState(155);
				raw_token();
				}
				}
				setState(160);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(161);
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
	public static class Transaction_statementContext extends ParserRuleContext {
		public TerminalNode START() { return getToken(PlSqlParser.START, 0); }
		public TerminalNode TRANSACTION() { return getToken(PlSqlParser.TRANSACTION, 0); }
		public TerminalNode END() { return getToken(PlSqlParser.END, 0); }
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
		enterRule(_localctx, 16, RULE_transaction_statement);
		try {
			setState(169);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case START:
				enterOuterAlt(_localctx, 1);
				{
				setState(163);
				match(START);
				setState(164);
				match(TRANSACTION);
				}
				break;
			case END:
				enterOuterAlt(_localctx, 2);
				{
				setState(165);
				match(END);
				setState(166);
				match(TRANSACTION);
				}
				break;
			case COMMIT:
				enterOuterAlt(_localctx, 3);
				{
				setState(167);
				match(COMMIT);
				}
				break;
			case ROLLBACK:
				enterOuterAlt(_localctx, 4);
				{
				setState(168);
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
	public static class Break_statementContext extends ParserRuleContext {
		public TerminalNode BREAK() { return getToken(PlSqlParser.BREAK, 0); }
		public Break_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_break_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterBreak_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitBreak_statement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitBreak_statement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Break_statementContext break_statement() throws RecognitionException {
		Break_statementContext _localctx = new Break_statementContext(_ctx, getState());
		enterRule(_localctx, 18, RULE_break_statement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(171);
			match(BREAK);
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
		enterRule(_localctx, 20, RULE_continue_statement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(173);
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
	public static class Raise_statementContext extends ParserRuleContext {
		public TerminalNode RAISE() { return getToken(PlSqlParser.RAISE, 0); }
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
		enterRule(_localctx, 22, RULE_raise_statement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(175);
			match(RAISE);
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
		public TerminalNode IF() { return getToken(PlSqlParser.IF, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public BodyContext body() {
			return getRuleContext(BodyContext.class,0);
		}
		public TerminalNode END() { return getToken(PlSqlParser.END, 0); }
		public List<Elsif_clauseContext> elsif_clause() {
			return getRuleContexts(Elsif_clauseContext.class);
		}
		public Elsif_clauseContext elsif_clause(int i) {
			return getRuleContext(Elsif_clauseContext.class,i);
		}
		public Else_clauseContext else_clause() {
			return getRuleContext(Else_clauseContext.class,0);
		}
		public TerminalNode SEMICOLON() { return getToken(PlSqlParser.SEMICOLON, 0); }
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
		enterRule(_localctx, 24, RULE_if_statement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(177);
			match(IF);
			setState(178);
			expression();
			setState(179);
			body();
			setState(183);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==ELSIF || _la==ELSEIF) {
				{
				{
				setState(180);
				elsif_clause();
				}
				}
				setState(185);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(187);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ELSE) {
				{
				setState(186);
				else_clause();
				}
			}

			setState(189);
			match(END);
			setState(191);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,13,_ctx) ) {
			case 1:
				{
				setState(190);
				match(SEMICOLON);
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
	public static class Elsif_clauseContext extends ParserRuleContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public BodyContext body() {
			return getRuleContext(BodyContext.class,0);
		}
		public TerminalNode ELSIF() { return getToken(PlSqlParser.ELSIF, 0); }
		public TerminalNode ELSEIF() { return getToken(PlSqlParser.ELSEIF, 0); }
		public Elsif_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_elsif_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterElsif_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitElsif_clause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitElsif_clause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Elsif_clauseContext elsif_clause() throws RecognitionException {
		Elsif_clauseContext _localctx = new Elsif_clauseContext(_ctx, getState());
		enterRule(_localctx, 26, RULE_elsif_clause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(193);
			_la = _input.LA(1);
			if ( !(_la==ELSIF || _la==ELSEIF) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			setState(194);
			expression();
			setState(195);
			body();
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
	public static class Else_clauseContext extends ParserRuleContext {
		public TerminalNode ELSE() { return getToken(PlSqlParser.ELSE, 0); }
		public BodyContext body() {
			return getRuleContext(BodyContext.class,0);
		}
		public Else_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_else_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterElse_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitElse_clause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitElse_clause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Else_clauseContext else_clause() throws RecognitionException {
		Else_clauseContext _localctx = new Else_clauseContext(_ctx, getState());
		enterRule(_localctx, 28, RULE_else_clause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(197);
			match(ELSE);
			setState(198);
			body();
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
	public static class While_statementContext extends ParserRuleContext {
		public TerminalNode WHILE() { return getToken(PlSqlParser.WHILE, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public BodyContext body() {
			return getRuleContext(BodyContext.class,0);
		}
		public TerminalNode END() { return getToken(PlSqlParser.END, 0); }
		public TerminalNode SEMICOLON() { return getToken(PlSqlParser.SEMICOLON, 0); }
		public While_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_while_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterWhile_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitWhile_statement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitWhile_statement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final While_statementContext while_statement() throws RecognitionException {
		While_statementContext _localctx = new While_statementContext(_ctx, getState());
		enterRule(_localctx, 30, RULE_while_statement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(200);
			match(WHILE);
			setState(201);
			expression();
			setState(202);
			body();
			setState(203);
			match(END);
			setState(205);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,14,_ctx) ) {
			case 1:
				{
				setState(204);
				match(SEMICOLON);
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
	public static class For_statementContext extends ParserRuleContext {
		public TerminalNode FOR() { return getToken(PlSqlParser.FOR, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode IN() { return getToken(PlSqlParser.IN, 0); }
		public For_sourceContext for_source() {
			return getRuleContext(For_sourceContext.class,0);
		}
		public BodyContext body() {
			return getRuleContext(BodyContext.class,0);
		}
		public TerminalNode END() { return getToken(PlSqlParser.END, 0); }
		public TerminalNode SEMICOLON() { return getToken(PlSqlParser.SEMICOLON, 0); }
		public For_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_for_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterFor_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitFor_statement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitFor_statement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final For_statementContext for_statement() throws RecognitionException {
		For_statementContext _localctx = new For_statementContext(_ctx, getState());
		enterRule(_localctx, 32, RULE_for_statement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(207);
			match(FOR);
			setState(208);
			identifier();
			setState(209);
			match(IN);
			setState(210);
			for_source();
			setState(211);
			body();
			setState(212);
			match(END);
			setState(214);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,15,_ctx) ) {
			case 1:
				{
				setState(213);
				match(SEMICOLON);
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
	public static class For_sourceContext extends ParserRuleContext {
		public For_queryContext for_query() {
			return getRuleContext(For_queryContext.class,0);
		}
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode DOUBLE_PERIOD() { return getToken(PlSqlParser.DOUBLE_PERIOD, 0); }
		public For_sourceContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_for_source; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterFor_source(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitFor_source(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitFor_source(this);
			else return visitor.visitChildren(this);
		}
	}

	public final For_sourceContext for_source() throws RecognitionException {
		For_sourceContext _localctx = new For_sourceContext(_ctx, getState());
		enterRule(_localctx, 34, RULE_for_source);
		try {
			setState(221);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,16,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(216);
				for_query();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(217);
				expression();
				setState(218);
				match(DOUBLE_PERIOD);
				setState(219);
				expression();
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
	public static class For_queryContext extends ParserRuleContext {
		public TerminalNode LEFT_PAREN() { return getToken(PlSqlParser.LEFT_PAREN, 0); }
		public TerminalNode RIGHT_PAREN() { return getToken(PlSqlParser.RIGHT_PAREN, 0); }
		public TerminalNode SELECT() { return getToken(PlSqlParser.SELECT, 0); }
		public TerminalNode WITH() { return getToken(PlSqlParser.WITH, 0); }
		public List<For_query_itemContext> for_query_item() {
			return getRuleContexts(For_query_itemContext.class);
		}
		public For_query_itemContext for_query_item(int i) {
			return getRuleContext(For_query_itemContext.class,i);
		}
		public For_queryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_for_query; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterFor_query(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitFor_query(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitFor_query(this);
			else return visitor.visitChildren(this);
		}
	}

	public final For_queryContext for_query() throws RecognitionException {
		For_queryContext _localctx = new For_queryContext(_ctx, getState());
		enterRule(_localctx, 36, RULE_for_query);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(223);
			match(LEFT_PAREN);
			setState(224);
			_la = _input.LA(1);
			if ( !(_la==SELECT || _la==WITH) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			setState(228);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while ((((_la) & ~0x3f) == 0 && ((1L << _la) & -36028797018963970L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & 2047L) != 0)) {
				{
				{
				setState(225);
				for_query_item();
				}
				}
				setState(230);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(231);
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
	public static class For_query_itemContext extends ParserRuleContext {
		public TerminalNode LEFT_PAREN() { return getToken(PlSqlParser.LEFT_PAREN, 0); }
		public TerminalNode RIGHT_PAREN() { return getToken(PlSqlParser.RIGHT_PAREN, 0); }
		public List<For_query_itemContext> for_query_item() {
			return getRuleContexts(For_query_itemContext.class);
		}
		public For_query_itemContext for_query_item(int i) {
			return getRuleContext(For_query_itemContext.class,i);
		}
		public For_query_itemContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_for_query_item; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterFor_query_item(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitFor_query_item(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitFor_query_item(this);
			else return visitor.visitChildren(this);
		}
	}

	public final For_query_itemContext for_query_item() throws RecognitionException {
		For_query_itemContext _localctx = new For_query_itemContext(_ctx, getState());
		enterRule(_localctx, 38, RULE_for_query_item);
		int _la;
		try {
			setState(242);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case LEFT_PAREN:
				enterOuterAlt(_localctx, 1);
				{
				setState(233);
				match(LEFT_PAREN);
				setState(237);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while ((((_la) & ~0x3f) == 0 && ((1L << _la) & -36028797018963970L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & 2047L) != 0)) {
					{
					{
					setState(234);
					for_query_item();
					}
					}
					setState(239);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(240);
				match(RIGHT_PAREN);
				}
				break;
			case DEF:
			case IF:
			case ELSE:
			case ELSIF:
			case ELSEIF:
			case WHILE:
			case FOR:
			case IN:
			case TRY:
			case CATCH:
			case BREAK:
			case CONTINUE:
			case RAISE:
			case SET:
			case START:
			case END:
			case TRANSACTION:
			case COMMIT:
			case ROLLBACK:
			case SELECT:
			case INSERT:
			case UPDATE:
			case DELETE:
			case MERGE:
			case TRUNCATE:
			case CREATE:
			case DROP:
			case ALTER:
			case WITH:
			case AND:
			case OR:
			case NOT:
			case IS:
			case NULL_:
			case TRUE:
			case FALSE:
			case BETWEEN:
			case LIKE:
			case ESCAPE:
			case NULL_SAFE_EQUALS:
			case NOT_EQUAL_OP:
			case LESS_THAN_OP:
			case GREATER_THAN_OP:
			case EQUALS_OP:
			case PLUS_SIGN:
			case MINUS_SIGN:
			case ASTERISK:
			case SOLIDUS:
			case PERCENT:
			case CONCAT:
			case BAR:
			case AMPERSAND:
			case CARET:
			case LEFT_BRACE:
			case RIGHT_BRACE:
			case LEFT_BRACKET:
			case RIGHT_BRACKET:
			case COMMA:
			case SEMICOLON:
			case PERIOD:
			case DOUBLE_PERIOD:
			case CHAR_STRING:
			case NATIONAL_CHAR_STRING_LIT:
			case UNSIGNED_INTEGER:
			case APPROXIMATE_NUM_LIT:
			case DELIMITED_ID:
			case BACKTICK_ID:
			case BINDVAR:
			case REGULAR_ID:
			case SINGLE_LINE_COMMENT:
			case MULTI_LINE_COMMENT:
			case SPACES:
				enterOuterAlt(_localctx, 2);
				{
				setState(241);
				_la = _input.LA(1);
				if ( _la <= 0 || (_la==LEFT_PAREN || _la==RIGHT_PAREN) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
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
	public static class Try_statementContext extends ParserRuleContext {
		public TerminalNode TRY() { return getToken(PlSqlParser.TRY, 0); }
		public List<BodyContext> body() {
			return getRuleContexts(BodyContext.class);
		}
		public BodyContext body(int i) {
			return getRuleContext(BodyContext.class,i);
		}
		public TerminalNode CATCH() { return getToken(PlSqlParser.CATCH, 0); }
		public TerminalNode END() { return getToken(PlSqlParser.END, 0); }
		public TerminalNode SEMICOLON() { return getToken(PlSqlParser.SEMICOLON, 0); }
		public Try_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_try_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterTry_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitTry_statement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitTry_statement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Try_statementContext try_statement() throws RecognitionException {
		Try_statementContext _localctx = new Try_statementContext(_ctx, getState());
		enterRule(_localctx, 40, RULE_try_statement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(244);
			match(TRY);
			setState(245);
			body();
			setState(246);
			match(CATCH);
			setState(247);
			body();
			setState(248);
			match(END);
			setState(250);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,20,_ctx) ) {
			case 1:
				{
				setState(249);
				match(SEMICOLON);
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
	public static class Sql_statementContext extends ParserRuleContext {
		public Sql_prefixContext sql_prefix() {
			return getRuleContext(Sql_prefixContext.class,0);
		}
		public TerminalNode SEMICOLON() { return getToken(PlSqlParser.SEMICOLON, 0); }
		public List<Raw_tokenContext> raw_token() {
			return getRuleContexts(Raw_tokenContext.class);
		}
		public Raw_tokenContext raw_token(int i) {
			return getRuleContext(Raw_tokenContext.class,i);
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
		enterRule(_localctx, 42, RULE_sql_statement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(252);
			sql_prefix();
			setState(256);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while ((((_la) & ~0x3f) == 0 && ((1L << _la) & -2305843009213693954L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & 2047L) != 0)) {
				{
				{
				setState(253);
				raw_token();
				}
				}
				setState(258);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(259);
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
	public static class Sql_prefixContext extends ParserRuleContext {
		public TerminalNode SELECT() { return getToken(PlSqlParser.SELECT, 0); }
		public TerminalNode INSERT() { return getToken(PlSqlParser.INSERT, 0); }
		public TerminalNode UPDATE() { return getToken(PlSqlParser.UPDATE, 0); }
		public TerminalNode DELETE() { return getToken(PlSqlParser.DELETE, 0); }
		public TerminalNode MERGE() { return getToken(PlSqlParser.MERGE, 0); }
		public TerminalNode TRUNCATE() { return getToken(PlSqlParser.TRUNCATE, 0); }
		public TerminalNode CREATE() { return getToken(PlSqlParser.CREATE, 0); }
		public TerminalNode DROP() { return getToken(PlSqlParser.DROP, 0); }
		public TerminalNode ALTER() { return getToken(PlSqlParser.ALTER, 0); }
		public TerminalNode WITH() { return getToken(PlSqlParser.WITH, 0); }
		public Sql_prefixContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sql_prefix; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterSql_prefix(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitSql_prefix(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitSql_prefix(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Sql_prefixContext sql_prefix() throws RecognitionException {
		Sql_prefixContext _localctx = new Sql_prefixContext(_ctx, getState());
		enterRule(_localctx, 44, RULE_sql_prefix);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(261);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & 1072693248L) != 0)) ) {
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
	public static class Raw_tokenContext extends ParserRuleContext {
		public TerminalNode SEMICOLON() { return getToken(PlSqlParser.SEMICOLON, 0); }
		public Raw_tokenContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_raw_token; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterRaw_token(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitRaw_token(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitRaw_token(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Raw_tokenContext raw_token() throws RecognitionException {
		Raw_tokenContext _localctx = new Raw_tokenContext(_ctx, getState());
		enterRule(_localctx, 46, RULE_raw_token);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(263);
			_la = _input.LA(1);
			if ( _la <= 0 || (_la==SEMICOLON) ) {
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
	public static class ExpressionContext extends ParserRuleContext {
		public Or_expressionContext or_expression() {
			return getRuleContext(Or_expressionContext.class,0);
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
		enterRule(_localctx, 48, RULE_expression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(265);
			or_expression();
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
	public static class Or_expressionContext extends ParserRuleContext {
		public List<And_expressionContext> and_expression() {
			return getRuleContexts(And_expressionContext.class);
		}
		public And_expressionContext and_expression(int i) {
			return getRuleContext(And_expressionContext.class,i);
		}
		public List<TerminalNode> OR() { return getTokens(PlSqlParser.OR); }
		public TerminalNode OR(int i) {
			return getToken(PlSqlParser.OR, i);
		}
		public Or_expressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_or_expression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterOr_expression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitOr_expression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitOr_expression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Or_expressionContext or_expression() throws RecognitionException {
		Or_expressionContext _localctx = new Or_expressionContext(_ctx, getState());
		enterRule(_localctx, 50, RULE_or_expression);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(267);
			and_expression();
			setState(272);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,22,_ctx);
			while ( _alt!=2 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(268);
					match(OR);
					setState(269);
					and_expression();
					}
					} 
				}
				setState(274);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,22,_ctx);
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
	public static class And_expressionContext extends ParserRuleContext {
		public List<Not_expressionContext> not_expression() {
			return getRuleContexts(Not_expressionContext.class);
		}
		public Not_expressionContext not_expression(int i) {
			return getRuleContext(Not_expressionContext.class,i);
		}
		public List<TerminalNode> AND() { return getTokens(PlSqlParser.AND); }
		public TerminalNode AND(int i) {
			return getToken(PlSqlParser.AND, i);
		}
		public And_expressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_and_expression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterAnd_expression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitAnd_expression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitAnd_expression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final And_expressionContext and_expression() throws RecognitionException {
		And_expressionContext _localctx = new And_expressionContext(_ctx, getState());
		enterRule(_localctx, 52, RULE_and_expression);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(275);
			not_expression();
			setState(280);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,23,_ctx);
			while ( _alt!=2 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(276);
					match(AND);
					setState(277);
					not_expression();
					}
					} 
				}
				setState(282);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,23,_ctx);
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
	public static class Not_expressionContext extends ParserRuleContext {
		public TerminalNode NOT() { return getToken(PlSqlParser.NOT, 0); }
		public Not_expressionContext not_expression() {
			return getRuleContext(Not_expressionContext.class,0);
		}
		public PredicateContext predicate() {
			return getRuleContext(PredicateContext.class,0);
		}
		public Not_expressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_not_expression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterNot_expression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitNot_expression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitNot_expression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Not_expressionContext not_expression() throws RecognitionException {
		Not_expressionContext _localctx = new Not_expressionContext(_ctx, getState());
		enterRule(_localctx, 54, RULE_not_expression);
		try {
			setState(286);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case NOT:
				enterOuterAlt(_localctx, 1);
				{
				setState(283);
				match(NOT);
				setState(284);
				not_expression();
				}
				break;
			case NULL_:
			case TRUE:
			case FALSE:
			case PLUS_SIGN:
			case MINUS_SIGN:
			case LEFT_PAREN:
			case CHAR_STRING:
			case NATIONAL_CHAR_STRING_LIT:
			case UNSIGNED_INTEGER:
			case APPROXIMATE_NUM_LIT:
			case DELIMITED_ID:
			case BACKTICK_ID:
			case BINDVAR:
			case REGULAR_ID:
				enterOuterAlt(_localctx, 2);
				{
				setState(285);
				predicate();
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
	public static class PredicateContext extends ParserRuleContext {
		public ConcatenationContext concatenation() {
			return getRuleContext(ConcatenationContext.class,0);
		}
		public Predicate_tailContext predicate_tail() {
			return getRuleContext(Predicate_tailContext.class,0);
		}
		public PredicateContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_predicate; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterPredicate(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitPredicate(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitPredicate(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PredicateContext predicate() throws RecognitionException {
		PredicateContext _localctx = new PredicateContext(_ctx, getState());
		enterRule(_localctx, 56, RULE_predicate);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(288);
			concatenation();
			setState(290);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,25,_ctx) ) {
			case 1:
				{
				setState(289);
				predicate_tail();
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
	public static class Predicate_tailContext extends ParserRuleContext {
		public Relational_operatorContext relational_operator() {
			return getRuleContext(Relational_operatorContext.class,0);
		}
		public List<ConcatenationContext> concatenation() {
			return getRuleContexts(ConcatenationContext.class);
		}
		public ConcatenationContext concatenation(int i) {
			return getRuleContext(ConcatenationContext.class,i);
		}
		public TerminalNode IN() { return getToken(PlSqlParser.IN, 0); }
		public TerminalNode LEFT_PAREN() { return getToken(PlSqlParser.LEFT_PAREN, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode RIGHT_PAREN() { return getToken(PlSqlParser.RIGHT_PAREN, 0); }
		public TerminalNode NOT() { return getToken(PlSqlParser.NOT, 0); }
		public List<TerminalNode> COMMA() { return getTokens(PlSqlParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(PlSqlParser.COMMA, i);
		}
		public TerminalNode BETWEEN() { return getToken(PlSqlParser.BETWEEN, 0); }
		public TerminalNode AND() { return getToken(PlSqlParser.AND, 0); }
		public TerminalNode LIKE() { return getToken(PlSqlParser.LIKE, 0); }
		public TerminalNode ESCAPE() { return getToken(PlSqlParser.ESCAPE, 0); }
		public TerminalNode IS() { return getToken(PlSqlParser.IS, 0); }
		public TerminalNode NULL_() { return getToken(PlSqlParser.NULL_, 0); }
		public Predicate_tailContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_predicate_tail; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterPredicate_tail(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitPredicate_tail(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitPredicate_tail(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Predicate_tailContext predicate_tail() throws RecognitionException {
		Predicate_tailContext _localctx = new Predicate_tailContext(_ctx, getState());
		enterRule(_localctx, 58, RULE_predicate_tail);
		int _la;
		try {
			setState(332);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,32,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(292);
				relational_operator();
				setState(293);
				concatenation();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(296);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(295);
					match(NOT);
					}
				}

				setState(298);
				match(IN);
				setState(299);
				match(LEFT_PAREN);
				setState(300);
				expression();
				setState(305);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(301);
					match(COMMA);
					setState(302);
					expression();
					}
					}
					setState(307);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(308);
				match(RIGHT_PAREN);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(311);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(310);
					match(NOT);
					}
				}

				setState(313);
				match(BETWEEN);
				setState(314);
				concatenation();
				setState(315);
				match(AND);
				setState(316);
				concatenation();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(319);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(318);
					match(NOT);
					}
				}

				setState(321);
				match(LIKE);
				setState(322);
				concatenation();
				setState(325);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,30,_ctx) ) {
				case 1:
					{
					setState(323);
					match(ESCAPE);
					setState(324);
					concatenation();
					}
					break;
				}
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(327);
				match(IS);
				setState(329);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(328);
					match(NOT);
					}
				}

				setState(331);
				match(NULL_);
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
	public static class Relational_operatorContext extends ParserRuleContext {
		public TerminalNode EQUALS_OP() { return getToken(PlSqlParser.EQUALS_OP, 0); }
		public TerminalNode NOT_EQUAL_OP() { return getToken(PlSqlParser.NOT_EQUAL_OP, 0); }
		public TerminalNode NULL_SAFE_EQUALS() { return getToken(PlSqlParser.NULL_SAFE_EQUALS, 0); }
		public TerminalNode LESS_THAN_OP() { return getToken(PlSqlParser.LESS_THAN_OP, 0); }
		public TerminalNode GREATER_THAN_OP() { return getToken(PlSqlParser.GREATER_THAN_OP, 0); }
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
		enterRule(_localctx, 60, RULE_relational_operator);
		int _la;
		try {
			setState(345);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case EQUALS_OP:
				enterOuterAlt(_localctx, 1);
				{
				setState(334);
				match(EQUALS_OP);
				}
				break;
			case NOT_EQUAL_OP:
				enterOuterAlt(_localctx, 2);
				{
				setState(335);
				match(NOT_EQUAL_OP);
				}
				break;
			case NULL_SAFE_EQUALS:
				enterOuterAlt(_localctx, 3);
				{
				setState(336);
				match(NULL_SAFE_EQUALS);
				}
				break;
			case LESS_THAN_OP:
				enterOuterAlt(_localctx, 4);
				{
				setState(337);
				match(LESS_THAN_OP);
				setState(339);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==EQUALS_OP) {
					{
					setState(338);
					match(EQUALS_OP);
					}
				}

				}
				break;
			case GREATER_THAN_OP:
				enterOuterAlt(_localctx, 5);
				{
				setState(341);
				match(GREATER_THAN_OP);
				setState(343);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==EQUALS_OP) {
					{
					setState(342);
					match(EQUALS_OP);
					}
				}

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
	public static class ConcatenationContext extends ParserRuleContext {
		public List<AdditiveContext> additive() {
			return getRuleContexts(AdditiveContext.class);
		}
		public AdditiveContext additive(int i) {
			return getRuleContext(AdditiveContext.class,i);
		}
		public List<TerminalNode> CONCAT() { return getTokens(PlSqlParser.CONCAT); }
		public TerminalNode CONCAT(int i) {
			return getToken(PlSqlParser.CONCAT, i);
		}
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
		ConcatenationContext _localctx = new ConcatenationContext(_ctx, getState());
		enterRule(_localctx, 62, RULE_concatenation);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(347);
			additive();
			setState(357);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,37,_ctx);
			while ( _alt!=2 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					setState(355);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,36,_ctx) ) {
					case 1:
						{
						setState(348);
						match(CONCAT);
						setState(349);
						additive();
						}
						break;
					case 2:
						{
						setState(350);
						match(BAR);
						setState(351);
						match(BAR);
						setState(352);
						additive();
						}
						break;
					case 3:
						{
						setState(353);
						match(BAR);
						setState(354);
						additive();
						}
						break;
					}
					} 
				}
				setState(359);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,37,_ctx);
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
	public static class AdditiveContext extends ParserRuleContext {
		public List<MultiplicativeContext> multiplicative() {
			return getRuleContexts(MultiplicativeContext.class);
		}
		public MultiplicativeContext multiplicative(int i) {
			return getRuleContext(MultiplicativeContext.class,i);
		}
		public List<TerminalNode> PLUS_SIGN() { return getTokens(PlSqlParser.PLUS_SIGN); }
		public TerminalNode PLUS_SIGN(int i) {
			return getToken(PlSqlParser.PLUS_SIGN, i);
		}
		public List<TerminalNode> MINUS_SIGN() { return getTokens(PlSqlParser.MINUS_SIGN); }
		public TerminalNode MINUS_SIGN(int i) {
			return getToken(PlSqlParser.MINUS_SIGN, i);
		}
		public AdditiveContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_additive; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterAdditive(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitAdditive(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitAdditive(this);
			else return visitor.visitChildren(this);
		}
	}

	public final AdditiveContext additive() throws RecognitionException {
		AdditiveContext _localctx = new AdditiveContext(_ctx, getState());
		enterRule(_localctx, 64, RULE_additive);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(360);
			multiplicative();
			setState(365);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,38,_ctx);
			while ( _alt!=2 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(361);
					_la = _input.LA(1);
					if ( !(_la==PLUS_SIGN || _la==MINUS_SIGN) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					setState(362);
					multiplicative();
					}
					} 
				}
				setState(367);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,38,_ctx);
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
	public static class MultiplicativeContext extends ParserRuleContext {
		public List<UnaryContext> unary() {
			return getRuleContexts(UnaryContext.class);
		}
		public UnaryContext unary(int i) {
			return getRuleContext(UnaryContext.class,i);
		}
		public List<TerminalNode> ASTERISK() { return getTokens(PlSqlParser.ASTERISK); }
		public TerminalNode ASTERISK(int i) {
			return getToken(PlSqlParser.ASTERISK, i);
		}
		public List<TerminalNode> SOLIDUS() { return getTokens(PlSqlParser.SOLIDUS); }
		public TerminalNode SOLIDUS(int i) {
			return getToken(PlSqlParser.SOLIDUS, i);
		}
		public List<TerminalNode> PERCENT() { return getTokens(PlSqlParser.PERCENT); }
		public TerminalNode PERCENT(int i) {
			return getToken(PlSqlParser.PERCENT, i);
		}
		public MultiplicativeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_multiplicative; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterMultiplicative(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitMultiplicative(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitMultiplicative(this);
			else return visitor.visitChildren(this);
		}
	}

	public final MultiplicativeContext multiplicative() throws RecognitionException {
		MultiplicativeContext _localctx = new MultiplicativeContext(_ctx, getState());
		enterRule(_localctx, 66, RULE_multiplicative);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(368);
			unary();
			setState(373);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,39,_ctx);
			while ( _alt!=2 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(369);
					_la = _input.LA(1);
					if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & 985162418487296L) != 0)) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					setState(370);
					unary();
					}
					} 
				}
				setState(375);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,39,_ctx);
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
	public static class UnaryContext extends ParserRuleContext {
		public UnaryContext unary() {
			return getRuleContext(UnaryContext.class,0);
		}
		public TerminalNode PLUS_SIGN() { return getToken(PlSqlParser.PLUS_SIGN, 0); }
		public TerminalNode MINUS_SIGN() { return getToken(PlSqlParser.MINUS_SIGN, 0); }
		public AtomContext atom() {
			return getRuleContext(AtomContext.class,0);
		}
		public UnaryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_unary; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterUnary(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitUnary(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitUnary(this);
			else return visitor.visitChildren(this);
		}
	}

	public final UnaryContext unary() throws RecognitionException {
		UnaryContext _localctx = new UnaryContext(_ctx, getState());
		enterRule(_localctx, 68, RULE_unary);
		int _la;
		try {
			setState(379);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case PLUS_SIGN:
			case MINUS_SIGN:
				enterOuterAlt(_localctx, 1);
				{
				setState(376);
				_la = _input.LA(1);
				if ( !(_la==PLUS_SIGN || _la==MINUS_SIGN) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(377);
				unary();
				}
				break;
			case NULL_:
			case TRUE:
			case FALSE:
			case LEFT_PAREN:
			case CHAR_STRING:
			case NATIONAL_CHAR_STRING_LIT:
			case UNSIGNED_INTEGER:
			case APPROXIMATE_NUM_LIT:
			case DELIMITED_ID:
			case BACKTICK_ID:
			case BINDVAR:
			case REGULAR_ID:
				enterOuterAlt(_localctx, 2);
				{
				setState(378);
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
		public LiteralContext literal() {
			return getRuleContext(LiteralContext.class,0);
		}
		public TerminalNode BINDVAR() { return getToken(PlSqlParser.BINDVAR, 0); }
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public List<TerminalNode> PERIOD() { return getTokens(PlSqlParser.PERIOD); }
		public TerminalNode PERIOD(int i) {
			return getToken(PlSqlParser.PERIOD, i);
		}
		public TerminalNode LEFT_PAREN() { return getToken(PlSqlParser.LEFT_PAREN, 0); }
		public TerminalNode RIGHT_PAREN() { return getToken(PlSqlParser.RIGHT_PAREN, 0); }
		public Func_argsContext func_args() {
			return getRuleContext(Func_argsContext.class,0);
		}
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
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
		enterRule(_localctx, 70, RULE_atom);
		int _la;
		try {
			int _alt;
			setState(402);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case NULL_:
			case TRUE:
			case FALSE:
			case CHAR_STRING:
			case NATIONAL_CHAR_STRING_LIT:
			case UNSIGNED_INTEGER:
			case APPROXIMATE_NUM_LIT:
				enterOuterAlt(_localctx, 1);
				{
				setState(381);
				literal();
				}
				break;
			case BINDVAR:
				enterOuterAlt(_localctx, 2);
				{
				setState(382);
				match(BINDVAR);
				}
				break;
			case DELIMITED_ID:
			case BACKTICK_ID:
			case REGULAR_ID:
				enterOuterAlt(_localctx, 3);
				{
				setState(383);
				identifier();
				setState(388);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,41,_ctx);
				while ( _alt!=2 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(384);
						match(PERIOD);
						setState(385);
						identifier();
						}
						} 
					}
					setState(390);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,41,_ctx);
				}
				setState(396);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,43,_ctx) ) {
				case 1:
					{
					setState(391);
					match(LEFT_PAREN);
					setState(393);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if ((((_la) & ~0x3f) == 0 && ((1L << _la) & -1188950301625810946L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & 2047L) != 0)) {
						{
						setState(392);
						func_args();
						}
					}

					setState(395);
					match(RIGHT_PAREN);
					}
					break;
				}
				}
				break;
			case LEFT_PAREN:
				enterOuterAlt(_localctx, 4);
				{
				setState(398);
				match(LEFT_PAREN);
				setState(399);
				expression();
				setState(400);
				match(RIGHT_PAREN);
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
	public static class Func_argsContext extends ParserRuleContext {
		public List<Func_argContext> func_arg() {
			return getRuleContexts(Func_argContext.class);
		}
		public Func_argContext func_arg(int i) {
			return getRuleContext(Func_argContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(PlSqlParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(PlSqlParser.COMMA, i);
		}
		public Func_argsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_func_args; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterFunc_args(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitFunc_args(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitFunc_args(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Func_argsContext func_args() throws RecognitionException {
		Func_argsContext _localctx = new Func_argsContext(_ctx, getState());
		enterRule(_localctx, 72, RULE_func_args);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(404);
			func_arg();
			setState(409);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(405);
				match(COMMA);
				setState(406);
				func_arg();
				}
				}
				setState(411);
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
	public static class Func_argContext extends ParserRuleContext {
		public List<Func_topContext> func_top() {
			return getRuleContexts(Func_topContext.class);
		}
		public Func_topContext func_top(int i) {
			return getRuleContext(Func_topContext.class,i);
		}
		public Func_argContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_func_arg; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterFunc_arg(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitFunc_arg(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitFunc_arg(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Func_argContext func_arg() throws RecognitionException {
		Func_argContext _localctx = new Func_argContext(_ctx, getState());
		enterRule(_localctx, 74, RULE_func_arg);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(413); 
			_errHandler.sync(this);
			_la = _input.LA(1);
			do {
				{
				{
				setState(412);
				func_top();
				}
				}
				setState(415); 
				_errHandler.sync(this);
				_la = _input.LA(1);
			} while ( (((_la) & ~0x3f) == 0 && ((1L << _la) & -1188950301625810946L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & 2047L) != 0) );
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
	public static class Func_topContext extends ParserRuleContext {
		public TerminalNode LEFT_PAREN() { return getToken(PlSqlParser.LEFT_PAREN, 0); }
		public TerminalNode RIGHT_PAREN() { return getToken(PlSqlParser.RIGHT_PAREN, 0); }
		public List<Func_nestedContext> func_nested() {
			return getRuleContexts(Func_nestedContext.class);
		}
		public Func_nestedContext func_nested(int i) {
			return getRuleContext(Func_nestedContext.class,i);
		}
		public TerminalNode COMMA() { return getToken(PlSqlParser.COMMA, 0); }
		public Func_topContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_func_top; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterFunc_top(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitFunc_top(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitFunc_top(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Func_topContext func_top() throws RecognitionException {
		Func_topContext _localctx = new Func_topContext(_ctx, getState());
		enterRule(_localctx, 76, RULE_func_top);
		int _la;
		try {
			setState(426);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case LEFT_PAREN:
				enterOuterAlt(_localctx, 1);
				{
				setState(417);
				match(LEFT_PAREN);
				setState(421);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while ((((_la) & ~0x3f) == 0 && ((1L << _la) & -36028797018963970L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & 2047L) != 0)) {
					{
					{
					setState(418);
					func_nested();
					}
					}
					setState(423);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(424);
				match(RIGHT_PAREN);
				}
				break;
			case DEF:
			case IF:
			case ELSE:
			case ELSIF:
			case ELSEIF:
			case WHILE:
			case FOR:
			case IN:
			case TRY:
			case CATCH:
			case BREAK:
			case CONTINUE:
			case RAISE:
			case SET:
			case START:
			case END:
			case TRANSACTION:
			case COMMIT:
			case ROLLBACK:
			case SELECT:
			case INSERT:
			case UPDATE:
			case DELETE:
			case MERGE:
			case TRUNCATE:
			case CREATE:
			case DROP:
			case ALTER:
			case WITH:
			case AND:
			case OR:
			case NOT:
			case IS:
			case NULL_:
			case TRUE:
			case FALSE:
			case BETWEEN:
			case LIKE:
			case ESCAPE:
			case NULL_SAFE_EQUALS:
			case NOT_EQUAL_OP:
			case LESS_THAN_OP:
			case GREATER_THAN_OP:
			case EQUALS_OP:
			case PLUS_SIGN:
			case MINUS_SIGN:
			case ASTERISK:
			case SOLIDUS:
			case PERCENT:
			case CONCAT:
			case BAR:
			case AMPERSAND:
			case CARET:
			case LEFT_BRACE:
			case RIGHT_BRACE:
			case LEFT_BRACKET:
			case RIGHT_BRACKET:
			case SEMICOLON:
			case PERIOD:
			case DOUBLE_PERIOD:
			case CHAR_STRING:
			case NATIONAL_CHAR_STRING_LIT:
			case UNSIGNED_INTEGER:
			case APPROXIMATE_NUM_LIT:
			case DELIMITED_ID:
			case BACKTICK_ID:
			case BINDVAR:
			case REGULAR_ID:
			case SINGLE_LINE_COMMENT:
			case MULTI_LINE_COMMENT:
			case SPACES:
				enterOuterAlt(_localctx, 2);
				{
				setState(425);
				_la = _input.LA(1);
				if ( _la <= 0 || ((((_la) & ~0x3f) == 0 && ((1L << _la) & 1206964700135292928L) != 0)) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
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
	public static class Func_nestedContext extends ParserRuleContext {
		public TerminalNode LEFT_PAREN() { return getToken(PlSqlParser.LEFT_PAREN, 0); }
		public TerminalNode RIGHT_PAREN() { return getToken(PlSqlParser.RIGHT_PAREN, 0); }
		public List<Func_nestedContext> func_nested() {
			return getRuleContexts(Func_nestedContext.class);
		}
		public Func_nestedContext func_nested(int i) {
			return getRuleContext(Func_nestedContext.class,i);
		}
		public Func_nestedContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_func_nested; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterFunc_nested(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitFunc_nested(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitFunc_nested(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Func_nestedContext func_nested() throws RecognitionException {
		Func_nestedContext _localctx = new Func_nestedContext(_ctx, getState());
		enterRule(_localctx, 78, RULE_func_nested);
		int _la;
		try {
			setState(437);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case LEFT_PAREN:
				enterOuterAlt(_localctx, 1);
				{
				setState(428);
				match(LEFT_PAREN);
				setState(432);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while ((((_la) & ~0x3f) == 0 && ((1L << _la) & -36028797018963970L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & 2047L) != 0)) {
					{
					{
					setState(429);
					func_nested();
					}
					}
					setState(434);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(435);
				match(RIGHT_PAREN);
				}
				break;
			case DEF:
			case IF:
			case ELSE:
			case ELSIF:
			case ELSEIF:
			case WHILE:
			case FOR:
			case IN:
			case TRY:
			case CATCH:
			case BREAK:
			case CONTINUE:
			case RAISE:
			case SET:
			case START:
			case END:
			case TRANSACTION:
			case COMMIT:
			case ROLLBACK:
			case SELECT:
			case INSERT:
			case UPDATE:
			case DELETE:
			case MERGE:
			case TRUNCATE:
			case CREATE:
			case DROP:
			case ALTER:
			case WITH:
			case AND:
			case OR:
			case NOT:
			case IS:
			case NULL_:
			case TRUE:
			case FALSE:
			case BETWEEN:
			case LIKE:
			case ESCAPE:
			case NULL_SAFE_EQUALS:
			case NOT_EQUAL_OP:
			case LESS_THAN_OP:
			case GREATER_THAN_OP:
			case EQUALS_OP:
			case PLUS_SIGN:
			case MINUS_SIGN:
			case ASTERISK:
			case SOLIDUS:
			case PERCENT:
			case CONCAT:
			case BAR:
			case AMPERSAND:
			case CARET:
			case LEFT_BRACE:
			case RIGHT_BRACE:
			case LEFT_BRACKET:
			case RIGHT_BRACKET:
			case COMMA:
			case SEMICOLON:
			case PERIOD:
			case DOUBLE_PERIOD:
			case CHAR_STRING:
			case NATIONAL_CHAR_STRING_LIT:
			case UNSIGNED_INTEGER:
			case APPROXIMATE_NUM_LIT:
			case DELIMITED_ID:
			case BACKTICK_ID:
			case BINDVAR:
			case REGULAR_ID:
			case SINGLE_LINE_COMMENT:
			case MULTI_LINE_COMMENT:
			case SPACES:
				enterOuterAlt(_localctx, 2);
				{
				setState(436);
				_la = _input.LA(1);
				if ( _la <= 0 || (_la==LEFT_PAREN || _la==RIGHT_PAREN) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
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
	public static class LiteralContext extends ParserRuleContext {
		public NumericContext numeric() {
			return getRuleContext(NumericContext.class,0);
		}
		public TerminalNode CHAR_STRING() { return getToken(PlSqlParser.CHAR_STRING, 0); }
		public TerminalNode NATIONAL_CHAR_STRING_LIT() { return getToken(PlSqlParser.NATIONAL_CHAR_STRING_LIT, 0); }
		public TerminalNode NULL_() { return getToken(PlSqlParser.NULL_, 0); }
		public TerminalNode TRUE() { return getToken(PlSqlParser.TRUE, 0); }
		public TerminalNode FALSE() { return getToken(PlSqlParser.FALSE, 0); }
		public LiteralContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_literal; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).enterLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PlSqlParserListener ) ((PlSqlParserListener)listener).exitLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PlSqlParserVisitor ) return ((PlSqlParserVisitor<? extends T>)visitor).visitLiteral(this);
			else return visitor.visitChildren(this);
		}
	}

	public final LiteralContext literal() throws RecognitionException {
		LiteralContext _localctx = new LiteralContext(_ctx, getState());
		enterRule(_localctx, 80, RULE_literal);
		try {
			setState(445);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case UNSIGNED_INTEGER:
			case APPROXIMATE_NUM_LIT:
				enterOuterAlt(_localctx, 1);
				{
				setState(439);
				numeric();
				}
				break;
			case CHAR_STRING:
				enterOuterAlt(_localctx, 2);
				{
				setState(440);
				match(CHAR_STRING);
				}
				break;
			case NATIONAL_CHAR_STRING_LIT:
				enterOuterAlt(_localctx, 3);
				{
				setState(441);
				match(NATIONAL_CHAR_STRING_LIT);
				}
				break;
			case NULL_:
				enterOuterAlt(_localctx, 4);
				{
				setState(442);
				match(NULL_);
				}
				break;
			case TRUE:
				enterOuterAlt(_localctx, 5);
				{
				setState(443);
				match(TRUE);
				}
				break;
			case FALSE:
				enterOuterAlt(_localctx, 6);
				{
				setState(444);
				match(FALSE);
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
		enterRule(_localctx, 82, RULE_numeric);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(447);
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
	public static class IdentifierContext extends ParserRuleContext {
		public TerminalNode REGULAR_ID() { return getToken(PlSqlParser.REGULAR_ID, 0); }
		public TerminalNode DELIMITED_ID() { return getToken(PlSqlParser.DELIMITED_ID, 0); }
		public TerminalNode BACKTICK_ID() { return getToken(PlSqlParser.BACKTICK_ID, 0); }
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
		enterRule(_localctx, 84, RULE_identifier);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(449);
			_la = _input.LA(1);
			if ( !(((((_la - 68)) & ~0x3f) == 0 && ((1L << (_la - 68)) & 11L) != 0)) ) {
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
		case 3:
			return body_sempred((BodyContext)_localctx, predIndex);
		}
		return true;
	}
	private boolean body_sempred(BodyContext _localctx, int predIndex) {
		switch (predIndex) {
		case 0:
			return  _input.LA(1) != END || _input.LA(2) == TRANSACTION ;
		}
		return true;
	}

	public static final String _serializedATN =
		"\u0004\u0001J\u01c4\u0002\u0000\u0007\u0000\u0002\u0001\u0007\u0001\u0002"+
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
		"(\u0007(\u0002)\u0007)\u0002*\u0007*\u0001\u0000\u0005\u0000X\b\u0000"+
		"\n\u0000\f\u0000[\t\u0000\u0001\u0000\u0001\u0000\u0001\u0001\u0001\u0001"+
		"\u0001\u0001\u0001\u0001\u0001\u0001\u0003\u0001d\b\u0001\u0001\u0002"+
		"\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002"+
		"\u0003\u0002m\b\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002"+
		"\u0003\u0002s\b\u0002\u0001\u0003\u0001\u0003\u0005\u0003w\b\u0003\n\u0003"+
		"\f\u0003z\t\u0003\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0004\u0003"+
		"\u0004\u0080\b\u0004\u0001\u0005\u0001\u0005\u0001\u0005\u0005\u0005\u0085"+
		"\b\u0005\n\u0005\f\u0005\u0088\t\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0005\u0006\u0090\b\u0006\n\u0006"+
		"\f\u0006\u0093\t\u0006\u0001\u0006\u0001\u0006\u0003\u0006\u0097\b\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0007\u0001\u0007\u0005\u0007\u009d\b\u0007"+
		"\n\u0007\f\u0007\u00a0\t\u0007\u0001\u0007\u0001\u0007\u0001\b\u0001\b"+
		"\u0001\b\u0001\b\u0001\b\u0001\b\u0003\b\u00aa\b\b\u0001\t\u0001\t\u0001"+
		"\n\u0001\n\u0001\u000b\u0001\u000b\u0001\f\u0001\f\u0001\f\u0001\f\u0005"+
		"\f\u00b6\b\f\n\f\f\f\u00b9\t\f\u0001\f\u0003\f\u00bc\b\f\u0001\f\u0001"+
		"\f\u0003\f\u00c0\b\f\u0001\r\u0001\r\u0001\r\u0001\r\u0001\u000e\u0001"+
		"\u000e\u0001\u000e\u0001\u000f\u0001\u000f\u0001\u000f\u0001\u000f\u0001"+
		"\u000f\u0003\u000f\u00ce\b\u000f\u0001\u0010\u0001\u0010\u0001\u0010\u0001"+
		"\u0010\u0001\u0010\u0001\u0010\u0001\u0010\u0003\u0010\u00d7\b\u0010\u0001"+
		"\u0011\u0001\u0011\u0001\u0011\u0001\u0011\u0001\u0011\u0003\u0011\u00de"+
		"\b\u0011\u0001\u0012\u0001\u0012\u0001\u0012\u0005\u0012\u00e3\b\u0012"+
		"\n\u0012\f\u0012\u00e6\t\u0012\u0001\u0012\u0001\u0012\u0001\u0013\u0001"+
		"\u0013\u0005\u0013\u00ec\b\u0013\n\u0013\f\u0013\u00ef\t\u0013\u0001\u0013"+
		"\u0001\u0013\u0003\u0013\u00f3\b\u0013\u0001\u0014\u0001\u0014\u0001\u0014"+
		"\u0001\u0014\u0001\u0014\u0001\u0014\u0003\u0014\u00fb\b\u0014\u0001\u0015"+
		"\u0001\u0015\u0005\u0015\u00ff\b\u0015\n\u0015\f\u0015\u0102\t\u0015\u0001"+
		"\u0015\u0001\u0015\u0001\u0016\u0001\u0016\u0001\u0017\u0001\u0017\u0001"+
		"\u0018\u0001\u0018\u0001\u0019\u0001\u0019\u0001\u0019\u0005\u0019\u010f"+
		"\b\u0019\n\u0019\f\u0019\u0112\t\u0019\u0001\u001a\u0001\u001a\u0001\u001a"+
		"\u0005\u001a\u0117\b\u001a\n\u001a\f\u001a\u011a\t\u001a\u0001\u001b\u0001"+
		"\u001b\u0001\u001b\u0003\u001b\u011f\b\u001b\u0001\u001c\u0001\u001c\u0003"+
		"\u001c\u0123\b\u001c\u0001\u001d\u0001\u001d\u0001\u001d\u0001\u001d\u0003"+
		"\u001d\u0129\b\u001d\u0001\u001d\u0001\u001d\u0001\u001d\u0001\u001d\u0001"+
		"\u001d\u0005\u001d\u0130\b\u001d\n\u001d\f\u001d\u0133\t\u001d\u0001\u001d"+
		"\u0001\u001d\u0001\u001d\u0003\u001d\u0138\b\u001d\u0001\u001d\u0001\u001d"+
		"\u0001\u001d\u0001\u001d\u0001\u001d\u0001\u001d\u0003\u001d\u0140\b\u001d"+
		"\u0001\u001d\u0001\u001d\u0001\u001d\u0001\u001d\u0003\u001d\u0146\b\u001d"+
		"\u0001\u001d\u0001\u001d\u0003\u001d\u014a\b\u001d\u0001\u001d\u0003\u001d"+
		"\u014d\b\u001d\u0001\u001e\u0001\u001e\u0001\u001e\u0001\u001e\u0001\u001e"+
		"\u0003\u001e\u0154\b\u001e\u0001\u001e\u0001\u001e\u0003\u001e\u0158\b"+
		"\u001e\u0003\u001e\u015a\b\u001e\u0001\u001f\u0001\u001f\u0001\u001f\u0001"+
		"\u001f\u0001\u001f\u0001\u001f\u0001\u001f\u0001\u001f\u0005\u001f\u0164"+
		"\b\u001f\n\u001f\f\u001f\u0167\t\u001f\u0001 \u0001 \u0001 \u0005 \u016c"+
		"\b \n \f \u016f\t \u0001!\u0001!\u0001!\u0005!\u0174\b!\n!\f!\u0177\t"+
		"!\u0001\"\u0001\"\u0001\"\u0003\"\u017c\b\"\u0001#\u0001#\u0001#\u0001"+
		"#\u0001#\u0005#\u0183\b#\n#\f#\u0186\t#\u0001#\u0001#\u0003#\u018a\b#"+
		"\u0001#\u0003#\u018d\b#\u0001#\u0001#\u0001#\u0001#\u0003#\u0193\b#\u0001"+
		"$\u0001$\u0001$\u0005$\u0198\b$\n$\f$\u019b\t$\u0001%\u0004%\u019e\b%"+
		"\u000b%\f%\u019f\u0001&\u0001&\u0005&\u01a4\b&\n&\f&\u01a7\t&\u0001&\u0001"+
		"&\u0003&\u01ab\b&\u0001\'\u0001\'\u0005\'\u01af\b\'\n\'\f\'\u01b2\t\'"+
		"\u0001\'\u0001\'\u0003\'\u01b6\b\'\u0001(\u0001(\u0001(\u0001(\u0001("+
		"\u0001(\u0003(\u01be\b(\u0001)\u0001)\u0001*\u0001*\u0001*\u0000\u0000"+
		"+\u0000\u0002\u0004\u0006\b\n\f\u000e\u0010\u0012\u0014\u0016\u0018\u001a"+
		"\u001c\u001e \"$&(*,.02468:<>@BDFHJLNPRT\u0000\n\u0001\u0000\u0004\u0005"+
		"\u0002\u0000\u0014\u0014\u001d\u001d\u0001\u000067\u0001\u0000\u0014\u001d"+
		"\u0001\u0000==\u0001\u0000-.\u0001\u0000/1\u0002\u000067<<\u0001\u0000"+
		"BC\u0002\u0000DEGG\u01e4\u0000Y\u0001\u0000\u0000\u0000\u0002c\u0001\u0000"+
		"\u0000\u0000\u0004r\u0001\u0000\u0000\u0000\u0006x\u0001\u0000\u0000\u0000"+
		"\b{\u0001\u0000\u0000\u0000\n\u0081\u0001\u0000\u0000\u0000\f\u008c\u0001"+
		"\u0000\u0000\u0000\u000e\u009a\u0001\u0000\u0000\u0000\u0010\u00a9\u0001"+
		"\u0000\u0000\u0000\u0012\u00ab\u0001\u0000\u0000\u0000\u0014\u00ad\u0001"+
		"\u0000\u0000\u0000\u0016\u00af\u0001\u0000\u0000\u0000\u0018\u00b1\u0001"+
		"\u0000\u0000\u0000\u001a\u00c1\u0001\u0000\u0000\u0000\u001c\u00c5\u0001"+
		"\u0000\u0000\u0000\u001e\u00c8\u0001\u0000\u0000\u0000 \u00cf\u0001\u0000"+
		"\u0000\u0000\"\u00dd\u0001\u0000\u0000\u0000$\u00df\u0001\u0000\u0000"+
		"\u0000&\u00f2\u0001\u0000\u0000\u0000(\u00f4\u0001\u0000\u0000\u0000*"+
		"\u00fc\u0001\u0000\u0000\u0000,\u0105\u0001\u0000\u0000\u0000.\u0107\u0001"+
		"\u0000\u0000\u00000\u0109\u0001\u0000\u0000\u00002\u010b\u0001\u0000\u0000"+
		"\u00004\u0113\u0001\u0000\u0000\u00006\u011e\u0001\u0000\u0000\u00008"+
		"\u0120\u0001\u0000\u0000\u0000:\u014c\u0001\u0000\u0000\u0000<\u0159\u0001"+
		"\u0000\u0000\u0000>\u015b\u0001\u0000\u0000\u0000@\u0168\u0001\u0000\u0000"+
		"\u0000B\u0170\u0001\u0000\u0000\u0000D\u017b\u0001\u0000\u0000\u0000F"+
		"\u0192\u0001\u0000\u0000\u0000H\u0194\u0001\u0000\u0000\u0000J\u019d\u0001"+
		"\u0000\u0000\u0000L\u01aa\u0001\u0000\u0000\u0000N\u01b5\u0001\u0000\u0000"+
		"\u0000P\u01bd\u0001\u0000\u0000\u0000R\u01bf\u0001\u0000\u0000\u0000T"+
		"\u01c1\u0001\u0000\u0000\u0000VX\u0003\u0002\u0001\u0000WV\u0001\u0000"+
		"\u0000\u0000X[\u0001\u0000\u0000\u0000YW\u0001\u0000\u0000\u0000YZ\u0001"+
		"\u0000\u0000\u0000Z\\\u0001\u0000\u0000\u0000[Y\u0001\u0000\u0000\u0000"+
		"\\]\u0005\u0000\u0000\u0001]\u0001\u0001\u0000\u0000\u0000^d\u0003\u0004"+
		"\u0002\u0000_d\u0003\u0018\f\u0000`d\u0003\u001e\u000f\u0000ad\u0003 "+
		"\u0010\u0000bd\u0003(\u0014\u0000c^\u0001\u0000\u0000\u0000c_\u0001\u0000"+
		"\u0000\u0000c`\u0001\u0000\u0000\u0000ca\u0001\u0000\u0000\u0000cb\u0001"+
		"\u0000\u0000\u0000d\u0003\u0001\u0000\u0000\u0000em\u0003\b\u0004\u0000"+
		"fm\u0003\n\u0005\u0000gm\u0003\f\u0006\u0000hm\u0003\u0010\b\u0000im\u0003"+
		"\u0012\t\u0000jm\u0003\u0014\n\u0000km\u0003\u0016\u000b\u0000le\u0001"+
		"\u0000\u0000\u0000lf\u0001\u0000\u0000\u0000lg\u0001\u0000\u0000\u0000"+
		"lh\u0001\u0000\u0000\u0000li\u0001\u0000\u0000\u0000lj\u0001\u0000\u0000"+
		"\u0000lk\u0001\u0000\u0000\u0000mn\u0001\u0000\u0000\u0000no\u0005=\u0000"+
		"\u0000os\u0001\u0000\u0000\u0000ps\u0003\u000e\u0007\u0000qs\u0003*\u0015"+
		"\u0000rl\u0001\u0000\u0000\u0000rp\u0001\u0000\u0000\u0000rq\u0001\u0000"+
		"\u0000\u0000s\u0005\u0001\u0000\u0000\u0000tu\u0004\u0003\u0000\u0000"+
		"uw\u0003\u0002\u0001\u0000vt\u0001\u0000\u0000\u0000wz\u0001\u0000\u0000"+
		"\u0000xv\u0001\u0000\u0000\u0000xy\u0001\u0000\u0000\u0000y\u0007\u0001"+
		"\u0000\u0000\u0000zx\u0001\u0000\u0000\u0000{|\u0005\u0001\u0000\u0000"+
		"|\u007f\u0003T*\u0000}~\u0005,\u0000\u0000~\u0080\u00030\u0018\u0000\u007f"+
		"}\u0001\u0000\u0000\u0000\u007f\u0080\u0001\u0000\u0000\u0000\u0080\t"+
		"\u0001\u0000\u0000\u0000\u0081\u0086\u0003T*\u0000\u0082\u0083\u0005>"+
		"\u0000\u0000\u0083\u0085\u0003T*\u0000\u0084\u0082\u0001\u0000\u0000\u0000"+
		"\u0085\u0088\u0001\u0000\u0000\u0000\u0086\u0084\u0001\u0000\u0000\u0000"+
		"\u0086\u0087\u0001\u0000\u0000\u0000\u0087\u0089\u0001\u0000\u0000\u0000"+
		"\u0088\u0086\u0001\u0000\u0000\u0000\u0089\u008a\u0005,\u0000\u0000\u008a"+
		"\u008b\u00030\u0018\u0000\u008b\u000b\u0001\u0000\u0000\u0000\u008c\u0091"+
		"\u0003T*\u0000\u008d\u008e\u0005>\u0000\u0000\u008e\u0090\u0003T*\u0000"+
		"\u008f\u008d\u0001\u0000\u0000\u0000\u0090\u0093\u0001\u0000\u0000\u0000"+
		"\u0091\u008f\u0001\u0000\u0000\u0000\u0091\u0092\u0001\u0000\u0000\u0000"+
		"\u0092\u0094\u0001\u0000\u0000\u0000\u0093\u0091\u0001\u0000\u0000\u0000"+
		"\u0094\u0096\u00056\u0000\u0000\u0095\u0097\u0003H$\u0000\u0096\u0095"+
		"\u0001\u0000\u0000\u0000\u0096\u0097\u0001\u0000\u0000\u0000\u0097\u0098"+
		"\u0001\u0000\u0000\u0000\u0098\u0099\u00057\u0000\u0000\u0099\r\u0001"+
		"\u0000\u0000\u0000\u009a\u009e\u0005\u000e\u0000\u0000\u009b\u009d\u0003"+
		".\u0017\u0000\u009c\u009b\u0001\u0000\u0000\u0000\u009d\u00a0\u0001\u0000"+
		"\u0000\u0000\u009e\u009c\u0001\u0000\u0000\u0000\u009e\u009f\u0001\u0000"+
		"\u0000\u0000\u009f\u00a1\u0001\u0000\u0000\u0000\u00a0\u009e\u0001\u0000"+
		"\u0000\u0000\u00a1\u00a2\u0005=\u0000\u0000\u00a2\u000f\u0001\u0000\u0000"+
		"\u0000\u00a3\u00a4\u0005\u000f\u0000\u0000\u00a4\u00aa\u0005\u0011\u0000"+
		"\u0000\u00a5\u00a6\u0005\u0010\u0000\u0000\u00a6\u00aa\u0005\u0011\u0000"+
		"\u0000\u00a7\u00aa\u0005\u0012\u0000\u0000\u00a8\u00aa\u0005\u0013\u0000"+
		"\u0000\u00a9\u00a3\u0001\u0000\u0000\u0000\u00a9\u00a5\u0001\u0000\u0000"+
		"\u0000\u00a9\u00a7\u0001\u0000\u0000\u0000\u00a9\u00a8\u0001\u0000\u0000"+
		"\u0000\u00aa\u0011\u0001\u0000\u0000\u0000\u00ab\u00ac\u0005\u000b\u0000"+
		"\u0000\u00ac\u0013\u0001\u0000\u0000\u0000\u00ad\u00ae\u0005\f\u0000\u0000"+
		"\u00ae\u0015\u0001\u0000\u0000\u0000\u00af\u00b0\u0005\r\u0000\u0000\u00b0"+
		"\u0017\u0001\u0000\u0000\u0000\u00b1\u00b2\u0005\u0002\u0000\u0000\u00b2"+
		"\u00b3\u00030\u0018\u0000\u00b3\u00b7\u0003\u0006\u0003\u0000\u00b4\u00b6"+
		"\u0003\u001a\r\u0000\u00b5\u00b4\u0001\u0000\u0000\u0000\u00b6\u00b9\u0001"+
		"\u0000\u0000\u0000\u00b7\u00b5\u0001\u0000\u0000\u0000\u00b7\u00b8\u0001"+
		"\u0000\u0000\u0000\u00b8\u00bb\u0001\u0000\u0000\u0000\u00b9\u00b7\u0001"+
		"\u0000\u0000\u0000\u00ba\u00bc\u0003\u001c\u000e\u0000\u00bb\u00ba\u0001"+
		"\u0000\u0000\u0000\u00bb\u00bc\u0001\u0000\u0000\u0000\u00bc\u00bd\u0001"+
		"\u0000\u0000\u0000\u00bd\u00bf\u0005\u0010\u0000\u0000\u00be\u00c0\u0005"+
		"=\u0000\u0000\u00bf\u00be\u0001\u0000\u0000\u0000\u00bf\u00c0\u0001\u0000"+
		"\u0000\u0000\u00c0\u0019\u0001\u0000\u0000\u0000\u00c1\u00c2\u0007\u0000"+
		"\u0000\u0000\u00c2\u00c3\u00030\u0018\u0000\u00c3\u00c4\u0003\u0006\u0003"+
		"\u0000\u00c4\u001b\u0001\u0000\u0000\u0000\u00c5\u00c6\u0005\u0003\u0000"+
		"\u0000\u00c6\u00c7\u0003\u0006\u0003\u0000\u00c7\u001d\u0001\u0000\u0000"+
		"\u0000\u00c8\u00c9\u0005\u0006\u0000\u0000\u00c9\u00ca\u00030\u0018\u0000"+
		"\u00ca\u00cb\u0003\u0006\u0003\u0000\u00cb\u00cd\u0005\u0010\u0000\u0000"+
		"\u00cc\u00ce\u0005=\u0000\u0000\u00cd\u00cc\u0001\u0000\u0000\u0000\u00cd"+
		"\u00ce\u0001\u0000\u0000\u0000\u00ce\u001f\u0001\u0000\u0000\u0000\u00cf"+
		"\u00d0\u0005\u0007\u0000\u0000\u00d0\u00d1\u0003T*\u0000\u00d1\u00d2\u0005"+
		"\b\u0000\u0000\u00d2\u00d3\u0003\"\u0011\u0000\u00d3\u00d4\u0003\u0006"+
		"\u0003\u0000\u00d4\u00d6\u0005\u0010\u0000\u0000\u00d5\u00d7\u0005=\u0000"+
		"\u0000\u00d6\u00d5\u0001\u0000\u0000\u0000\u00d6\u00d7\u0001\u0000\u0000"+
		"\u0000\u00d7!\u0001\u0000\u0000\u0000\u00d8\u00de\u0003$\u0012\u0000\u00d9"+
		"\u00da\u00030\u0018\u0000\u00da\u00db\u0005?\u0000\u0000\u00db\u00dc\u0003"+
		"0\u0018\u0000\u00dc\u00de\u0001\u0000\u0000\u0000\u00dd\u00d8\u0001\u0000"+
		"\u0000\u0000\u00dd\u00d9\u0001\u0000\u0000\u0000\u00de#\u0001\u0000\u0000"+
		"\u0000\u00df\u00e0\u00056\u0000\u0000\u00e0\u00e4\u0007\u0001\u0000\u0000"+
		"\u00e1\u00e3\u0003&\u0013\u0000\u00e2\u00e1\u0001\u0000\u0000\u0000\u00e3"+
		"\u00e6\u0001\u0000\u0000\u0000\u00e4\u00e2\u0001\u0000\u0000\u0000\u00e4"+
		"\u00e5\u0001\u0000\u0000\u0000\u00e5\u00e7\u0001\u0000\u0000\u0000\u00e6"+
		"\u00e4\u0001\u0000\u0000\u0000\u00e7\u00e8\u00057\u0000\u0000\u00e8%\u0001"+
		"\u0000\u0000\u0000\u00e9\u00ed\u00056\u0000\u0000\u00ea\u00ec\u0003&\u0013"+
		"\u0000\u00eb\u00ea\u0001\u0000\u0000\u0000\u00ec\u00ef\u0001\u0000\u0000"+
		"\u0000\u00ed\u00eb\u0001\u0000\u0000\u0000\u00ed\u00ee\u0001\u0000\u0000"+
		"\u0000\u00ee\u00f0\u0001\u0000\u0000\u0000\u00ef\u00ed\u0001\u0000\u0000"+
		"\u0000\u00f0\u00f3\u00057\u0000\u0000\u00f1\u00f3\b\u0002\u0000\u0000"+
		"\u00f2\u00e9\u0001\u0000\u0000\u0000\u00f2\u00f1\u0001\u0000\u0000\u0000"+
		"\u00f3\'\u0001\u0000\u0000\u0000\u00f4\u00f5\u0005\t\u0000\u0000\u00f5"+
		"\u00f6\u0003\u0006\u0003\u0000\u00f6\u00f7\u0005\n\u0000\u0000\u00f7\u00f8"+
		"\u0003\u0006\u0003\u0000\u00f8\u00fa\u0005\u0010\u0000\u0000\u00f9\u00fb"+
		"\u0005=\u0000\u0000\u00fa\u00f9\u0001\u0000\u0000\u0000\u00fa\u00fb\u0001"+
		"\u0000\u0000\u0000\u00fb)\u0001\u0000\u0000\u0000\u00fc\u0100\u0003,\u0016"+
		"\u0000\u00fd\u00ff\u0003.\u0017\u0000\u00fe\u00fd\u0001\u0000\u0000\u0000"+
		"\u00ff\u0102\u0001\u0000\u0000\u0000\u0100\u00fe\u0001\u0000\u0000\u0000"+
		"\u0100\u0101\u0001\u0000\u0000\u0000\u0101\u0103\u0001\u0000\u0000\u0000"+
		"\u0102\u0100\u0001\u0000\u0000\u0000\u0103\u0104\u0005=\u0000\u0000\u0104"+
		"+\u0001\u0000\u0000\u0000\u0105\u0106\u0007\u0003\u0000\u0000\u0106-\u0001"+
		"\u0000\u0000\u0000\u0107\u0108\b\u0004\u0000\u0000\u0108/\u0001\u0000"+
		"\u0000\u0000\u0109\u010a\u00032\u0019\u0000\u010a1\u0001\u0000\u0000\u0000"+
		"\u010b\u0110\u00034\u001a\u0000\u010c\u010d\u0005\u001f\u0000\u0000\u010d"+
		"\u010f\u00034\u001a\u0000\u010e\u010c\u0001\u0000\u0000\u0000\u010f\u0112"+
		"\u0001\u0000\u0000\u0000\u0110\u010e\u0001\u0000\u0000\u0000\u0110\u0111"+
		"\u0001\u0000\u0000\u0000\u01113\u0001\u0000\u0000\u0000\u0112\u0110\u0001"+
		"\u0000\u0000\u0000\u0113\u0118\u00036\u001b\u0000\u0114\u0115\u0005\u001e"+
		"\u0000\u0000\u0115\u0117\u00036\u001b\u0000\u0116\u0114\u0001\u0000\u0000"+
		"\u0000\u0117\u011a\u0001\u0000\u0000\u0000\u0118\u0116\u0001\u0000\u0000"+
		"\u0000\u0118\u0119\u0001\u0000\u0000\u0000\u01195\u0001\u0000\u0000\u0000"+
		"\u011a\u0118\u0001\u0000\u0000\u0000\u011b\u011c\u0005 \u0000\u0000\u011c"+
		"\u011f\u00036\u001b\u0000\u011d\u011f\u00038\u001c\u0000\u011e\u011b\u0001"+
		"\u0000\u0000\u0000\u011e\u011d\u0001\u0000\u0000\u0000\u011f7\u0001\u0000"+
		"\u0000\u0000\u0120\u0122\u0003>\u001f\u0000\u0121\u0123\u0003:\u001d\u0000"+
		"\u0122\u0121\u0001\u0000\u0000\u0000\u0122\u0123\u0001\u0000\u0000\u0000"+
		"\u01239\u0001\u0000\u0000\u0000\u0124\u0125\u0003<\u001e\u0000\u0125\u0126"+
		"\u0003>\u001f\u0000\u0126\u014d\u0001\u0000\u0000\u0000\u0127\u0129\u0005"+
		" \u0000\u0000\u0128\u0127\u0001\u0000\u0000\u0000\u0128\u0129\u0001\u0000"+
		"\u0000\u0000\u0129\u012a\u0001\u0000\u0000\u0000\u012a\u012b\u0005\b\u0000"+
		"\u0000\u012b\u012c\u00056\u0000\u0000\u012c\u0131\u00030\u0018\u0000\u012d"+
		"\u012e\u0005<\u0000\u0000\u012e\u0130\u00030\u0018\u0000\u012f\u012d\u0001"+
		"\u0000\u0000\u0000\u0130\u0133\u0001\u0000\u0000\u0000\u0131\u012f\u0001"+
		"\u0000\u0000\u0000\u0131\u0132\u0001\u0000\u0000\u0000\u0132\u0134\u0001"+
		"\u0000\u0000\u0000\u0133\u0131\u0001\u0000\u0000\u0000\u0134\u0135\u0005"+
		"7\u0000\u0000\u0135\u014d\u0001\u0000\u0000\u0000\u0136\u0138\u0005 \u0000"+
		"\u0000\u0137\u0136\u0001\u0000\u0000\u0000\u0137\u0138\u0001\u0000\u0000"+
		"\u0000\u0138\u0139\u0001\u0000\u0000\u0000\u0139\u013a\u0005%\u0000\u0000"+
		"\u013a\u013b\u0003>\u001f\u0000\u013b\u013c\u0005\u001e\u0000\u0000\u013c"+
		"\u013d\u0003>\u001f\u0000\u013d\u014d\u0001\u0000\u0000\u0000\u013e\u0140"+
		"\u0005 \u0000\u0000\u013f\u013e\u0001\u0000\u0000\u0000\u013f\u0140\u0001"+
		"\u0000\u0000\u0000\u0140\u0141\u0001\u0000\u0000\u0000\u0141\u0142\u0005"+
		"&\u0000\u0000\u0142\u0145\u0003>\u001f\u0000\u0143\u0144\u0005\'\u0000"+
		"\u0000\u0144\u0146\u0003>\u001f\u0000\u0145\u0143\u0001\u0000\u0000\u0000"+
		"\u0145\u0146\u0001\u0000\u0000\u0000\u0146\u014d\u0001\u0000\u0000\u0000"+
		"\u0147\u0149\u0005!\u0000\u0000\u0148\u014a\u0005 \u0000\u0000\u0149\u0148"+
		"\u0001\u0000\u0000\u0000\u0149\u014a\u0001\u0000\u0000\u0000\u014a\u014b"+
		"\u0001\u0000\u0000\u0000\u014b\u014d\u0005\"\u0000\u0000\u014c\u0124\u0001"+
		"\u0000\u0000\u0000\u014c\u0128\u0001\u0000\u0000\u0000\u014c\u0137\u0001"+
		"\u0000\u0000\u0000\u014c\u013f\u0001\u0000\u0000\u0000\u014c\u0147\u0001"+
		"\u0000\u0000\u0000\u014d;\u0001\u0000\u0000\u0000\u014e\u015a\u0005,\u0000"+
		"\u0000\u014f\u015a\u0005)\u0000\u0000\u0150\u015a\u0005(\u0000\u0000\u0151"+
		"\u0153\u0005*\u0000\u0000\u0152\u0154\u0005,\u0000\u0000\u0153\u0152\u0001"+
		"\u0000\u0000\u0000\u0153\u0154\u0001\u0000\u0000\u0000\u0154\u015a\u0001"+
		"\u0000\u0000\u0000\u0155\u0157\u0005+\u0000\u0000\u0156\u0158\u0005,\u0000"+
		"\u0000\u0157\u0156\u0001\u0000\u0000\u0000\u0157\u0158\u0001\u0000\u0000"+
		"\u0000\u0158\u015a\u0001\u0000\u0000\u0000\u0159\u014e\u0001\u0000\u0000"+
		"\u0000\u0159\u014f\u0001\u0000\u0000\u0000\u0159\u0150\u0001\u0000\u0000"+
		"\u0000\u0159\u0151\u0001\u0000\u0000\u0000\u0159\u0155\u0001\u0000\u0000"+
		"\u0000\u015a=\u0001\u0000\u0000\u0000\u015b\u0165\u0003@ \u0000\u015c"+
		"\u015d\u00052\u0000\u0000\u015d\u0164\u0003@ \u0000\u015e\u015f\u0005"+
		"3\u0000\u0000\u015f\u0160\u00053\u0000\u0000\u0160\u0164\u0003@ \u0000"+
		"\u0161\u0162\u00053\u0000\u0000\u0162\u0164\u0003@ \u0000\u0163\u015c"+
		"\u0001\u0000\u0000\u0000\u0163\u015e\u0001\u0000\u0000\u0000\u0163\u0161"+
		"\u0001\u0000\u0000\u0000\u0164\u0167\u0001\u0000\u0000\u0000\u0165\u0163"+
		"\u0001\u0000\u0000\u0000\u0165\u0166\u0001\u0000\u0000\u0000\u0166?\u0001"+
		"\u0000\u0000\u0000\u0167\u0165\u0001\u0000\u0000\u0000\u0168\u016d\u0003"+
		"B!\u0000\u0169\u016a\u0007\u0005\u0000\u0000\u016a\u016c\u0003B!\u0000"+
		"\u016b\u0169\u0001\u0000\u0000\u0000\u016c\u016f\u0001\u0000\u0000\u0000"+
		"\u016d\u016b\u0001\u0000\u0000\u0000\u016d\u016e\u0001\u0000\u0000\u0000"+
		"\u016eA\u0001\u0000\u0000\u0000\u016f\u016d\u0001\u0000\u0000\u0000\u0170"+
		"\u0175\u0003D\"\u0000\u0171\u0172\u0007\u0006\u0000\u0000\u0172\u0174"+
		"\u0003D\"\u0000\u0173\u0171\u0001\u0000\u0000\u0000\u0174\u0177\u0001"+
		"\u0000\u0000\u0000\u0175\u0173\u0001\u0000\u0000\u0000\u0175\u0176\u0001"+
		"\u0000\u0000\u0000\u0176C\u0001\u0000\u0000\u0000\u0177\u0175\u0001\u0000"+
		"\u0000\u0000\u0178\u0179\u0007\u0005\u0000\u0000\u0179\u017c\u0003D\""+
		"\u0000\u017a\u017c\u0003F#\u0000\u017b\u0178\u0001\u0000\u0000\u0000\u017b"+
		"\u017a\u0001\u0000\u0000\u0000\u017cE\u0001\u0000\u0000\u0000\u017d\u0193"+
		"\u0003P(\u0000\u017e\u0193\u0005F\u0000\u0000\u017f\u0184\u0003T*\u0000"+
		"\u0180\u0181\u0005>\u0000\u0000\u0181\u0183\u0003T*\u0000\u0182\u0180"+
		"\u0001\u0000\u0000\u0000\u0183\u0186\u0001\u0000\u0000\u0000\u0184\u0182"+
		"\u0001\u0000\u0000\u0000\u0184\u0185\u0001\u0000\u0000\u0000\u0185\u018c"+
		"\u0001\u0000\u0000\u0000\u0186\u0184\u0001\u0000\u0000\u0000\u0187\u0189"+
		"\u00056\u0000\u0000\u0188\u018a\u0003H$\u0000\u0189\u0188\u0001\u0000"+
		"\u0000\u0000\u0189\u018a\u0001\u0000\u0000\u0000\u018a\u018b\u0001\u0000"+
		"\u0000\u0000\u018b\u018d\u00057\u0000\u0000\u018c\u0187\u0001\u0000\u0000"+
		"\u0000\u018c\u018d\u0001\u0000\u0000\u0000\u018d\u0193\u0001\u0000\u0000"+
		"\u0000\u018e\u018f\u00056\u0000\u0000\u018f\u0190\u00030\u0018\u0000\u0190"+
		"\u0191\u00057\u0000\u0000\u0191\u0193\u0001\u0000\u0000\u0000\u0192\u017d"+
		"\u0001\u0000\u0000\u0000\u0192\u017e\u0001\u0000\u0000\u0000\u0192\u017f"+
		"\u0001\u0000\u0000\u0000\u0192\u018e\u0001\u0000\u0000\u0000\u0193G\u0001"+
		"\u0000\u0000\u0000\u0194\u0199\u0003J%\u0000\u0195\u0196\u0005<\u0000"+
		"\u0000\u0196\u0198\u0003J%\u0000\u0197\u0195\u0001\u0000\u0000\u0000\u0198"+
		"\u019b\u0001\u0000\u0000\u0000\u0199\u0197\u0001\u0000\u0000\u0000\u0199"+
		"\u019a\u0001\u0000\u0000\u0000\u019aI\u0001\u0000\u0000\u0000\u019b\u0199"+
		"\u0001\u0000\u0000\u0000\u019c\u019e\u0003L&\u0000\u019d\u019c\u0001\u0000"+
		"\u0000\u0000\u019e\u019f\u0001\u0000\u0000\u0000\u019f\u019d\u0001\u0000"+
		"\u0000\u0000\u019f\u01a0\u0001\u0000\u0000\u0000\u01a0K\u0001\u0000\u0000"+
		"\u0000\u01a1\u01a5\u00056\u0000\u0000\u01a2\u01a4\u0003N\'\u0000\u01a3"+
		"\u01a2\u0001\u0000\u0000\u0000\u01a4\u01a7\u0001\u0000\u0000\u0000\u01a5"+
		"\u01a3\u0001\u0000\u0000\u0000\u01a5\u01a6\u0001\u0000\u0000\u0000\u01a6"+
		"\u01a8\u0001\u0000\u0000\u0000\u01a7\u01a5\u0001\u0000\u0000\u0000\u01a8"+
		"\u01ab\u00057\u0000\u0000\u01a9\u01ab\b\u0007\u0000\u0000\u01aa\u01a1"+
		"\u0001\u0000\u0000\u0000\u01aa\u01a9\u0001\u0000\u0000\u0000\u01abM\u0001"+
		"\u0000\u0000\u0000\u01ac\u01b0\u00056\u0000\u0000\u01ad\u01af\u0003N\'"+
		"\u0000\u01ae\u01ad\u0001\u0000\u0000\u0000\u01af\u01b2\u0001\u0000\u0000"+
		"\u0000\u01b0\u01ae\u0001\u0000\u0000\u0000\u01b0\u01b1\u0001\u0000\u0000"+
		"\u0000\u01b1\u01b3\u0001\u0000\u0000\u0000\u01b2\u01b0\u0001\u0000\u0000"+
		"\u0000\u01b3\u01b6\u00057\u0000\u0000\u01b4\u01b6\b\u0002\u0000\u0000"+
		"\u01b5\u01ac\u0001\u0000\u0000\u0000\u01b5\u01b4\u0001\u0000\u0000\u0000"+
		"\u01b6O\u0001\u0000\u0000\u0000\u01b7\u01be\u0003R)\u0000\u01b8\u01be"+
		"\u0005@\u0000\u0000\u01b9\u01be\u0005A\u0000\u0000\u01ba\u01be\u0005\""+
		"\u0000\u0000\u01bb\u01be\u0005#\u0000\u0000\u01bc\u01be\u0005$\u0000\u0000"+
		"\u01bd\u01b7\u0001\u0000\u0000\u0000\u01bd\u01b8\u0001\u0000\u0000\u0000"+
		"\u01bd\u01b9\u0001\u0000\u0000\u0000\u01bd\u01ba\u0001\u0000\u0000\u0000"+
		"\u01bd\u01bb\u0001\u0000\u0000\u0000\u01bd\u01bc\u0001\u0000\u0000\u0000"+
		"\u01beQ\u0001\u0000\u0000\u0000\u01bf\u01c0\u0007\b\u0000\u0000\u01c0"+
		"S\u0001\u0000\u0000\u0000\u01c1\u01c2\u0007\t\u0000\u0000\u01c2U\u0001"+
		"\u0000\u0000\u00004Yclrx\u007f\u0086\u0091\u0096\u009e\u00a9\u00b7\u00bb"+
		"\u00bf\u00cd\u00d6\u00dd\u00e4\u00ed\u00f2\u00fa\u0100\u0110\u0118\u011e"+
		"\u0122\u0128\u0131\u0137\u013f\u0145\u0149\u014c\u0153\u0157\u0159\u0163"+
		"\u0165\u016d\u0175\u017b\u0184\u0189\u018c\u0192\u0199\u019f\u01a5\u01aa"+
		"\u01b0\u01b5\u01bd";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}