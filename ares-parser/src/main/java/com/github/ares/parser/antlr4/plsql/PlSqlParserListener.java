// Generated from PlSqlParser.g4 by ANTLR 4.13.1
package com.github.ares.parser.antlr4.plsql;
import com.github.ares.org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link PlSqlParser}.
 */
public interface PlSqlParserListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#sql_script}.
	 * @param ctx the parse tree
	 */
	void enterSql_script(PlSqlParser.Sql_scriptContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#sql_script}.
	 * @param ctx the parse tree
	 */
	void exitSql_script(PlSqlParser.Sql_scriptContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterStatement(PlSqlParser.StatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitStatement(PlSqlParser.StatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#terminated_statement}.
	 * @param ctx the parse tree
	 */
	void enterTerminated_statement(PlSqlParser.Terminated_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#terminated_statement}.
	 * @param ctx the parse tree
	 */
	void exitTerminated_statement(PlSqlParser.Terminated_statementContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#body}.
	 * @param ctx the parse tree
	 */
	void enterBody(PlSqlParser.BodyContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#body}.
	 * @param ctx the parse tree
	 */
	void exitBody(PlSqlParser.BodyContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#def_statement}.
	 * @param ctx the parse tree
	 */
	void enterDef_statement(PlSqlParser.Def_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#def_statement}.
	 * @param ctx the parse tree
	 */
	void exitDef_statement(PlSqlParser.Def_statementContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#assignment_statement}.
	 * @param ctx the parse tree
	 */
	void enterAssignment_statement(PlSqlParser.Assignment_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#assignment_statement}.
	 * @param ctx the parse tree
	 */
	void exitAssignment_statement(PlSqlParser.Assignment_statementContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#call_statement}.
	 * @param ctx the parse tree
	 */
	void enterCall_statement(PlSqlParser.Call_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#call_statement}.
	 * @param ctx the parse tree
	 */
	void exitCall_statement(PlSqlParser.Call_statementContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#set_statement}.
	 * @param ctx the parse tree
	 */
	void enterSet_statement(PlSqlParser.Set_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#set_statement}.
	 * @param ctx the parse tree
	 */
	void exitSet_statement(PlSqlParser.Set_statementContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#transaction_statement}.
	 * @param ctx the parse tree
	 */
	void enterTransaction_statement(PlSqlParser.Transaction_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#transaction_statement}.
	 * @param ctx the parse tree
	 */
	void exitTransaction_statement(PlSqlParser.Transaction_statementContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#break_statement}.
	 * @param ctx the parse tree
	 */
	void enterBreak_statement(PlSqlParser.Break_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#break_statement}.
	 * @param ctx the parse tree
	 */
	void exitBreak_statement(PlSqlParser.Break_statementContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#continue_statement}.
	 * @param ctx the parse tree
	 */
	void enterContinue_statement(PlSqlParser.Continue_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#continue_statement}.
	 * @param ctx the parse tree
	 */
	void exitContinue_statement(PlSqlParser.Continue_statementContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#raise_statement}.
	 * @param ctx the parse tree
	 */
	void enterRaise_statement(PlSqlParser.Raise_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#raise_statement}.
	 * @param ctx the parse tree
	 */
	void exitRaise_statement(PlSqlParser.Raise_statementContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#if_statement}.
	 * @param ctx the parse tree
	 */
	void enterIf_statement(PlSqlParser.If_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#if_statement}.
	 * @param ctx the parse tree
	 */
	void exitIf_statement(PlSqlParser.If_statementContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#elsif_clause}.
	 * @param ctx the parse tree
	 */
	void enterElsif_clause(PlSqlParser.Elsif_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#elsif_clause}.
	 * @param ctx the parse tree
	 */
	void exitElsif_clause(PlSqlParser.Elsif_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#else_clause}.
	 * @param ctx the parse tree
	 */
	void enterElse_clause(PlSqlParser.Else_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#else_clause}.
	 * @param ctx the parse tree
	 */
	void exitElse_clause(PlSqlParser.Else_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#while_statement}.
	 * @param ctx the parse tree
	 */
	void enterWhile_statement(PlSqlParser.While_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#while_statement}.
	 * @param ctx the parse tree
	 */
	void exitWhile_statement(PlSqlParser.While_statementContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#for_statement}.
	 * @param ctx the parse tree
	 */
	void enterFor_statement(PlSqlParser.For_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#for_statement}.
	 * @param ctx the parse tree
	 */
	void exitFor_statement(PlSqlParser.For_statementContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#for_source}.
	 * @param ctx the parse tree
	 */
	void enterFor_source(PlSqlParser.For_sourceContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#for_source}.
	 * @param ctx the parse tree
	 */
	void exitFor_source(PlSqlParser.For_sourceContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#for_query}.
	 * @param ctx the parse tree
	 */
	void enterFor_query(PlSqlParser.For_queryContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#for_query}.
	 * @param ctx the parse tree
	 */
	void exitFor_query(PlSqlParser.For_queryContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#for_query_item}.
	 * @param ctx the parse tree
	 */
	void enterFor_query_item(PlSqlParser.For_query_itemContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#for_query_item}.
	 * @param ctx the parse tree
	 */
	void exitFor_query_item(PlSqlParser.For_query_itemContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#try_statement}.
	 * @param ctx the parse tree
	 */
	void enterTry_statement(PlSqlParser.Try_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#try_statement}.
	 * @param ctx the parse tree
	 */
	void exitTry_statement(PlSqlParser.Try_statementContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#sql_statement}.
	 * @param ctx the parse tree
	 */
	void enterSql_statement(PlSqlParser.Sql_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#sql_statement}.
	 * @param ctx the parse tree
	 */
	void exitSql_statement(PlSqlParser.Sql_statementContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#sql_prefix}.
	 * @param ctx the parse tree
	 */
	void enterSql_prefix(PlSqlParser.Sql_prefixContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#sql_prefix}.
	 * @param ctx the parse tree
	 */
	void exitSql_prefix(PlSqlParser.Sql_prefixContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#raw_token}.
	 * @param ctx the parse tree
	 */
	void enterRaw_token(PlSqlParser.Raw_tokenContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#raw_token}.
	 * @param ctx the parse tree
	 */
	void exitRaw_token(PlSqlParser.Raw_tokenContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#expression}.
	 * @param ctx the parse tree
	 */
	void enterExpression(PlSqlParser.ExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#expression}.
	 * @param ctx the parse tree
	 */
	void exitExpression(PlSqlParser.ExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#or_expression}.
	 * @param ctx the parse tree
	 */
	void enterOr_expression(PlSqlParser.Or_expressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#or_expression}.
	 * @param ctx the parse tree
	 */
	void exitOr_expression(PlSqlParser.Or_expressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#and_expression}.
	 * @param ctx the parse tree
	 */
	void enterAnd_expression(PlSqlParser.And_expressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#and_expression}.
	 * @param ctx the parse tree
	 */
	void exitAnd_expression(PlSqlParser.And_expressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#not_expression}.
	 * @param ctx the parse tree
	 */
	void enterNot_expression(PlSqlParser.Not_expressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#not_expression}.
	 * @param ctx the parse tree
	 */
	void exitNot_expression(PlSqlParser.Not_expressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#predicate}.
	 * @param ctx the parse tree
	 */
	void enterPredicate(PlSqlParser.PredicateContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#predicate}.
	 * @param ctx the parse tree
	 */
	void exitPredicate(PlSqlParser.PredicateContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#predicate_tail}.
	 * @param ctx the parse tree
	 */
	void enterPredicate_tail(PlSqlParser.Predicate_tailContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#predicate_tail}.
	 * @param ctx the parse tree
	 */
	void exitPredicate_tail(PlSqlParser.Predicate_tailContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#relational_operator}.
	 * @param ctx the parse tree
	 */
	void enterRelational_operator(PlSqlParser.Relational_operatorContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#relational_operator}.
	 * @param ctx the parse tree
	 */
	void exitRelational_operator(PlSqlParser.Relational_operatorContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#concatenation}.
	 * @param ctx the parse tree
	 */
	void enterConcatenation(PlSqlParser.ConcatenationContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#concatenation}.
	 * @param ctx the parse tree
	 */
	void exitConcatenation(PlSqlParser.ConcatenationContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#additive}.
	 * @param ctx the parse tree
	 */
	void enterAdditive(PlSqlParser.AdditiveContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#additive}.
	 * @param ctx the parse tree
	 */
	void exitAdditive(PlSqlParser.AdditiveContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#multiplicative}.
	 * @param ctx the parse tree
	 */
	void enterMultiplicative(PlSqlParser.MultiplicativeContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#multiplicative}.
	 * @param ctx the parse tree
	 */
	void exitMultiplicative(PlSqlParser.MultiplicativeContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#unary}.
	 * @param ctx the parse tree
	 */
	void enterUnary(PlSqlParser.UnaryContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#unary}.
	 * @param ctx the parse tree
	 */
	void exitUnary(PlSqlParser.UnaryContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#atom}.
	 * @param ctx the parse tree
	 */
	void enterAtom(PlSqlParser.AtomContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#atom}.
	 * @param ctx the parse tree
	 */
	void exitAtom(PlSqlParser.AtomContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#func_args}.
	 * @param ctx the parse tree
	 */
	void enterFunc_args(PlSqlParser.Func_argsContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#func_args}.
	 * @param ctx the parse tree
	 */
	void exitFunc_args(PlSqlParser.Func_argsContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#func_arg}.
	 * @param ctx the parse tree
	 */
	void enterFunc_arg(PlSqlParser.Func_argContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#func_arg}.
	 * @param ctx the parse tree
	 */
	void exitFunc_arg(PlSqlParser.Func_argContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#func_top}.
	 * @param ctx the parse tree
	 */
	void enterFunc_top(PlSqlParser.Func_topContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#func_top}.
	 * @param ctx the parse tree
	 */
	void exitFunc_top(PlSqlParser.Func_topContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#func_nested}.
	 * @param ctx the parse tree
	 */
	void enterFunc_nested(PlSqlParser.Func_nestedContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#func_nested}.
	 * @param ctx the parse tree
	 */
	void exitFunc_nested(PlSqlParser.Func_nestedContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#literal}.
	 * @param ctx the parse tree
	 */
	void enterLiteral(PlSqlParser.LiteralContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#literal}.
	 * @param ctx the parse tree
	 */
	void exitLiteral(PlSqlParser.LiteralContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#numeric}.
	 * @param ctx the parse tree
	 */
	void enterNumeric(PlSqlParser.NumericContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#numeric}.
	 * @param ctx the parse tree
	 */
	void exitNumeric(PlSqlParser.NumericContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#identifier}.
	 * @param ctx the parse tree
	 */
	void enterIdentifier(PlSqlParser.IdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#identifier}.
	 * @param ctx the parse tree
	 */
	void exitIdentifier(PlSqlParser.IdentifierContext ctx);
}