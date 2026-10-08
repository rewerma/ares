// Generated from PlSqlParser.g4 by ANTLR 4.13.1
package com.github.ares.parser.antlr4.plsql;
import com.github.ares.org.antlr.v4.runtime.tree.ParseTreeVisitor;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link PlSqlParser}.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
public interface PlSqlParserVisitor<T> extends ParseTreeVisitor<T> {
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#sql_script}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSql_script(PlSqlParser.Sql_scriptContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitStatement(PlSqlParser.StatementContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#terminated_statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTerminated_statement(PlSqlParser.Terminated_statementContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#body}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBody(PlSqlParser.BodyContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#def_statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDef_statement(PlSqlParser.Def_statementContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#assignment_statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAssignment_statement(PlSqlParser.Assignment_statementContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#call_statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCall_statement(PlSqlParser.Call_statementContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#set_statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSet_statement(PlSqlParser.Set_statementContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#transaction_statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTransaction_statement(PlSqlParser.Transaction_statementContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#break_statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBreak_statement(PlSqlParser.Break_statementContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#continue_statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitContinue_statement(PlSqlParser.Continue_statementContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#raise_statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRaise_statement(PlSqlParser.Raise_statementContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#if_statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIf_statement(PlSqlParser.If_statementContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#elsif_clause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitElsif_clause(PlSqlParser.Elsif_clauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#else_clause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitElse_clause(PlSqlParser.Else_clauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#while_statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitWhile_statement(PlSqlParser.While_statementContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#for_statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFor_statement(PlSqlParser.For_statementContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#for_source}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFor_source(PlSqlParser.For_sourceContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#for_query}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFor_query(PlSqlParser.For_queryContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#for_query_item}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFor_query_item(PlSqlParser.For_query_itemContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#try_statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTry_statement(PlSqlParser.Try_statementContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#sql_statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSql_statement(PlSqlParser.Sql_statementContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#sql_prefix}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSql_prefix(PlSqlParser.Sql_prefixContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#raw_token}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRaw_token(PlSqlParser.Raw_tokenContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#expression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitExpression(PlSqlParser.ExpressionContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#or_expression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitOr_expression(PlSqlParser.Or_expressionContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#and_expression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAnd_expression(PlSqlParser.And_expressionContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#not_expression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNot_expression(PlSqlParser.Not_expressionContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#predicate}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPredicate(PlSqlParser.PredicateContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#predicate_tail}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPredicate_tail(PlSqlParser.Predicate_tailContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#relational_operator}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRelational_operator(PlSqlParser.Relational_operatorContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#concatenation}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitConcatenation(PlSqlParser.ConcatenationContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#additive}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAdditive(PlSqlParser.AdditiveContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#multiplicative}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitMultiplicative(PlSqlParser.MultiplicativeContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#unary}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUnary(PlSqlParser.UnaryContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#atom}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAtom(PlSqlParser.AtomContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#func_args}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFunc_args(PlSqlParser.Func_argsContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#func_arg}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFunc_arg(PlSqlParser.Func_argContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#func_top}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFunc_top(PlSqlParser.Func_topContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#func_nested}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFunc_nested(PlSqlParser.Func_nestedContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#literal}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLiteral(PlSqlParser.LiteralContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#numeric}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNumeric(PlSqlParser.NumericContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#identifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIdentifier(PlSqlParser.IdentifierContext ctx);
}