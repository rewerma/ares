// Generated from /Users/rewerma/Develop/git_workspace/ares/ares-parser/src/main/java/com/github/ares/parser/antlr4/plsql/PlSqlParser.g4 by ANTLR 4.13.1
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
	 * Visit a parse tree produced by {@link PlSqlParser#unit_statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUnit_statement(PlSqlParser.Unit_statementContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#function_body}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFunction_body(PlSqlParser.Function_bodyContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#create_procedure_body}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCreate_procedure_body(PlSqlParser.Create_procedure_bodyContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#create_function_body}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCreate_function_body(PlSqlParser.Create_function_bodyContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#anonymous_body}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAnonymous_body(PlSqlParser.Anonymous_bodyContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#create_table_as}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCreate_table_as(PlSqlParser.Create_table_asContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#create_table_as2}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCreate_table_as2(PlSqlParser.Create_table_as2Context ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#create_table}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCreate_table(PlSqlParser.Create_tableContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#create_with}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCreate_with(PlSqlParser.Create_withContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#create_options}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCreate_options(PlSqlParser.Create_optionsContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#option_}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitOption_(PlSqlParser.Option_Context ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#table_name}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTable_name(PlSqlParser.Table_nameContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#relational_table}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRelational_table(PlSqlParser.Relational_tableContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#relational_property}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRelational_property(PlSqlParser.Relational_propertyContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#column_definition}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitColumn_definition(PlSqlParser.Column_definitionContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#truncate_table_block}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTruncate_table_block(PlSqlParser.Truncate_table_blockContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#select_block}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSelect_block(PlSqlParser.Select_blockContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#update_block}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUpdate_block(PlSqlParser.Update_blockContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#delete_block}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDelete_block(PlSqlParser.Delete_blockContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#insert_block}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitInsert_block(PlSqlParser.Insert_blockContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#merge_block}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitMerge_block(PlSqlParser.Merge_blockContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#set_bleck}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSet_bleck(PlSqlParser.Set_bleckContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#parameter}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitParameter(PlSqlParser.ParameterContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#default_value_part}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDefault_value_part(PlSqlParser.Default_value_partContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#seq_of_declare_specs}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSeq_of_declare_specs(PlSqlParser.Seq_of_declare_specsContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#declare_spec}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDeclare_spec(PlSqlParser.Declare_specContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#variable_declaration}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitVariable_declaration(PlSqlParser.Variable_declarationContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#seq_of_statements}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSeq_of_statements(PlSqlParser.Seq_of_statementsContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitStatement(PlSqlParser.StatementContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#assignment_statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAssignment_statement(PlSqlParser.Assignment_statementContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#continue_statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitContinue_statement(PlSqlParser.Continue_statementContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#exit_statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitExit_statement(PlSqlParser.Exit_statementContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#if_statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIf_statement(PlSqlParser.If_statementContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#elsif_part}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitElsif_part(PlSqlParser.Elsif_partContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#else_part}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitElse_part(PlSqlParser.Else_partContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#loop_statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLoop_statement(PlSqlParser.Loop_statementContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#cursor_loop_param}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCursor_loop_param(PlSqlParser.Cursor_loop_paramContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#select_statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSelect_statement(PlSqlParser.Select_statementContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#lower_bound}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLower_bound(PlSqlParser.Lower_boundContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#upper_bound}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUpper_bound(PlSqlParser.Upper_boundContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#raise_statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRaise_statement(PlSqlParser.Raise_statementContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#return_statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitReturn_statement(PlSqlParser.Return_statementContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#call_statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCall_statement(PlSqlParser.Call_statementContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#body}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBody(PlSqlParser.BodyContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#exception_handler}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitException_handler(PlSqlParser.Exception_handlerContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#sql_statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSql_statement(PlSqlParser.Sql_statementContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#data_manipulation_language_statements}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitData_manipulation_language_statements(PlSqlParser.Data_manipulation_language_statementsContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#update_statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUpdate_statement(PlSqlParser.Update_statementContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#delete_statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDelete_statement(PlSqlParser.Delete_statementContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#insert_statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitInsert_statement(PlSqlParser.Insert_statementContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#merge_statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitMerge_statement(PlSqlParser.Merge_statementContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#condition}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCondition(PlSqlParser.ConditionContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#expressions}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitExpressions(PlSqlParser.ExpressionsContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#expression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitExpression(PlSqlParser.ExpressionContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#logical_expression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLogical_expression(PlSqlParser.Logical_expressionContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#unary_logical_expression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUnary_logical_expression(PlSqlParser.Unary_logical_expressionContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#multiset_expression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitMultiset_expression(PlSqlParser.Multiset_expressionContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#relational_expression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRelational_expression(PlSqlParser.Relational_expressionContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#compound_expression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCompound_expression(PlSqlParser.Compound_expressionContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#relational_operator}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRelational_operator(PlSqlParser.Relational_operatorContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#in_elements}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIn_elements(PlSqlParser.In_elementsContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#between_elements}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBetween_elements(PlSqlParser.Between_elementsContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#concatenation}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitConcatenation(PlSqlParser.ConcatenationContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#unary_expression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUnary_expression(PlSqlParser.Unary_expressionContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#atom}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAtom(PlSqlParser.AtomContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#routine_name}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRoutine_name(PlSqlParser.Routine_nameContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#parameter_name}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitParameter_name(PlSqlParser.Parameter_nameContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#procedure_name}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitProcedure_name(PlSqlParser.Procedure_nameContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#exception_name}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitException_name(PlSqlParser.Exception_nameContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#index_name}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIndex_name(PlSqlParser.Index_nameContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#record_name}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRecord_name(PlSqlParser.Record_nameContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#column_name}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitColumn_name(PlSqlParser.Column_nameContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#function_argument}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFunction_argument(PlSqlParser.Function_argumentContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#argument}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitArgument(PlSqlParser.ArgumentContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#type_spec}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitType_spec(PlSqlParser.Type_specContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#datatype}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDatatype(PlSqlParser.DatatypeContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#precision_part}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPrecision_part(PlSqlParser.Precision_partContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#native_datatype_element}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNative_datatype_element(PlSqlParser.Native_datatype_elementContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#bind_variable}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBind_variable(PlSqlParser.Bind_variableContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#general_element}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitGeneral_element(PlSqlParser.General_elementContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#constant}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitConstant(PlSqlParser.ConstantContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#numeric}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNumeric(PlSqlParser.NumericContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#quoted_string}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitQuoted_string(PlSqlParser.Quoted_stringContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#identifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIdentifier(PlSqlParser.IdentifierContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#id_expression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitId_expression(PlSqlParser.Id_expressionContext ctx);
	/**
	 * Visit a parse tree produced by {@link PlSqlParser#regular_id}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRegular_id(PlSqlParser.Regular_idContext ctx);
}
