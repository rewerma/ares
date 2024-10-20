// Generated from /Users/rewerma/Develop/git_aliyun/ares/ares-parser/src/main/java/com/github/ares/parser/antlr4/plsql/PlSqlParser.g4 by ANTLR 4.12.0
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
	 * Enter a parse tree produced by {@link PlSqlParser#unit_statement}.
	 * @param ctx the parse tree
	 */
	void enterUnit_statement(PlSqlParser.Unit_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#unit_statement}.
	 * @param ctx the parse tree
	 */
	void exitUnit_statement(PlSqlParser.Unit_statementContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#function_body}.
	 * @param ctx the parse tree
	 */
	void enterFunction_body(PlSqlParser.Function_bodyContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#function_body}.
	 * @param ctx the parse tree
	 */
	void exitFunction_body(PlSqlParser.Function_bodyContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#create_procedure_body}.
	 * @param ctx the parse tree
	 */
	void enterCreate_procedure_body(PlSqlParser.Create_procedure_bodyContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#create_procedure_body}.
	 * @param ctx the parse tree
	 */
	void exitCreate_procedure_body(PlSqlParser.Create_procedure_bodyContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#create_function_body}.
	 * @param ctx the parse tree
	 */
	void enterCreate_function_body(PlSqlParser.Create_function_bodyContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#create_function_body}.
	 * @param ctx the parse tree
	 */
	void exitCreate_function_body(PlSqlParser.Create_function_bodyContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#anonymous_body}.
	 * @param ctx the parse tree
	 */
	void enterAnonymous_body(PlSqlParser.Anonymous_bodyContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#anonymous_body}.
	 * @param ctx the parse tree
	 */
	void exitAnonymous_body(PlSqlParser.Anonymous_bodyContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#create_table_as}.
	 * @param ctx the parse tree
	 */
	void enterCreate_table_as(PlSqlParser.Create_table_asContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#create_table_as}.
	 * @param ctx the parse tree
	 */
	void exitCreate_table_as(PlSqlParser.Create_table_asContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#create_table_as2}.
	 * @param ctx the parse tree
	 */
	void enterCreate_table_as2(PlSqlParser.Create_table_as2Context ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#create_table_as2}.
	 * @param ctx the parse tree
	 */
	void exitCreate_table_as2(PlSqlParser.Create_table_as2Context ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#create_table}.
	 * @param ctx the parse tree
	 */
	void enterCreate_table(PlSqlParser.Create_tableContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#create_table}.
	 * @param ctx the parse tree
	 */
	void exitCreate_table(PlSqlParser.Create_tableContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#create_with}.
	 * @param ctx the parse tree
	 */
	void enterCreate_with(PlSqlParser.Create_withContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#create_with}.
	 * @param ctx the parse tree
	 */
	void exitCreate_with(PlSqlParser.Create_withContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#create_options}.
	 * @param ctx the parse tree
	 */
	void enterCreate_options(PlSqlParser.Create_optionsContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#create_options}.
	 * @param ctx the parse tree
	 */
	void exitCreate_options(PlSqlParser.Create_optionsContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#option_}.
	 * @param ctx the parse tree
	 */
	void enterOption_(PlSqlParser.Option_Context ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#option_}.
	 * @param ctx the parse tree
	 */
	void exitOption_(PlSqlParser.Option_Context ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#table_name}.
	 * @param ctx the parse tree
	 */
	void enterTable_name(PlSqlParser.Table_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#table_name}.
	 * @param ctx the parse tree
	 */
	void exitTable_name(PlSqlParser.Table_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#relational_table}.
	 * @param ctx the parse tree
	 */
	void enterRelational_table(PlSqlParser.Relational_tableContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#relational_table}.
	 * @param ctx the parse tree
	 */
	void exitRelational_table(PlSqlParser.Relational_tableContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#relational_property}.
	 * @param ctx the parse tree
	 */
	void enterRelational_property(PlSqlParser.Relational_propertyContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#relational_property}.
	 * @param ctx the parse tree
	 */
	void exitRelational_property(PlSqlParser.Relational_propertyContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#column_definition}.
	 * @param ctx the parse tree
	 */
	void enterColumn_definition(PlSqlParser.Column_definitionContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#column_definition}.
	 * @param ctx the parse tree
	 */
	void exitColumn_definition(PlSqlParser.Column_definitionContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#column_collation_name}.
	 * @param ctx the parse tree
	 */
	void enterColumn_collation_name(PlSqlParser.Column_collation_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#column_collation_name}.
	 * @param ctx the parse tree
	 */
	void exitColumn_collation_name(PlSqlParser.Column_collation_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#identity_clause}.
	 * @param ctx the parse tree
	 */
	void enterIdentity_clause(PlSqlParser.Identity_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#identity_clause}.
	 * @param ctx the parse tree
	 */
	void exitIdentity_clause(PlSqlParser.Identity_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#identity_options_parentheses}.
	 * @param ctx the parse tree
	 */
	void enterIdentity_options_parentheses(PlSqlParser.Identity_options_parenthesesContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#identity_options_parentheses}.
	 * @param ctx the parse tree
	 */
	void exitIdentity_options_parentheses(PlSqlParser.Identity_options_parenthesesContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#identity_options}.
	 * @param ctx the parse tree
	 */
	void enterIdentity_options(PlSqlParser.Identity_optionsContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#identity_options}.
	 * @param ctx the parse tree
	 */
	void exitIdentity_options(PlSqlParser.Identity_optionsContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#encryption_spec}.
	 * @param ctx the parse tree
	 */
	void enterEncryption_spec(PlSqlParser.Encryption_specContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#encryption_spec}.
	 * @param ctx the parse tree
	 */
	void exitEncryption_spec(PlSqlParser.Encryption_specContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#truncate_table}.
	 * @param ctx the parse tree
	 */
	void enterTruncate_table(PlSqlParser.Truncate_tableContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#truncate_table}.
	 * @param ctx the parse tree
	 */
	void exitTruncate_table(PlSqlParser.Truncate_tableContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#rule_on_column}.
	 * @param ctx the parse tree
	 */
	void enterRule_on_column(PlSqlParser.Rule_on_columnContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#rule_on_column}.
	 * @param ctx the parse tree
	 */
	void exitRule_on_column(PlSqlParser.Rule_on_columnContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#parameter}.
	 * @param ctx the parse tree
	 */
	void enterParameter(PlSqlParser.ParameterContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#parameter}.
	 * @param ctx the parse tree
	 */
	void exitParameter(PlSqlParser.ParameterContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#default_value_part}.
	 * @param ctx the parse tree
	 */
	void enterDefault_value_part(PlSqlParser.Default_value_partContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#default_value_part}.
	 * @param ctx the parse tree
	 */
	void exitDefault_value_part(PlSqlParser.Default_value_partContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#seq_of_declare_specs}.
	 * @param ctx the parse tree
	 */
	void enterSeq_of_declare_specs(PlSqlParser.Seq_of_declare_specsContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#seq_of_declare_specs}.
	 * @param ctx the parse tree
	 */
	void exitSeq_of_declare_specs(PlSqlParser.Seq_of_declare_specsContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#declare_spec}.
	 * @param ctx the parse tree
	 */
	void enterDeclare_spec(PlSqlParser.Declare_specContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#declare_spec}.
	 * @param ctx the parse tree
	 */
	void exitDeclare_spec(PlSqlParser.Declare_specContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#variable_declaration}.
	 * @param ctx the parse tree
	 */
	void enterVariable_declaration(PlSqlParser.Variable_declarationContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#variable_declaration}.
	 * @param ctx the parse tree
	 */
	void exitVariable_declaration(PlSqlParser.Variable_declarationContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#seq_of_statements}.
	 * @param ctx the parse tree
	 */
	void enterSeq_of_statements(PlSqlParser.Seq_of_statementsContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#seq_of_statements}.
	 * @param ctx the parse tree
	 */
	void exitSeq_of_statements(PlSqlParser.Seq_of_statementsContext ctx);
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
	 * Enter a parse tree produced by {@link PlSqlParser#exit_statement}.
	 * @param ctx the parse tree
	 */
	void enterExit_statement(PlSqlParser.Exit_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#exit_statement}.
	 * @param ctx the parse tree
	 */
	void exitExit_statement(PlSqlParser.Exit_statementContext ctx);
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
	 * Enter a parse tree produced by {@link PlSqlParser#elsif_part}.
	 * @param ctx the parse tree
	 */
	void enterElsif_part(PlSqlParser.Elsif_partContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#elsif_part}.
	 * @param ctx the parse tree
	 */
	void exitElsif_part(PlSqlParser.Elsif_partContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#else_part}.
	 * @param ctx the parse tree
	 */
	void enterElse_part(PlSqlParser.Else_partContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#else_part}.
	 * @param ctx the parse tree
	 */
	void exitElse_part(PlSqlParser.Else_partContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#loop_statement}.
	 * @param ctx the parse tree
	 */
	void enterLoop_statement(PlSqlParser.Loop_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#loop_statement}.
	 * @param ctx the parse tree
	 */
	void exitLoop_statement(PlSqlParser.Loop_statementContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#cursor_loop_param}.
	 * @param ctx the parse tree
	 */
	void enterCursor_loop_param(PlSqlParser.Cursor_loop_paramContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#cursor_loop_param}.
	 * @param ctx the parse tree
	 */
	void exitCursor_loop_param(PlSqlParser.Cursor_loop_paramContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#select_only_statement}.
	 * @param ctx the parse tree
	 */
	void enterSelect_only_statement(PlSqlParser.Select_only_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#select_only_statement}.
	 * @param ctx the parse tree
	 */
	void exitSelect_only_statement(PlSqlParser.Select_only_statementContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#lower_bound}.
	 * @param ctx the parse tree
	 */
	void enterLower_bound(PlSqlParser.Lower_boundContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#lower_bound}.
	 * @param ctx the parse tree
	 */
	void exitLower_bound(PlSqlParser.Lower_boundContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#upper_bound}.
	 * @param ctx the parse tree
	 */
	void enterUpper_bound(PlSqlParser.Upper_boundContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#upper_bound}.
	 * @param ctx the parse tree
	 */
	void exitUpper_bound(PlSqlParser.Upper_boundContext ctx);
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
	 * Enter a parse tree produced by {@link PlSqlParser#return_statement}.
	 * @param ctx the parse tree
	 */
	void enterReturn_statement(PlSqlParser.Return_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#return_statement}.
	 * @param ctx the parse tree
	 */
	void exitReturn_statement(PlSqlParser.Return_statementContext ctx);
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
	 * Enter a parse tree produced by {@link PlSqlParser#exception_handler}.
	 * @param ctx the parse tree
	 */
	void enterException_handler(PlSqlParser.Exception_handlerContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#exception_handler}.
	 * @param ctx the parse tree
	 */
	void exitException_handler(PlSqlParser.Exception_handlerContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#block}.
	 * @param ctx the parse tree
	 */
	void enterBlock(PlSqlParser.BlockContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#block}.
	 * @param ctx the parse tree
	 */
	void exitBlock(PlSqlParser.BlockContext ctx);
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
	 * Enter a parse tree produced by {@link PlSqlParser#data_manipulation_language_statements}.
	 * @param ctx the parse tree
	 */
	void enterData_manipulation_language_statements(PlSqlParser.Data_manipulation_language_statementsContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#data_manipulation_language_statements}.
	 * @param ctx the parse tree
	 */
	void exitData_manipulation_language_statements(PlSqlParser.Data_manipulation_language_statementsContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#select_statement}.
	 * @param ctx the parse tree
	 */
	void enterSelect_statement(PlSqlParser.Select_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#select_statement}.
	 * @param ctx the parse tree
	 */
	void exitSelect_statement(PlSqlParser.Select_statementContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#subquery}.
	 * @param ctx the parse tree
	 */
	void enterSubquery(PlSqlParser.SubqueryContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#subquery}.
	 * @param ctx the parse tree
	 */
	void exitSubquery(PlSqlParser.SubqueryContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#subquery_basic_elements}.
	 * @param ctx the parse tree
	 */
	void enterSubquery_basic_elements(PlSqlParser.Subquery_basic_elementsContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#subquery_basic_elements}.
	 * @param ctx the parse tree
	 */
	void exitSubquery_basic_elements(PlSqlParser.Subquery_basic_elementsContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#subquery_operation_part}.
	 * @param ctx the parse tree
	 */
	void enterSubquery_operation_part(PlSqlParser.Subquery_operation_partContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#subquery_operation_part}.
	 * @param ctx the parse tree
	 */
	void exitSubquery_operation_part(PlSqlParser.Subquery_operation_partContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#select_block}.
	 * @param ctx the parse tree
	 */
	void enterSelect_block(PlSqlParser.Select_blockContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#select_block}.
	 * @param ctx the parse tree
	 */
	void exitSelect_block(PlSqlParser.Select_blockContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#query_block}.
	 * @param ctx the parse tree
	 */
	void enterQuery_block(PlSqlParser.Query_blockContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#query_block}.
	 * @param ctx the parse tree
	 */
	void exitQuery_block(PlSqlParser.Query_blockContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#rollup_cube_clause}.
	 * @param ctx the parse tree
	 */
	void enterRollup_cube_clause(PlSqlParser.Rollup_cube_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#rollup_cube_clause}.
	 * @param ctx the parse tree
	 */
	void exitRollup_cube_clause(PlSqlParser.Rollup_cube_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#grouping_sets_elements}.
	 * @param ctx the parse tree
	 */
	void enterGrouping_sets_elements(PlSqlParser.Grouping_sets_elementsContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#grouping_sets_elements}.
	 * @param ctx the parse tree
	 */
	void exitGrouping_sets_elements(PlSqlParser.Grouping_sets_elementsContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#update_block}.
	 * @param ctx the parse tree
	 */
	void enterUpdate_block(PlSqlParser.Update_blockContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#update_block}.
	 * @param ctx the parse tree
	 */
	void exitUpdate_block(PlSqlParser.Update_blockContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#update_statement}.
	 * @param ctx the parse tree
	 */
	void enterUpdate_statement(PlSqlParser.Update_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#update_statement}.
	 * @param ctx the parse tree
	 */
	void exitUpdate_statement(PlSqlParser.Update_statementContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#delete_block}.
	 * @param ctx the parse tree
	 */
	void enterDelete_block(PlSqlParser.Delete_blockContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#delete_block}.
	 * @param ctx the parse tree
	 */
	void exitDelete_block(PlSqlParser.Delete_blockContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#delete_statement}.
	 * @param ctx the parse tree
	 */
	void enterDelete_statement(PlSqlParser.Delete_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#delete_statement}.
	 * @param ctx the parse tree
	 */
	void exitDelete_statement(PlSqlParser.Delete_statementContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#insert_block}.
	 * @param ctx the parse tree
	 */
	void enterInsert_block(PlSqlParser.Insert_blockContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#insert_block}.
	 * @param ctx the parse tree
	 */
	void exitInsert_block(PlSqlParser.Insert_blockContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#insert_statement}.
	 * @param ctx the parse tree
	 */
	void enterInsert_statement(PlSqlParser.Insert_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#insert_statement}.
	 * @param ctx the parse tree
	 */
	void exitInsert_statement(PlSqlParser.Insert_statementContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#set_bleck}.
	 * @param ctx the parse tree
	 */
	void enterSet_bleck(PlSqlParser.Set_bleckContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#set_bleck}.
	 * @param ctx the parse tree
	 */
	void exitSet_bleck(PlSqlParser.Set_bleckContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#truncate_table_block}.
	 * @param ctx the parse tree
	 */
	void enterTruncate_table_block(PlSqlParser.Truncate_table_blockContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#truncate_table_block}.
	 * @param ctx the parse tree
	 */
	void exitTruncate_table_block(PlSqlParser.Truncate_table_blockContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#truncate_table_block2}.
	 * @param ctx the parse tree
	 */
	void enterTruncate_table_block2(PlSqlParser.Truncate_table_block2Context ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#truncate_table_block2}.
	 * @param ctx the parse tree
	 */
	void exitTruncate_table_block2(PlSqlParser.Truncate_table_block2Context ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#merge_block}.
	 * @param ctx the parse tree
	 */
	void enterMerge_block(PlSqlParser.Merge_blockContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#merge_block}.
	 * @param ctx the parse tree
	 */
	void exitMerge_block(PlSqlParser.Merge_blockContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#merge_statement}.
	 * @param ctx the parse tree
	 */
	void enterMerge_statement(PlSqlParser.Merge_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#merge_statement}.
	 * @param ctx the parse tree
	 */
	void exitMerge_statement(PlSqlParser.Merge_statementContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#condition}.
	 * @param ctx the parse tree
	 */
	void enterCondition(PlSqlParser.ConditionContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#condition}.
	 * @param ctx the parse tree
	 */
	void exitCondition(PlSqlParser.ConditionContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#expressions}.
	 * @param ctx the parse tree
	 */
	void enterExpressions(PlSqlParser.ExpressionsContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#expressions}.
	 * @param ctx the parse tree
	 */
	void exitExpressions(PlSqlParser.ExpressionsContext ctx);
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
	 * Enter a parse tree produced by {@link PlSqlParser#logical_expression}.
	 * @param ctx the parse tree
	 */
	void enterLogical_expression(PlSqlParser.Logical_expressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#logical_expression}.
	 * @param ctx the parse tree
	 */
	void exitLogical_expression(PlSqlParser.Logical_expressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#unary_logical_expression}.
	 * @param ctx the parse tree
	 */
	void enterUnary_logical_expression(PlSqlParser.Unary_logical_expressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#unary_logical_expression}.
	 * @param ctx the parse tree
	 */
	void exitUnary_logical_expression(PlSqlParser.Unary_logical_expressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#logical_operation}.
	 * @param ctx the parse tree
	 */
	void enterLogical_operation(PlSqlParser.Logical_operationContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#logical_operation}.
	 * @param ctx the parse tree
	 */
	void exitLogical_operation(PlSqlParser.Logical_operationContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#multiset_expression}.
	 * @param ctx the parse tree
	 */
	void enterMultiset_expression(PlSqlParser.Multiset_expressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#multiset_expression}.
	 * @param ctx the parse tree
	 */
	void exitMultiset_expression(PlSqlParser.Multiset_expressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#relational_expression}.
	 * @param ctx the parse tree
	 */
	void enterRelational_expression(PlSqlParser.Relational_expressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#relational_expression}.
	 * @param ctx the parse tree
	 */
	void exitRelational_expression(PlSqlParser.Relational_expressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#compound_expression}.
	 * @param ctx the parse tree
	 */
	void enterCompound_expression(PlSqlParser.Compound_expressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#compound_expression}.
	 * @param ctx the parse tree
	 */
	void exitCompound_expression(PlSqlParser.Compound_expressionContext ctx);
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
	 * Enter a parse tree produced by {@link PlSqlParser#in_elements}.
	 * @param ctx the parse tree
	 */
	void enterIn_elements(PlSqlParser.In_elementsContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#in_elements}.
	 * @param ctx the parse tree
	 */
	void exitIn_elements(PlSqlParser.In_elementsContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#between_elements}.
	 * @param ctx the parse tree
	 */
	void enterBetween_elements(PlSqlParser.Between_elementsContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#between_elements}.
	 * @param ctx the parse tree
	 */
	void exitBetween_elements(PlSqlParser.Between_elementsContext ctx);
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
	 * Enter a parse tree produced by {@link PlSqlParser#interval_expression}.
	 * @param ctx the parse tree
	 */
	void enterInterval_expression(PlSqlParser.Interval_expressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#interval_expression}.
	 * @param ctx the parse tree
	 */
	void exitInterval_expression(PlSqlParser.Interval_expressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#model_expression}.
	 * @param ctx the parse tree
	 */
	void enterModel_expression(PlSqlParser.Model_expressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#model_expression}.
	 * @param ctx the parse tree
	 */
	void exitModel_expression(PlSqlParser.Model_expressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#unary_expression}.
	 * @param ctx the parse tree
	 */
	void enterUnary_expression(PlSqlParser.Unary_expressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#unary_expression}.
	 * @param ctx the parse tree
	 */
	void exitUnary_expression(PlSqlParser.Unary_expressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#other_function}.
	 * @param ctx the parse tree
	 */
	void enterOther_function(PlSqlParser.Other_functionContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#other_function}.
	 * @param ctx the parse tree
	 */
	void exitOther_function(PlSqlParser.Other_functionContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#case_statement}.
	 * @param ctx the parse tree
	 */
	void enterCase_statement(PlSqlParser.Case_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#case_statement}.
	 * @param ctx the parse tree
	 */
	void exitCase_statement(PlSqlParser.Case_statementContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#simple_case_statement}.
	 * @param ctx the parse tree
	 */
	void enterSimple_case_statement(PlSqlParser.Simple_case_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#simple_case_statement}.
	 * @param ctx the parse tree
	 */
	void exitSimple_case_statement(PlSqlParser.Simple_case_statementContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#simple_case_when_part}.
	 * @param ctx the parse tree
	 */
	void enterSimple_case_when_part(PlSqlParser.Simple_case_when_partContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#simple_case_when_part}.
	 * @param ctx the parse tree
	 */
	void exitSimple_case_when_part(PlSqlParser.Simple_case_when_partContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#searched_case_statement}.
	 * @param ctx the parse tree
	 */
	void enterSearched_case_statement(PlSqlParser.Searched_case_statementContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#searched_case_statement}.
	 * @param ctx the parse tree
	 */
	void exitSearched_case_statement(PlSqlParser.Searched_case_statementContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#searched_case_when_part}.
	 * @param ctx the parse tree
	 */
	void enterSearched_case_when_part(PlSqlParser.Searched_case_when_partContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#searched_case_when_part}.
	 * @param ctx the parse tree
	 */
	void exitSearched_case_when_part(PlSqlParser.Searched_case_when_partContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#case_else_part}.
	 * @param ctx the parse tree
	 */
	void enterCase_else_part(PlSqlParser.Case_else_partContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#case_else_part}.
	 * @param ctx the parse tree
	 */
	void exitCase_else_part(PlSqlParser.Case_else_partContext ctx);
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
	 * Enter a parse tree produced by {@link PlSqlParser#quantified_expression}.
	 * @param ctx the parse tree
	 */
	void enterQuantified_expression(PlSqlParser.Quantified_expressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#quantified_expression}.
	 * @param ctx the parse tree
	 */
	void exitQuantified_expression(PlSqlParser.Quantified_expressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#routine_name}.
	 * @param ctx the parse tree
	 */
	void enterRoutine_name(PlSqlParser.Routine_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#routine_name}.
	 * @param ctx the parse tree
	 */
	void exitRoutine_name(PlSqlParser.Routine_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#parameter_name}.
	 * @param ctx the parse tree
	 */
	void enterParameter_name(PlSqlParser.Parameter_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#parameter_name}.
	 * @param ctx the parse tree
	 */
	void exitParameter_name(PlSqlParser.Parameter_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#label_name}.
	 * @param ctx the parse tree
	 */
	void enterLabel_name(PlSqlParser.Label_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#label_name}.
	 * @param ctx the parse tree
	 */
	void exitLabel_name(PlSqlParser.Label_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#type_name}.
	 * @param ctx the parse tree
	 */
	void enterType_name(PlSqlParser.Type_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#type_name}.
	 * @param ctx the parse tree
	 */
	void exitType_name(PlSqlParser.Type_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#exception_name}.
	 * @param ctx the parse tree
	 */
	void enterException_name(PlSqlParser.Exception_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#exception_name}.
	 * @param ctx the parse tree
	 */
	void exitException_name(PlSqlParser.Exception_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#procedure_name}.
	 * @param ctx the parse tree
	 */
	void enterProcedure_name(PlSqlParser.Procedure_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#procedure_name}.
	 * @param ctx the parse tree
	 */
	void exitProcedure_name(PlSqlParser.Procedure_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#variable_name}.
	 * @param ctx the parse tree
	 */
	void enterVariable_name(PlSqlParser.Variable_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#variable_name}.
	 * @param ctx the parse tree
	 */
	void exitVariable_name(PlSqlParser.Variable_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#index_name}.
	 * @param ctx the parse tree
	 */
	void enterIndex_name(PlSqlParser.Index_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#index_name}.
	 * @param ctx the parse tree
	 */
	void exitIndex_name(PlSqlParser.Index_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#cursor_name}.
	 * @param ctx the parse tree
	 */
	void enterCursor_name(PlSqlParser.Cursor_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#cursor_name}.
	 * @param ctx the parse tree
	 */
	void exitCursor_name(PlSqlParser.Cursor_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#record_name}.
	 * @param ctx the parse tree
	 */
	void enterRecord_name(PlSqlParser.Record_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#record_name}.
	 * @param ctx the parse tree
	 */
	void exitRecord_name(PlSqlParser.Record_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#link_name}.
	 * @param ctx the parse tree
	 */
	void enterLink_name(PlSqlParser.Link_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#link_name}.
	 * @param ctx the parse tree
	 */
	void exitLink_name(PlSqlParser.Link_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#column_name}.
	 * @param ctx the parse tree
	 */
	void enterColumn_name(PlSqlParser.Column_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#column_name}.
	 * @param ctx the parse tree
	 */
	void exitColumn_name(PlSqlParser.Column_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#tableview_name}.
	 * @param ctx the parse tree
	 */
	void enterTableview_name(PlSqlParser.Tableview_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#tableview_name}.
	 * @param ctx the parse tree
	 */
	void exitTableview_name(PlSqlParser.Tableview_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#char_set_name}.
	 * @param ctx the parse tree
	 */
	void enterChar_set_name(PlSqlParser.Char_set_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#char_set_name}.
	 * @param ctx the parse tree
	 */
	void exitChar_set_name(PlSqlParser.Char_set_nameContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#function_argument}.
	 * @param ctx the parse tree
	 */
	void enterFunction_argument(PlSqlParser.Function_argumentContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#function_argument}.
	 * @param ctx the parse tree
	 */
	void exitFunction_argument(PlSqlParser.Function_argumentContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#argument}.
	 * @param ctx the parse tree
	 */
	void enterArgument(PlSqlParser.ArgumentContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#argument}.
	 * @param ctx the parse tree
	 */
	void exitArgument(PlSqlParser.ArgumentContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#type_spec}.
	 * @param ctx the parse tree
	 */
	void enterType_spec(PlSqlParser.Type_specContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#type_spec}.
	 * @param ctx the parse tree
	 */
	void exitType_spec(PlSqlParser.Type_specContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#datatype}.
	 * @param ctx the parse tree
	 */
	void enterDatatype(PlSqlParser.DatatypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#datatype}.
	 * @param ctx the parse tree
	 */
	void exitDatatype(PlSqlParser.DatatypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#precision_part}.
	 * @param ctx the parse tree
	 */
	void enterPrecision_part(PlSqlParser.Precision_partContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#precision_part}.
	 * @param ctx the parse tree
	 */
	void exitPrecision_part(PlSqlParser.Precision_partContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#native_datatype_element}.
	 * @param ctx the parse tree
	 */
	void enterNative_datatype_element(PlSqlParser.Native_datatype_elementContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#native_datatype_element}.
	 * @param ctx the parse tree
	 */
	void exitNative_datatype_element(PlSqlParser.Native_datatype_elementContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#bind_variable}.
	 * @param ctx the parse tree
	 */
	void enterBind_variable(PlSqlParser.Bind_variableContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#bind_variable}.
	 * @param ctx the parse tree
	 */
	void exitBind_variable(PlSqlParser.Bind_variableContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#general_element}.
	 * @param ctx the parse tree
	 */
	void enterGeneral_element(PlSqlParser.General_elementContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#general_element}.
	 * @param ctx the parse tree
	 */
	void exitGeneral_element(PlSqlParser.General_elementContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#general_element_part}.
	 * @param ctx the parse tree
	 */
	void enterGeneral_element_part(PlSqlParser.General_element_partContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#general_element_part}.
	 * @param ctx the parse tree
	 */
	void exitGeneral_element_part(PlSqlParser.General_element_partContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#table_element}.
	 * @param ctx the parse tree
	 */
	void enterTable_element(PlSqlParser.Table_elementContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#table_element}.
	 * @param ctx the parse tree
	 */
	void exitTable_element(PlSqlParser.Table_elementContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#constant}.
	 * @param ctx the parse tree
	 */
	void enterConstant(PlSqlParser.ConstantContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#constant}.
	 * @param ctx the parse tree
	 */
	void exitConstant(PlSqlParser.ConstantContext ctx);
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
	 * Enter a parse tree produced by {@link PlSqlParser#numeric_negative}.
	 * @param ctx the parse tree
	 */
	void enterNumeric_negative(PlSqlParser.Numeric_negativeContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#numeric_negative}.
	 * @param ctx the parse tree
	 */
	void exitNumeric_negative(PlSqlParser.Numeric_negativeContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#quoted_string}.
	 * @param ctx the parse tree
	 */
	void enterQuoted_string(PlSqlParser.Quoted_stringContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#quoted_string}.
	 * @param ctx the parse tree
	 */
	void exitQuoted_string(PlSqlParser.Quoted_stringContext ctx);
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
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#id_expression}.
	 * @param ctx the parse tree
	 */
	void enterId_expression(PlSqlParser.Id_expressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#id_expression}.
	 * @param ctx the parse tree
	 */
	void exitId_expression(PlSqlParser.Id_expressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#outer_join_sign}.
	 * @param ctx the parse tree
	 */
	void enterOuter_join_sign(PlSqlParser.Outer_join_signContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#outer_join_sign}.
	 * @param ctx the parse tree
	 */
	void exitOuter_join_sign(PlSqlParser.Outer_join_signContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#regular_id}.
	 * @param ctx the parse tree
	 */
	void enterRegular_id(PlSqlParser.Regular_idContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#regular_id}.
	 * @param ctx the parse tree
	 */
	void exitRegular_id(PlSqlParser.Regular_idContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#non_reserved_keywords_in_12c}.
	 * @param ctx the parse tree
	 */
	void enterNon_reserved_keywords_in_12c(PlSqlParser.Non_reserved_keywords_in_12cContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#non_reserved_keywords_in_12c}.
	 * @param ctx the parse tree
	 */
	void exitNon_reserved_keywords_in_12c(PlSqlParser.Non_reserved_keywords_in_12cContext ctx);
	/**
	 * Enter a parse tree produced by {@link PlSqlParser#non_reserved_keywords_pre12c}.
	 * @param ctx the parse tree
	 */
	void enterNon_reserved_keywords_pre12c(PlSqlParser.Non_reserved_keywords_pre12cContext ctx);
	/**
	 * Exit a parse tree produced by {@link PlSqlParser#non_reserved_keywords_pre12c}.
	 * @param ctx the parse tree
	 */
	void exitNon_reserved_keywords_pre12c(PlSqlParser.Non_reserved_keywords_pre12cContext ctx);
}