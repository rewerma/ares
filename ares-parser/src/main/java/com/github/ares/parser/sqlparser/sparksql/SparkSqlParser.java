package com.github.ares.parser.sqlparser.sparksql;

import com.github.ares.api.common.CriteriaClause;
import com.github.ares.api.common.EngineType;
import com.github.ares.api.common.ExecutionEngineType;
import com.github.ares.common.exceptions.ParseException;
import com.github.ares.org.antlr.v4.runtime.CharStream;
import com.github.ares.org.antlr.v4.runtime.CharStreams;
import com.github.ares.org.antlr.v4.runtime.CommonTokenStream;
import com.github.ares.org.antlr.v4.runtime.tree.ParseTree;
import com.github.ares.org.antlr.v4.runtime.tree.TerminalNodeImpl;
import com.github.ares.parser.antlr4.CaseChangingCharStream;
import com.github.ares.parser.antlr4.CustomErrorListener;
import com.github.ares.parser.antlr4.sparksql.SqlBaseLexer;
import com.github.ares.parser.antlr4.sparksql.SqlBaseParser;
import com.github.ares.parser.sqlparser.SQLParser;
import com.github.ares.parser.sqlparser.model.SQLDelete;
import com.github.ares.parser.sqlparser.model.SQLHint;
import com.github.ares.parser.sqlparser.model.SQLInsert;
import com.github.ares.parser.sqlparser.model.SQLMerge;
import com.github.ares.parser.sqlparser.model.SQLSelect;
import com.github.ares.parser.sqlparser.model.SQLTruncate;
import com.github.ares.parser.sqlparser.model.SQLUpdate;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

import static com.github.ares.parser.sqlparser.sparksql.CriteriaParser.parseWhereClause;
import static com.github.ares.parser.sqlparser.sparksql.CriteriaParser.visitOnWhereClause;
import static com.github.ares.parser.utils.PLParserUtil.clearParam;
import static com.github.ares.parser.utils.PLParserUtil.getFullText;

public class SparkSqlParser implements SQLParser {
    @Override
    public SQLSelect parseSelect(String sql) {
        SQLSelect sqlSelect = new SQLSelect();
        try (InputStream in = new ByteArrayInputStream(sql.getBytes(StandardCharsets.UTF_8))) {
            SqlBaseParser parser = parseSql(in);
            SqlBaseParser.QueryContext queryContext = parser.query();

            if (!(queryContext.queryTerm() instanceof SqlBaseParser.QueryTermDefaultContext)) {
                throw new ParseException(String.format("unsupported syntax: %s", sql));
            }
            SqlBaseParser.QueryTermDefaultContext queryTermDefaultContext = (SqlBaseParser.QueryTermDefaultContext) queryContext.queryTerm();
            if (!(queryTermDefaultContext.queryPrimary() instanceof SqlBaseParser.QueryPrimaryDefaultContext)) {
                throw new ParseException(String.format("unsupported syntax: %s", sql));
            }
            SqlBaseParser.QueryPrimaryDefaultContext queryPrimaryDefaultContext = (SqlBaseParser.QueryPrimaryDefaultContext) queryTermDefaultContext.queryPrimary();
            if (!(queryPrimaryDefaultContext.querySpecification() instanceof SqlBaseParser.RegularQuerySpecificationContext)) {
                throw new ParseException(String.format("unsupported syntax: %s", sql));
            }
            SqlBaseParser.RegularQuerySpecificationContext regularQuerySpecificationContext = (SqlBaseParser.RegularQuerySpecificationContext) queryPrimaryDefaultContext.querySpecification();
            SqlBaseParser.IntoClauseContext intoClauseContext = regularQuerySpecificationContext.selectClause().intoClause();
            if (intoClauseContext == null) {
                Pair<List<SQLHint>, String> hintsWithSql = parseSelectHints(sql, queryPrimaryDefaultContext);
                sqlSelect.setHints(hintsWithSql.getLeft());
                sqlSelect.setSourceSql(hintsWithSql.getRight());
            } else {
                List<String> intoParams = new ArrayList<>();
                intoClauseContext.expression().forEach(expressionContext -> intoParams.add(expressionContext.getText()));

                sqlSelect.setIntoParams(new ArrayList<>());
                for (String intoParam : intoParams) {
                    intoParam = clearParam(intoParam);
                    sqlSelect.getIntoParams().add(intoParam);
                }

                Pair<List<SQLHint>, String> hintsWithSql = parseSelectHints(sql, queryPrimaryDefaultContext);
                sqlSelect.setHints(hintsWithSql.getLeft());
                sqlSelect.setSourceSql(hintsWithSql.getRight());
            }
        } catch (ParseException e) {
            throw e;
        } catch (Exception e) {
            throw new ParseException(e.getMessage(), e);
        }
        return sqlSelect;
    }

    @Override
    public SQLInsert parseInsert(String sql) {
        SQLInsert sqlInsert = new SQLInsert();

        try (InputStream in = new ByteArrayInputStream(sql.getBytes(StandardCharsets.UTF_8))) {

            SqlBaseParser parser = parseSql(in);
            SqlBaseParser.DmlStatementNoWithContext dmlStatementNoWithContext = parser.dmlStatementNoWith();

            if (!(dmlStatementNoWithContext instanceof SqlBaseParser.SingleInsertQueryContext)) {
                throw new ParseException(String.format("unsupported syntax: %s", sql));
            }
            SqlBaseParser.SingleInsertQueryContext singleInsertQueryContext = (SqlBaseParser.SingleInsertQueryContext) dmlStatementNoWithContext;
            SqlBaseParser.InsertIntoTableContext insertIntoContext = (SqlBaseParser.InsertIntoTableContext) singleInsertQueryContext.insertInto();
            if (insertIntoContext.getChildCount() < 3 ||
                    !(insertIntoContext.getChild(1) instanceof TerminalNodeImpl) ||
                    !"INTO".equalsIgnoreCase(insertIntoContext.getChild(1).getText()) ||
                    !(insertIntoContext.getChild(2) instanceof SqlBaseParser.MultipartIdentifierContext)) {
                throw new ParseException(String.format("unsupported syntax: %s", sql));
            }
            SqlBaseParser.MultipartIdentifierContext multipartIdentifierContext = insertIntoContext.multipartIdentifier();
            sqlInsert.setTable(multipartIdentifierContext.getText());
            if (insertIntoContext.identifierList() != null) {
                SqlBaseParser.IdentifierListContext identifierListContext = insertIntoContext.identifierList();
                if (identifierListContext.identifierSeq() == null) {
                    throw new ParseException(String.format("unsupported syntax: %s", sql));
                }

                SqlBaseParser.IdentifierSeqContext identifierSeqContext = identifierListContext.identifierSeq();
                for (SqlBaseParser.ErrorCapturingIdentifierContext errorCapturingIdentifierContext : identifierSeqContext.errorCapturingIdentifier()) {
                    sqlInsert.getColumns().add(errorCapturingIdentifierContext.getText());
                }
            }

            String selectSql;
            SqlBaseParser.QueryContext queryContext = singleInsertQueryContext.query();
            SqlBaseParser.QueryPrimaryContext queryPrimaryContext = ((SqlBaseParser.QueryTermDefaultContext) queryContext.queryTerm()).queryPrimary();
            if (queryPrimaryContext instanceof SqlBaseParser.InlineTableDefault1Context) {
                List<SqlBaseParser.ExpressionContext> expressionContexts = ((SqlBaseParser.InlineTableDefault1Context)
                        ((SqlBaseParser.QueryTermDefaultContext) queryContext.queryTerm()).queryPrimary()).inlineTable().expression();
                List<String> valuesExpressions = new ArrayList<>();
                List<List<String>> valuesArray = new ArrayList<>();
                for (SqlBaseParser.ExpressionContext expressionContext : expressionContexts) {
                    if (expressionContext.getChildCount() < 1 || !(expressionContext.getChild(0) instanceof SqlBaseParser.PredicatedContext)) {
                        throw new ParseException(String.format("unsupported syntax: %s", sql));
                    }
                    SqlBaseParser.PredicatedContext predicatedContext = (SqlBaseParser.PredicatedContext) expressionContext.getChild(0);
                    if (predicatedContext.valueExpression().getChildCount() < 1 || !(predicatedContext.valueExpression().getChild(0) instanceof SqlBaseParser.RowConstructorContext)) {
                        throw new ParseException(String.format("unsupported syntax: %s", sql));
                    }
                    List<String> values = new ArrayList<>();
                    SqlBaseParser.RowConstructorContext rowConstructorContext = (SqlBaseParser.RowConstructorContext) predicatedContext.valueExpression().getChild(0);
                    for (int i = 0; i < rowConstructorContext.getChildCount(); i++) {
                        ParseTree item = rowConstructorContext.getChild(i);
                        if (item instanceof TerminalNodeImpl) {
                            continue;
                        }
                        values.add(getFullText(item));
                    }
                    valuesArray.add(values);
                    sqlInsert.setValuesArray(valuesArray);

                    String selectExpression = getFullText(expressionContext);
                    if (!selectExpression.startsWith("(") && !selectExpression.endsWith(")")) {
                        throw new ParseException(String.format("unsupported syntax: %s", sql));
                    }
                    selectExpression = selectExpression.substring(0, selectExpression.length() - 1).substring(1);
                    valuesExpressions.add(selectExpression);
                }
                StringJoiner stringJoiner = new StringJoiner(" UNION ALL ");
                valuesExpressions.forEach(valuesExpression -> {
                    stringJoiner.add("SELECT " + valuesExpression);
                });

                selectSql = stringJoiner.toString();
            } else if (queryPrimaryContext instanceof SqlBaseParser.QueryPrimaryDefaultContext) {
                Pair<List<SQLHint>, String> hintsWithSql = parseSelectHints(sql, (SqlBaseParser.QueryPrimaryDefaultContext) queryPrimaryContext);
                sqlInsert.setHints(hintsWithSql.getLeft());
                selectSql = hintsWithSql.getRight();
            } else {
                throw new ParseException(String.format("unsupported syntax: %s", sql));
            }
            sqlInsert.setSourceSql(selectSql);
        } catch (ParseException e) {
            throw e;
        } catch (Exception e) {
            throw new ParseException(e.getMessage(), e);
        }
        return sqlInsert;
    }

    @Override
    public SQLUpdate parseUpdate(String sql) {
        SQLUpdate sqlUpdate = new SQLUpdate();
        try (InputStream in = new ByteArrayInputStream(sql.getBytes(StandardCharsets.UTF_8))) {
            SqlBaseParser parser = parseSql(in);
            SqlBaseParser.DmlStatementNoWithContext dmlStatementNoWithContext = parser.dmlStatementNoWith();

            if (!(dmlStatementNoWithContext instanceof SqlBaseParser.UpdateTableContext)) {
                throw new ParseException(String.format("unsupported syntax: %s", sql));
            }

            SqlBaseParser.UpdateTableContext updateTableContext = (SqlBaseParser.UpdateTableContext) dmlStatementNoWithContext;

            SqlBaseParser.MultipartIdentifierContext mappingTable = updateTableContext.multipartIdentifier(0);
            sqlUpdate.setTable(mappingTable.getText());

            if (updateTableContext.source != null || updateTableContext.sourceQuery != null) {
                if (updateTableContext.tableAlias().size() != 2) {
                    throw new ParseException(String.format("Alias not defined for source table or target table: %s", sql));
                }
                if (updateTableContext.source != null) {
                    sqlUpdate.setJoinTable(updateTableContext.source.getText());
                    String sourceTableAlias = updateTableContext.tableAlias().get(1).getText();
                    sqlUpdate.setJoinAlias(sourceTableAlias);
                    sqlUpdate.setAlias(updateTableContext.tableAlias().get(0).getText());
                } else if (updateTableContext.sourceQuery != null) {
                    if (!(updateTableContext.sourceQuery.queryTerm() instanceof SqlBaseParser.QueryTermDefaultContext)) {
                        throw new ParseException(String.format("unsupported syntax: %s", sql));
                    }
                    SqlBaseParser.QueryPrimaryContext queryPrimaryContext = ((SqlBaseParser.QueryTermDefaultContext) updateTableContext.sourceQuery.queryTerm()).queryPrimary();
                    if (!(queryPrimaryContext instanceof SqlBaseParser.QueryPrimaryDefaultContext)) {
                        throw new ParseException(String.format("unsupported syntax: %s", sql));
                    }
                    String sourceTableAlias = updateTableContext.tableAlias().get(1).getText();
                    sqlUpdate.setJoinAlias(sourceTableAlias);
                    sqlUpdate.setAlias(updateTableContext.tableAlias().get(0).getText());
                    Pair<List<SQLHint>, String> hintsWithSql = parseSelectHints(sql, (SqlBaseParser.QueryPrimaryDefaultContext) queryPrimaryContext);
                    sqlUpdate.setHints(hintsWithSql.getLeft());
                    sqlUpdate.setJoinSql(hintsWithSql.getRight());
                }
            } else if (!updateTableContext.tableAlias().isEmpty()) {
                sqlUpdate.setAlias(updateTableContext.tableAlias().get(0).getText());
            }

            List<SqlBaseParser.AssignmentContext> assignmentContexts = updateTableContext.setClause().assignmentList().assignment();
            for (SqlBaseParser.AssignmentContext assignmentContext : assignmentContexts) {
                List<SqlBaseParser.ErrorCapturingIdentifierContext> identifierContexts = assignmentContext.multipartIdentifier().errorCapturingIdentifier();
                if (StringUtils.isBlank(sqlUpdate.getAlias())) {
                    if (identifierContexts.size() == 2 && identifierContexts.get(0).getText().equalsIgnoreCase(sqlUpdate.getAlias())) {
                        throw new ParseException("column owner must be same as table alias in update statement: " + sql);
                    }
                }
                String targetCol = assignmentContext.multipartIdentifier().errorCapturingIdentifier.getText();
                String sourceExpression = getFullText(assignmentContext.expression());
                sqlUpdate.getUpdateColumns().add(targetCol);
                sqlUpdate.getUpdateValues().add(sourceExpression);
            }
            if (updateTableContext.whereClause() == null) {
                throw new ParseException("update SQL must have WHERE clause: " + sql);
            }
            CriteriaClause criteriaClause = new CriteriaClause();
            SqlBaseParser.BooleanExpressionContext expressionContext = updateTableContext.whereClause().booleanExpression();
            CriteriaParser.parseWhereClause(expressionContext, criteriaClause, sqlUpdate.getAlias());
            sqlUpdate.setWhereClause(criteriaClause);

            List<String> selectItems = new ArrayList<>();
            visitCriteriaClause(criteriaClause, selectItems);

            StringBuilder selectSql = new StringBuilder();
            selectSql.append("SELECT ");
            selectSql.append(String.join(", ", sqlUpdate.getUpdateValues()));
            selectSql.append(", ").append(String.join(", ", selectItems));
            if (StringUtils.isNotBlank(sqlUpdate.getJoinTable())) {
                selectSql.append(" FROM ").append(sqlUpdate.getJoinTable()).append(" ").append(sqlUpdate.getJoinAlias());
            } else if (StringUtils.isNotBlank(sqlUpdate.getJoinSql())) {
                selectSql.append(" FROM (").append(sqlUpdate.getJoinSql()).append(") ").append(sqlUpdate.getJoinAlias());
            }
            sqlUpdate.setSourceSql(selectSql.toString());
        } catch (ParseException e) {
            throw e;
        } catch (Exception e) {
            throw new ParseException(e.getMessage(), e);
        }
        return sqlUpdate;
    }

    @Override
    public SQLDelete parseDelete(String sql) {
        SQLDelete sqlDelete = new SQLDelete();
        try (InputStream in = new ByteArrayInputStream(sql.getBytes(StandardCharsets.UTF_8))) {
            SqlBaseParser parser = parseSql(in);
            SqlBaseParser.DmlStatementNoWithContext dmlStatementNoWithContext = parser.dmlStatementNoWith();

            if (!(dmlStatementNoWithContext instanceof SqlBaseParser.DeleteFromTableContext)) {
                throw new ParseException(String.format("unsupported syntax: %s", sql));
            }

            SqlBaseParser.DeleteFromTableContext deleteFromTableContext = (SqlBaseParser.DeleteFromTableContext) dmlStatementNoWithContext;

            SqlBaseParser.MultipartIdentifierContext mappingTable = deleteFromTableContext.multipartIdentifier(0);
            sqlDelete.setTable(mappingTable.getText());

            if (deleteFromTableContext.source != null || deleteFromTableContext.sourceQuery != null) {
                if (deleteFromTableContext.tableAlias().size() != 2) {
                    throw new ParseException(String.format("Alias not defined for source table or target table: %s", sql));
                }
                if (deleteFromTableContext.source != null) {
                    sqlDelete.setJoinTable(deleteFromTableContext.source.getText());
                    String sourceTableAlias = deleteFromTableContext.tableAlias().get(1).getText();
                    sqlDelete.setJoinAlias(sourceTableAlias);
                    sqlDelete.setAlias(deleteFromTableContext.tableAlias().get(0).getText());
                } else if (deleteFromTableContext.sourceQuery != null) {
                    if (!(deleteFromTableContext.sourceQuery.queryTerm() instanceof SqlBaseParser.QueryTermDefaultContext)) {
                        throw new ParseException(String.format("unsupported syntax: %s", sql));
                    }
                    SqlBaseParser.QueryPrimaryContext queryPrimaryContext = ((SqlBaseParser.QueryTermDefaultContext) deleteFromTableContext.sourceQuery.queryTerm()).queryPrimary();
                    if (!(queryPrimaryContext instanceof SqlBaseParser.QueryPrimaryDefaultContext)) {
                        throw new ParseException(String.format("unsupported syntax: %s", sql));
                    }
                    String sourceTableAlias = deleteFromTableContext.tableAlias().get(1).getText();
                    sqlDelete.setJoinAlias(sourceTableAlias);
                    sqlDelete.setAlias(deleteFromTableContext.tableAlias().get(0).getText());
                    Pair<List<SQLHint>, String> hintsWithSql = parseSelectHints(sql, (SqlBaseParser.QueryPrimaryDefaultContext) queryPrimaryContext);
                    sqlDelete.setHints(hintsWithSql.getLeft());
                    sqlDelete.setJoinSql(hintsWithSql.getRight());
                }
            } else if (!deleteFromTableContext.tableAlias().isEmpty()) {
                sqlDelete.setAlias(deleteFromTableContext.tableAlias().get(0).getText());
            }

            if (deleteFromTableContext.whereClause() == null) {
                throw new ParseException("delete SQL must have WHERE clause: " + sql);
            }
            CriteriaClause criteriaClause = new CriteriaClause();
            SqlBaseParser.BooleanExpressionContext expressionContext = deleteFromTableContext.whereClause().booleanExpression();
            CriteriaParser.parseWhereClause(expressionContext, criteriaClause, sqlDelete.getAlias());
            sqlDelete.setWhereClause(criteriaClause);

            List<String> selectItems = new ArrayList<>();
            visitCriteriaClause(criteriaClause, selectItems);

            StringBuilder selectSql = new StringBuilder();
            selectSql.append("SELECT ");
            selectSql.append(String.join(", ", selectItems));
            if (StringUtils.isNotBlank(sqlDelete.getJoinTable())) {
                selectSql.append(" FROM ").append(sqlDelete.getJoinTable()).append(" ").append(sqlDelete.getJoinAlias());
            } else if (StringUtils.isNotBlank(sqlDelete.getJoinSql())) {
                selectSql.append(" FROM (").append(sqlDelete.getJoinSql()).append(") ").append(sqlDelete.getJoinAlias());
            }
            sqlDelete.setSourceSql(selectSql.toString());
        } catch (ParseException e) {
            throw e;
        } catch (Exception e) {
            throw new ParseException(e.getMessage(), e);
        }
        return sqlDelete;
    }

    @Override
    public SQLMerge parseMerge(String sql) {
        SQLMerge sqlMerge = new SQLMerge();
        try (InputStream in = new ByteArrayInputStream(sql.getBytes(StandardCharsets.UTF_8))) {
            SqlBaseParser parser = parseSql(in);
            SqlBaseParser.DmlStatementNoWithContext dmlStatementNoWithContext = parser.dmlStatementNoWith();

            if (!(dmlStatementNoWithContext instanceof SqlBaseParser.MergeIntoTableContext)) {
                throw new ParseException(String.format("unsupported syntax: %s", sql));
            }
            getFullText(dmlStatementNoWithContext);

            SqlBaseParser.MergeIntoTableContext mergeIntoTableContext = (SqlBaseParser.MergeIntoTableContext) dmlStatementNoWithContext;
            SqlBaseParser.MultipartIdentifierContext mappingTable = mergeIntoTableContext.multipartIdentifier(0);
            sqlMerge.setTable(mappingTable.getText());

            if (mergeIntoTableContext.source != null || mergeIntoTableContext.sourceQuery != null) {
                if (mergeIntoTableContext.tableAlias().size() != 2) {
                    throw new ParseException(String.format("Alias not defined for source table or target table: %s", sql));
                }
                if (mergeIntoTableContext.source != null) {
                    sqlMerge.setUsingTable(mergeIntoTableContext.source.getText());
                    String sourceTableAlias = mergeIntoTableContext.tableAlias().get(1).getText();
                    sqlMerge.setUsingAlias(sourceTableAlias);
                    sqlMerge.setAlias(mergeIntoTableContext.tableAlias().get(0).getText());
                } else if (mergeIntoTableContext.sourceQuery != null) {
                    if (!(mergeIntoTableContext.sourceQuery.queryTerm() instanceof SqlBaseParser.QueryTermDefaultContext)) {
                        throw new ParseException(String.format("unsupported syntax: %s", sql));
                    }
                    SqlBaseParser.QueryPrimaryContext queryPrimaryContext = ((SqlBaseParser.QueryTermDefaultContext) mergeIntoTableContext.sourceQuery.queryTerm()).queryPrimary();
                    if (!(queryPrimaryContext instanceof SqlBaseParser.QueryPrimaryDefaultContext)) {
                        throw new ParseException(String.format("unsupported syntax: %s", sql));
                    }
                    String sourceTableAlias = mergeIntoTableContext.tableAlias().get(1).getText();
                    sqlMerge.setUsingAlias(sourceTableAlias);
                    sqlMerge.setAlias(mergeIntoTableContext.tableAlias().get(0).getText());
                    Pair<List<SQLHint>, String> hintsWithSql = parseSelectHints(sql, (SqlBaseParser.QueryPrimaryDefaultContext) queryPrimaryContext);
                    sqlMerge.setHints(hintsWithSql.getLeft());
                    sqlMerge.setUsingSql(hintsWithSql.getRight());
                }
            } else if (!mergeIntoTableContext.tableAlias().isEmpty()) {
                sqlMerge.setAlias(mergeIntoTableContext.tableAlias().get(0).getText());
            }

            CriteriaClause onClause = new CriteriaClause();
            SqlBaseParser.BooleanExpressionContext onExpressionContext = mergeIntoTableContext.mergeCondition;
            CriteriaParser.parseWhereClause(onExpressionContext, onClause, sqlMerge.getAlias());
            List<String> onSelectItems = new ArrayList<>();
            visitCriteriaClause(onClause, onSelectItems);
            sqlMerge.setOnSelectItems(onSelectItems);

            String usingSQL;
            if (sqlMerge.getUsingTable() != null) {
                usingSQL = "SELECT * FROM " + sqlMerge.getUsingTable();
            } else {
                usingSQL = sqlMerge.getUsingSql();
            }

            StringBuilder conditionSql = new StringBuilder();
            visitOnWhereClause(onClause, conditionSql);

            if (mergeIntoTableContext.matchedClause().size() > 1 || mergeIntoTableContext.notMatchedClause().size() > 1) {
                throw new ParseException(String.format("unsupported syntax: %s", sql));
            }
            if (!mergeIntoTableContext.notMatchedClause().isEmpty()) {
                SQLInsert sqlInsert = new SQLInsert();
                sqlInsert.setTable(mappingTable.getText());

                SqlBaseParser.NotMatchedActionContext notMatchedAction = mergeIntoTableContext.notMatchedClause().get(0).notMatchedAction();

                for (SqlBaseParser.MultipartIdentifierContext multipartIdentifierContext : notMatchedAction.multipartIdentifierList().multipartIdentifier()) {
                    sqlInsert.getColumns().add(multipartIdentifierContext.errorCapturingIdentifier.identifier().getText());
                }

                List<List<String>> valuesArray = new ArrayList<>();
                List<String> values = new ArrayList<>();
                for (SqlBaseParser.ExpressionContext expressionContext : notMatchedAction.expression()) {
                    values.add(getFullText(expressionContext));
                }
                valuesArray.add(values);
                sqlInsert.setValuesArray(valuesArray);
                sqlMerge.setSqlInsert(sqlInsert);

                StringBuilder sourceSql = new StringBuilder();
                sourceSql.append("SELECT ");
                sourceSql.append(String.join(", ", sqlInsert.getValuesArray().get(0)));
                sourceSql.append(" FROM (");
                sourceSql.append(usingSQL).append(") ").append(sqlMerge.getUsingAlias()).append(" ");
                sourceSql.append("WHERE NOT EXISTS (SELECT 1 FROM ");
                sourceSql.append(sqlMerge.getTable());
                sourceSql.append(" WHERE ").append(conditionSql).append(" )");
                sqlInsert.setSourceSql(sourceSql.toString());
            }

            if (!mergeIntoTableContext.matchedClause().isEmpty()) {
                SQLUpdate sqlUpdate = new SQLUpdate();
                sqlUpdate.setTable(mappingTable.getText());

                sqlUpdate.setAlias(sqlMerge.getAlias());
                sqlUpdate.setJoinAlias(sqlMerge.getUsingAlias());
                if (sqlMerge.getUsingTable() != null) {
                    sqlUpdate.setJoinTable(sqlMerge.getUsingTable());
                } else if (sqlMerge.getUsingSql() != null) {
                    sqlUpdate.setJoinSql(sqlMerge.getUsingSql());
                }

                SqlBaseParser.MatchedActionContext matchedActionContext = mergeIntoTableContext.matchedClause().get(0).matchedAction();

                for (SqlBaseParser.AssignmentContext assignmentContext : matchedActionContext.assignmentList().assignment()) {
                    sqlUpdate.getUpdateColumns().add(assignmentContext.key.errorCapturingIdentifier.identifier().getText());
                    sqlUpdate.getUpdateValues().add(getFullText(assignmentContext.value));
                }

                if (matchedActionContext.booleanExpression() != null) {
                    CriteriaClause whereClause = new CriteriaClause();
                    parseWhereClause(matchedActionContext.booleanExpression(), whereClause, sqlUpdate.getAlias());

                    sqlUpdate.setWhereClause(whereClause);
                    List<String> selectItems = new ArrayList<>();
                    visitCriteriaClause(whereClause, selectItems);
                    sqlUpdate.setSelectWhereItems(selectItems);

                    CriteriaClause allWhereClause = new CriteriaClause();
                    allWhereClause.setLeftCriteria(onClause);
                    allWhereClause.setOperator("AND");
                    allWhereClause.setRightCriteria(whereClause);
                    sqlMerge.setAllWhereClause(allWhereClause);
                }
                sqlMerge.setSqlUpdate(sqlUpdate);

                StringBuilder sourceSql = new StringBuilder();
                sourceSql.append("SELECT ");
                sourceSql.append(String.join(", ", sqlUpdate.getUpdateValues()));
                sourceSql.append(", ").append(String.join(", ", sqlMerge.getOnSelectItems()));
                if (sqlUpdate.getSelectWhereItems() != null) {
                    sourceSql.append(", ").append(String.join(", ", sqlUpdate.getSelectWhereItems()));
                }
                sourceSql.append(" FROM (");
                sourceSql.append(usingSQL).append(") ").append(sqlMerge.getUsingAlias()).append(" ");
                sourceSql.append("WHERE EXISTS (SELECT 1 FROM ");
                sourceSql.append(sqlMerge.getTable());
                sourceSql.append(" WHERE ").append(conditionSql).append(" )");

                sqlUpdate.setSourceSql(sourceSql.toString());
            }
        } catch (ParseException e) {
            throw e;
        } catch (Exception e) {
            throw new ParseException(e.getMessage(), e);
        }
        return sqlMerge;
    }

    @Override
    public SQLTruncate parseTruncate(String sql) {
        SQLTruncate sqlTruncate = new SQLTruncate();
        try (InputStream in = new ByteArrayInputStream(sql.getBytes(StandardCharsets.UTF_8))) {
            SqlBaseParser parser = parseSql(in);
            SqlBaseParser.StatementContext statementContext = parser.statement();
            if (!(statementContext instanceof SqlBaseParser.TruncateTableContext)) {
                throw new ParseException(String.format("unsupported syntax: %s", sql));
            }
            SqlBaseParser.TruncateTableContext truncateTableContext = (SqlBaseParser.TruncateTableContext) statementContext;

            sqlTruncate.setTableName(truncateTableContext.multipartIdentifier().getText());
        } catch (ParseException e) {
            throw e;
        } catch (Exception e) {
            throw new ParseException(e.getMessage(), e);
        }
        return sqlTruncate;
    }

    private SqlBaseParser parseSql(InputStream in) throws IOException {
        CharStream s = CharStreams.fromStream(in);
        CaseChangingCharStream upper = new CaseChangingCharStream(s, true);

        CustomErrorListener lexerErrorListener = new CustomErrorListener();
        SqlBaseLexer lexer = new SqlBaseLexer(upper);
        lexer.removeErrorListeners();
        lexer.addErrorListener(lexerErrorListener);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        SqlBaseParser parser = new SqlBaseParser(tokens);
        CustomErrorListener parserErrorListener = new CustomErrorListener();
        parser.removeErrorListeners();
        parser.addErrorListener(parserErrorListener);
        return parser;
    }

    private Pair<List<SQLHint>, String> parseSelectHints(String sql, SqlBaseParser.QueryPrimaryDefaultContext queryPrimaryDefaultContext) {
        if (!(queryPrimaryDefaultContext.querySpecification() instanceof SqlBaseParser.RegularQuerySpecificationContext)) {
            throw new ParseException(String.format("unsupported syntax: %s", sql));
        }
        SqlBaseParser.RegularQuerySpecificationContext regularQuerySpecificationContext = (SqlBaseParser.RegularQuerySpecificationContext) queryPrimaryDefaultContext.querySpecification();
        if (regularQuerySpecificationContext.selectClause() == null) {
            throw new ParseException(String.format("unsupported syntax: %s", sql));
        }

        List<SQLHint> sqlHints = new ArrayList<>();
        SqlBaseParser.SelectClauseContext selectClauseContext = regularQuerySpecificationContext.selectClause();
        if (!selectClauseContext.hints.isEmpty()) {
            for (int i = 0; i < selectClauseContext.hints.size(); i++) {
                SQLHint sqlHint = new SQLHint();
                SqlBaseParser.HintContext hintContext = selectClauseContext.hint(i);
                String hintName = hintContext.hintStatement.hintName.getText();
                sqlHint.setHintName(hintName);
                for (SqlBaseParser.PrimaryExpressionContext primaryExpressionContext : hintContext.hintStatement.parameters) {
                    String parameter = getFullText(primaryExpressionContext);
                    if (StringUtils.isNotBlank(parameter)) {
                        sqlHint.getArguments().add(parameter);
                    }
                }
                sqlHints.add(sqlHint);
            }
        }

        // filter out hints and into clause from select clause
        StringBuilder selectSql = new StringBuilder();
        for (ParseTree child : regularQuerySpecificationContext.children) {
            if (child instanceof SqlBaseParser.SelectClauseContext) {
                SqlBaseParser.SelectClauseContext selectClauseContext1 = (SqlBaseParser.SelectClauseContext) child;
                for (ParseTree grandChild : selectClauseContext1.children) {
                    if (grandChild instanceof SqlBaseParser.IntoClauseContext) {
                        continue;
                    } else if (grandChild instanceof SqlBaseParser.HintContext) {
                        if (ExecutionEngineType.engineType == EngineType.SPARK) {
                            String hint = getFullText(grandChild);
                            String hintLower = hint.toLowerCase();
                            if (hintLower.contains("mapjoin") || hintLower.contains("broadcast")) {
                                selectSql.append(hint).append(" ");
                            }
                        }
                        continue;
                    }
                    selectSql.append(getFullText(grandChild)).append(" ");
                }
            } else {
                selectSql.append(getFullText(child)).append(" ");
            }
        }

        return Pair.of(sqlHints, selectSql.toString());
    }
}
