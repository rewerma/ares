package com.github.ares.parser.sqlparser.jsqlparser;

import com.github.ares.api.common.CriteriaClause;
import com.github.ares.common.exceptions.ParseException;
import com.github.ares.parser.sqlparser.SQLParser;
import com.github.ares.parser.sqlparser.model.SQLDelete;
import com.github.ares.parser.sqlparser.model.SQLHint;
import com.github.ares.parser.sqlparser.model.SQLInsert;
import com.github.ares.parser.sqlparser.model.SQLMerge;
import com.github.ares.parser.sqlparser.model.SQLSelect;
import com.github.ares.parser.sqlparser.model.SQLTruncate;
import com.github.ares.parser.sqlparser.model.SQLUpdate;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.RowConstructor;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.merge.Merge;
import net.sf.jsqlparser.statement.merge.MergeInsert;
import net.sf.jsqlparser.statement.merge.MergeUpdate;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.statement.update.UpdateSet;
import net.sf.jsqlparser.statement.values.ValuesStatement;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.github.ares.parser.sqlparser.SQLHintParser.parseHints;
import static com.github.ares.parser.sqlparser.jsqlparser.CriteriaParser.parseOnExpression;
import static com.github.ares.parser.sqlparser.jsqlparser.CriteriaParser.parseWhere;
import static com.github.ares.parser.sqlparser.jsqlparser.CriteriaParser.visitOnWhereClause;
import static com.github.ares.parser.utils.PLParserUtil.clearParam;

public class JSQLParser implements SQLParser {
    @Override
    public SQLSelect parseSelect(String sql) {
        try {
            SQLSelect sqlSelect = new SQLSelect();
            Statement statement = CCJSqlParserUtil.parse(sql);
            Select selectStatement = (Select) statement;
            PlainSelect plainSelect = (PlainSelect) selectStatement.getSelectBody();
            if (plainSelect.getIntoTables() != null) {
                sqlSelect.setIntoParams(new ArrayList<>());
                for (Table table : plainSelect.getIntoTables()) {
                    String intoParam = clearParam(table.getName());
                    sqlSelect.getIntoParams().add(intoParam);
                }
                plainSelect.setIntoTables(null);
            }
            List<SQLHint> sqlHints = parseHints(plainSelect);
            sqlSelect.setHints(sqlHints);
            sqlSelect.setSourceSql(plainSelect.toString());

            return sqlSelect;
        } catch (ParseException e) {
            throw e;
        } catch (Exception e) {
            throw new ParseException("unsupported syntax: " + sql, e);
        }
    }

    @Override
    public SQLInsert parseInsert(String sql) {
        try {
            SQLInsert sqlInsert = new SQLInsert();
            Statement statement = CCJSqlParserUtil.parse(sql);
            Insert insertStatement = (Insert) statement;
            Table table = insertStatement.getTable();
            sqlInsert.setTable(table.getName());

            if (insertStatement.getColumns() != null) {
                for (Column column : insertStatement.getColumns()) {
                    sqlInsert.getColumns().add(column.getColumnName());
                }
            }

            Select select = insertStatement.getSelect();
            SelectBody selectBody = select.getSelectBody();
            if (selectBody instanceof SetOperationList) {
                List<List<String>> valuesArray = new ArrayList<>();
                SetOperationList setOperationList = (SetOperationList) selectBody;
                ValuesStatement valuesStatement = (ValuesStatement) setOperationList.getSelects().get(0);
                ExpressionList expressionList = (ExpressionList) valuesStatement.getExpressions();
                for (Expression expression : expressionList.getExpressions()) {
                    if (expression instanceof RowConstructor) {
                        RowConstructor rowConstructor = (RowConstructor) expression;
                        List<Expression> expressions = rowConstructor.getExprList().getExpressions();
                        List<String> values = new ArrayList<>();
                        for (Expression expression1 : expressions) {
                            values.add(expression1.toString());
                        }
                        valuesArray.add(values);
                    } else if (expression instanceof Parenthesis) {
                        Parenthesis parenthesis = (Parenthesis) expression;
                        valuesArray.add(Collections.singletonList(parenthesis.getExpression().toString()));
                    } else {
                        throw new ParseException("unsupported insert value expression: " + expression.toString());
                    }
                }
                sqlInsert.setValuesArray(valuesArray);
                StringBuilder sourceSql = new StringBuilder();
                List<String> valuesGroup = new ArrayList<>();
                for (List<String> values : valuesArray) {
                    valuesGroup.add(" SELECT " + String.join(", ", values));
                }
                sourceSql.append(String.join(" UNION ALL ", valuesGroup));
                sqlInsert.setSourceSql(sourceSql.toString());
            } else if (selectBody instanceof PlainSelect) {
                PlainSelect plainSelect = (PlainSelect) selectBody;
                List<SQLHint> sqlHints = parseHints(plainSelect);
                sqlInsert.setHints(sqlHints);
                sqlInsert.setSourceSql(plainSelect.toString());
            }
            return sqlInsert;
        } catch (ParseException e) {
            throw e;
        } catch (Exception e) {
            throw new ParseException("unsupported syntax: " + sql, e);
        }
    }

    @Override
    public SQLUpdate parseUpdate(String sql) {
        try {
            SQLUpdate sqlUpdate = new SQLUpdate();
            Statement statement = CCJSqlParserUtil.parse(sql);
            Update updateStatement = (Update) statement;
            Table table = updateStatement.getTable();
            sqlUpdate.setTable(table.getName());
            if (table.getAlias() != null) {
                sqlUpdate.setAlias(table.getAlias().getName());
            }

            if (updateStatement.getStartJoins() != null && !updateStatement.getStartJoins().isEmpty()) {
                if (updateStatement.getStartJoins().size() > 1) {
                    throw new ParseException("unsupported join multiple tables in update statement: " + sql);
                }
                Join join = updateStatement.getStartJoins().get(0);
                FromItem fromItem = join.getRightItem();
                if (fromItem instanceof Table) {
                    Table joinTable = (Table) join.getRightItem();
                    sqlUpdate.setJoinTable(joinTable.getName());
                    if (joinTable.getAlias() == null) {
                        throw new ParseException("join table must have alias in update statement: " + sql);
                    }
                    sqlUpdate.setJoinAlias(joinTable.getAlias().getName());
                } else if (fromItem instanceof SubSelect) {
                    SubSelect subSelect = (SubSelect) fromItem;
                    PlainSelect plainSelect = (PlainSelect) subSelect.getSelectBody();
                    List<SQLHint> hints = parseHints(plainSelect);
                    sqlUpdate.setHints(hints);
                    sqlUpdate.setJoinSql(plainSelect.toString());
                    if (subSelect.getAlias() == null) {
                        throw new ParseException("join SQL must have alias in update statement: " + sql);
                    }
                    sqlUpdate.setJoinAlias(subSelect.getAlias().getName());
                }
            }
            if (updateStatement.getUpdateSets().isEmpty()) {
                throw new ParseException("update SQL must have set clause: " + sql);
            }

            List<UpdateSet> updateSets = updateStatement.getUpdateSets();
            for (UpdateSet updateSet : updateSets) {
                Column column = updateSet.getColumns().get(0);
                if (column.getTable() != null && !column.getTable().getName().equals(sqlUpdate.getAlias())) {
                    throw new ParseException("column owner must be same as table alias in update statement: " + sql);
                }
                Expression expression = updateSet.getExpressions().get(0);
                sqlUpdate.getUpdateColumns().add(column.getColumnName());
                sqlUpdate.getUpdateValues().add(expression.toString());
            }

            CriteriaClause criteriaClause = new CriteriaClause();
            if (updateStatement.getWhere() == null) {
                throw new ParseException("update SQL must have where clause: " + sql);
            }
            parseWhere(sqlUpdate.getAlias(), updateStatement.getWhere(), criteriaClause);
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
            return sqlUpdate;
        } catch (ParseException e) {
            throw e;
        } catch (Exception e) {
            throw new ParseException("unsupported syntax: " + sql, e);
        }
    }

    @Override
    public SQLDelete parseDelete(String sql) {
        try {
            SQLDelete sqlDelete = new SQLDelete();
            Statement statement = CCJSqlParserUtil.parse(sql);
            Delete deleteStatement = (Delete) statement;
            Table table = deleteStatement.getTable();
            sqlDelete.setTable(table.getName());
            if (table.getAlias() != null) {
                sqlDelete.setAlias(table.getAlias().getName());
            }

            if (deleteStatement.getJoins() != null) {
                if (deleteStatement.getJoins().size() > 1) {
                    throw new ParseException("unsupported join multiple tables in delete statement: " + sql);
                }
                Join join = deleteStatement.getJoins().get(0);

                FromItem fromItem = join.getRightItem();
                if (fromItem instanceof Table) {
                    Table joinTable = (Table) join.getRightItem();
                    sqlDelete.setJoinTable(joinTable.getName());
                    if (joinTable.getAlias() == null) {
                        throw new ParseException("join table must have alias in delete statement: " + sql);
                    }
                    sqlDelete.setJoinAlias(joinTable.getAlias().getName());
                } else if (fromItem instanceof SubSelect) {
                    SubSelect subSelect = (SubSelect) fromItem;
                    PlainSelect plainSelect = (PlainSelect) subSelect.getSelectBody();
                    List<SQLHint> hints = parseHints(plainSelect);
                    sqlDelete.setHints(hints);
                    sqlDelete.setJoinSql(plainSelect.toString());
                    if (subSelect.getAlias() == null) {
                        throw new ParseException("join SQL must have alias in delete statement: " + sql);
                    }
                    sqlDelete.setJoinAlias(subSelect.getAlias().getName());
                }
            }
            CriteriaClause criteriaClause = new CriteriaClause();
            if (deleteStatement.getWhere() == null) {
                throw new ParseException("delete SQL must have where clause: " + sql);
            }
            parseWhere(sqlDelete.getAlias(), deleteStatement.getWhere(), criteriaClause);
            sqlDelete.setWhereClause(criteriaClause);

            // parse the left values of criteria clause into select items
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

            return sqlDelete;
        } catch (ParseException e) {
            throw e;
        } catch (Exception e) {
            throw new ParseException("unsupported syntax: " + sql, e);
        }
    }

    @Override
    public SQLMerge parseMerge(String sql) {
        try {
            SQLMerge sqlMerge = new SQLMerge();
            Statement statement = CCJSqlParserUtil.parse(sql);
            Merge mergeStatement = (Merge) statement;
            Table table = mergeStatement.getTable();
            sqlMerge.setTable(table.getName());
            if (table.getAlias() != null) {
                sqlMerge.setAlias(table.getAlias().getName());
            }

            Table usingTable = mergeStatement.getUsingTable();
            if (usingTable != null) {
                sqlMerge.setUsingTable(usingTable.getName());
            } else {
                SubSelect usingSelect = mergeStatement.getUsingSelect();
                if (usingSelect == null) {
                    throw new ParseException("using table or using select must be set in merge statement: " + sql);
                }
                PlainSelect plainSelect = (PlainSelect) usingSelect.getSelectBody();
                List<SQLHint> hints = parseHints(plainSelect);
                sqlMerge.setHints(hints);
                sqlMerge.setUsingSql(usingSelect.toString());
            }
            if (mergeStatement.getUsingAlias() == null) {
                throw new ParseException("using table or using select must have alias in merge statement: " + sql);
            }
            sqlMerge.setUsingAlias(mergeStatement.getUsingAlias().getName());

            CriteriaClause onClause = new CriteriaClause();
            parseOnExpression(sqlMerge.getAlias(), sqlMerge.getUsingAlias(), mergeStatement.getOnCondition(), onClause);
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

            MergeInsert mergeInsert = mergeStatement.getMergeInsert();
            if (mergeInsert != null) {
                SQLInsert sqlInsert = new SQLInsert();
                sqlInsert.setTable(table.getName());
                if (mergeInsert.getColumns() != null) {
                    for (Column column : mergeInsert.getColumns()) {
                        sqlInsert.getColumns().add(column.getColumnName());
                    }
                }
                List<List<String>> valuesArray = new ArrayList<>();
                List<String> values = new ArrayList<>();
                for (Expression valueExpr : mergeInsert.getValues()) {
                    values.add(valueExpr.toString());
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

            MergeUpdate mergeUpdate = mergeStatement.getMergeUpdate();
            if (mergeUpdate != null) {
                SQLUpdate sqlUpdate = new SQLUpdate();
                sqlUpdate.setTable(table.getName());
                if (table.getAlias() != null) {
                    sqlUpdate.setAlias(table.getAlias().getName());
                }
                if (sqlMerge.getUsingTable() != null) {
                    sqlUpdate.setJoinTable(sqlMerge.getUsingTable());
                } else {
                    sqlUpdate.setJoinSql(sqlMerge.getUsingSql());
                }
                sqlUpdate.setJoinAlias(sqlMerge.getUsingAlias());
                for (Column updateColumn : mergeUpdate.getColumns()) {
                    if (updateColumn.getTable() != null && !updateColumn.getTable().getName().equals(sqlMerge.getAlias())) {
                        throw new ParseException("column owner must be same as table alias in merge statement: " + sql);
                    }
                    sqlUpdate.getUpdateColumns().add(updateColumn.getColumnName());
                }
                for (Expression valueExpr : mergeUpdate.getValues()) {
                    sqlUpdate.getUpdateValues().add(valueExpr.toString());
                }
                if (mergeUpdate.getWhereCondition() != null) {
                    CriteriaClause whereClause = new CriteriaClause();
                    parseWhere(sqlMerge.getAlias(), mergeUpdate.getWhereCondition(), whereClause);
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

            return sqlMerge;
        } catch (ParseException e) {
            throw e;
        } catch (Exception e) {
            throw new ParseException("unsupported syntax: " + sql, e);
        }
    }

    @Override
    public SQLTruncate parseTruncate(String sql) {
        try {
            SQLTruncate sqlTruncate = new SQLTruncate();
            Statement statement = CCJSqlParserUtil.parse(sql);
            Truncate truncateStatement = (Truncate) statement;
            Table table = truncateStatement.getTable();
            sqlTruncate.setTableName(table.getName());
            return sqlTruncate;
        } catch (ParseException e) {
            throw e;
        } catch (Exception e) {
            throw new ParseException("unsupported syntax: " + sql, e);
        }
    }
}
