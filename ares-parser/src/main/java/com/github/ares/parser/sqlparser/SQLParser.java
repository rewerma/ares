package com.github.ares.parser.sqlparser;

import com.github.ares.api.common.CriteriaClause;
import com.github.ares.parser.sqlparser.model.SQLDelete;
import com.github.ares.parser.sqlparser.model.SQLInsert;
import com.github.ares.parser.sqlparser.model.SQLMerge;
import com.github.ares.parser.sqlparser.model.SQLSelect;
import com.github.ares.parser.sqlparser.model.SQLTruncate;
import com.github.ares.parser.sqlparser.model.SQLUpdate;

import java.util.List;

public interface SQLParser {
    SQLSelect parseSelect(String sql);

    SQLInsert parseInsert(String sql);

    SQLUpdate parseUpdate(String sql);

    SQLDelete parseDelete(String sql);

    SQLMerge parseMerge(String sql);

    SQLTruncate parseTruncate(String sql);

    default void visitCriteriaClause(CriteriaClause criteriaClause, List<String> items) {
        if ("AND".equalsIgnoreCase(criteriaClause.getOperator()) || "OR".equalsIgnoreCase(criteriaClause.getOperator())) {
            visitCriteriaClause(criteriaClause.getLeftCriteria(), items);
            visitCriteriaClause(criteriaClause.getRightCriteria(), items);
        } else {
            if ("IN".equalsIgnoreCase(criteriaClause.getOperator())) {
                if (criteriaClause.getInItems() != null) {
                    items.addAll(criteriaClause.getInItems());
                }
            } else {
                items.add(criteriaClause.getRightExpr());
            }
        }
    }
}
