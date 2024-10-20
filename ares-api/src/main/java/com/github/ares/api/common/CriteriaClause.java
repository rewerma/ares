package com.github.ares.api.common;

import java.io.Serializable;
import java.util.List;

public class CriteriaClause implements Serializable {
    private String operator;

    private String leftExpr;

    private String rightExpr;

    private CriteriaClause leftCriteria;

    private CriteriaClause rightCriteria;

    private List<String> inItems;

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    public String getLeftExpr() {
        return leftExpr;
    }

    public void setLeftExpr(String leftExpr) {
        this.leftExpr = leftExpr;
    }

    public String getRightExpr() {
        return rightExpr;
    }

    public void setRightExpr(String rightExpr) {
        this.rightExpr = rightExpr;
    }

    public CriteriaClause getLeftCriteria() {
        return leftCriteria;
    }

    public void setLeftCriteria(CriteriaClause leftCriteria) {
        this.leftCriteria = leftCriteria;
    }

    public CriteriaClause getRightCriteria() {
        return rightCriteria;
    }

    public void setRightCriteria(CriteriaClause rightCriteria) {
        this.rightCriteria = rightCriteria;
    }

    public List<String> getInItems() {
        return inItems;
    }

    public void setInItems(List<String> inItems) {
        this.inItems = inItems;
    }
}
