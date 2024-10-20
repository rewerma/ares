package com.github.ares.common.configuration.utils;

import com.github.ares.common.configuration.Option;

import java.util.Objects;

public class Expression {
    private final Condition<?> condition;
    private Boolean and = null;
    private Expression next = null;

    Expression(Condition<?> condition) {
        this.condition = condition;
    }

    public static <T> Expression of(Option<T> option, T expectValue) {
        return new Expression(Condition.of(option, expectValue));
    }

    public static Expression of(Condition<?> condition) {
        return new Expression(condition);
    }

    public Expression and(Expression next) {
        addExpression(true, next);
        return this;
    }

    public Expression or(Expression next) {
        addExpression(false, next);
        return this;
    }

    private void addExpression(boolean and, Expression next) {
        Expression tail = getTailExpression();
        tail.and = and;
        tail.next = next;
    }

    private Expression getTailExpression() {
        return hasNext() ? this.next.getTailExpression() : this;
    }

    public Condition<?> getCondition() {
        return condition;
    }

    public boolean hasNext() {
        return this.next != null;
    }

    public Expression getNext() {
        return this.next;
    }

    public Boolean and() {
        return this.and;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Expression)) {
            return false;
        }
        Expression that = (Expression) obj;
        return Objects.equals(this.condition, that.condition)
                && Objects.equals(this.and, that.and)
                && Objects.equals(this.next, that.next);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.condition, this.and, this.next);
    }

    @Override
    public String toString() {
        Expression cur = this;
        StringBuilder builder = new StringBuilder();
        boolean bracket = false;
        do {
            if (cur.condition.getCount() > 1) {
                builder.append("(").append(cur.condition).append(")");
            } else {
                builder.append(cur.condition);
            }
            if (bracket) {
                builder = new StringBuilder(String.format("(%s)", builder));
                bracket = false;
            }
            if (cur.hasNext()) {
                if (cur.next.hasNext() && !cur.and.equals(cur.next.and)) {
                    bracket = true;
                }
                builder.append(cur.and ? " && " : " || ");
            }
            cur = cur.next;
        } while (cur != null);
        return builder.toString();
    }
}
