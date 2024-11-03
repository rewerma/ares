package com.github.ares.api.table.catalog;


import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

public class ConstraintKey implements Serializable {
    private static final long serialVersionUID = -1L;

    private final ConstraintType constraintType;

    private final String constraintName;

    private final List<ConstraintKeyColumn> columnNames;

    private ConstraintKey(
            ConstraintType constraintType,
            String constraintName,
            List<ConstraintKeyColumn> columnNames) {
        if (constraintType == null) {
            throw new NullPointerException("constraintType must not be null");
        }

        this.constraintType = constraintType;
        this.constraintName = constraintName;
        this.columnNames = columnNames;
    }

    public static ConstraintKey of(
            ConstraintType constraintType,
            String constraintName,
            List<ConstraintKeyColumn> columnNames) {
        return new ConstraintKey(constraintType, constraintName, columnNames);
    }

    public ConstraintType getConstraintType() {
        return constraintType;
    }

    public String getConstraintName() {
        return constraintName;
    }

    public List<ConstraintKeyColumn> getColumnNames() {
        return columnNames;
    }

    public static class ConstraintKeyColumn implements Serializable {
        private final String columnName;
        private final ColumnSortType sortType;

        public ConstraintKeyColumn(String columnName, ColumnSortType sortType) {
            this.columnName = columnName;
            this.sortType = sortType;
        }

        public String getColumnName() {
            return columnName;
        }

        public ColumnSortType getSortType() {
            return sortType;
        }

        public static ConstraintKeyColumn of(String columnName, ColumnSortType sortType) {
            return new ConstraintKeyColumn(columnName, sortType);
        }

        public ConstraintKeyColumn copy() {
            return ConstraintKeyColumn.of(columnName, sortType);
        }
    }

    public enum ConstraintType {
        INDEX_KEY,
        UNIQUE_KEY,
        FOREIGN_KEY
    }

    public enum ColumnSortType {
        ASC,
        DESC
    }

    public ConstraintKey copy() {
        List<ConstraintKeyColumn> collect =
                columnNames.stream().map(ConstraintKeyColumn::copy).collect(Collectors.toList());
        return ConstraintKey.of(constraintType, constraintName, collect);
    }
}
