/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.ares.api.table.catalog;


import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.AresRowType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Represent a physical table schema.
 */
public final class TableSchema implements Serializable {
    private static final long serialVersionUID = -1L;
    private final List<Column> columns;

    private final List<Column> whereColumns;

    private final PrimaryKey primaryKey;

    private final List<ConstraintKey> constraintKeys;

    public TableSchema(List<Column> columns, List<Column> whereColumns, PrimaryKey primaryKey, List<ConstraintKey> constraintKeys) {
        this.columns = columns;
        this.whereColumns = whereColumns;
        this.primaryKey = primaryKey;
        this.constraintKeys = constraintKeys;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public List<Column> getWhereColumns() {
        return whereColumns;
    }

    public PrimaryKey getPrimaryKey() {
        return primaryKey;
    }

    public List<ConstraintKey> getConstraintKeys() {
        return constraintKeys;
    }

    public static Builder builder() {
        return new Builder();
    }

    public AresRowType toPhysicalRowDataType() {
        AresDataType<?>[] fieldTypes =
                columns.stream()
                        .filter(Column::isPhysical)
                        .map(Column::getDataType)
                        .toArray(AresDataType[]::new);
        String[] fields =
                columns.stream()
                        .filter(Column::isPhysical)
                        .map(Column::getName)
                        .toArray(String[]::new);
        return new AresRowType(fields, fieldTypes);
    }

    public static final class Builder {
        private final List<Column> columns = new ArrayList<>();

        private final List<Column> whereColumns = new ArrayList<>();

        private PrimaryKey primaryKey;

        private final List<ConstraintKey> constraintKeys = new ArrayList<>();

        public Builder columns(List<Column> columns) {
            this.columns.addAll(columns);
            return this;
        }

        public Builder whereColumns(List<Column> columns) {
            this.whereColumns.addAll(columns);
            return this;
        }

        public Builder column(Column column) {
            this.columns.add(column);
            return this;
        }

        public Builder primaryKey(PrimaryKey primaryKey) {
            this.primaryKey = primaryKey;
            return this;
        }

        public Builder constraintKey(ConstraintKey constraintKey) {
            this.constraintKeys.add(constraintKey);
            return this;
        }

        public Builder constraintKey(List<ConstraintKey> constraintKeys) {
            this.constraintKeys.addAll(constraintKeys);
            return this;
        }

        public TableSchema build() {
            return new TableSchema(columns, whereColumns, primaryKey, constraintKeys);
        }
    }

    public TableSchema copy() {
        List<Column> copyColumns = columns.stream().map(Column::copy).collect(Collectors.toList());
        List<ConstraintKey> copyConstraintKeys =
                constraintKeys.stream().map(ConstraintKey::copy).collect(Collectors.toList());
        return TableSchema.builder()
                .constraintKey(copyConstraintKeys)
                .columns(copyColumns)
                .primaryKey(primaryKey == null ? null : primaryKey.copy())
                .build();
    }
}
