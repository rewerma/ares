package com.github.ares.api.table.catalog.schema;

import com.github.ares.api.table.catalog.Column;
import com.github.ares.api.table.catalog.ConstraintKey;
import com.github.ares.api.table.catalog.PrimaryKey;
import com.github.ares.api.table.catalog.TableSchema;

import java.util.List;

public interface TableSchemaParser<T> {

    /**
     * Parse schema config to TableSchema
     *
     * @param schemaConfig schema config
     * @return TableSchema
     */
    TableSchema parse(T schemaConfig);

    @Deprecated
    interface FieldParser<T> {

        /**
         * Parse field config to List<Column>
         *
         * @param schemaConfig schema config
         * @return List<Column> column list
         */
        List<Column> parse(T schemaConfig);
    }

    interface ColumnParser<T> {

        /**
         * Parse column config to List<Column>
         *
         * @param schemaConfig schema config
         * @return List<Column> column list
         */
        List<Column> parse(T schemaConfig);
    }

    interface ConstraintKeyParser<T> {

        /**
         * Parse constraint key config to ConstraintKey
         *
         * @param schemaConfig schema config
         * @return List<ConstraintKey> constraint key list
         */
        List<ConstraintKey> parse(T schemaConfig);
    }

    interface PrimaryKeyParser<T> {

        /**
         * Parse primary key config to PrimaryKey
         *
         * @param schemaConfig schema config
         * @return PrimaryKey
         */
        PrimaryKey parse(T schemaConfig);
    }
}
