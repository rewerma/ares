package com.github.ares.api.table.catalog;

import com.github.ares.common.configuration.Option;
import com.github.ares.common.configuration.Options;

import java.util.List;
import java.util.Map;

public interface CatalogOptions {

    @Deprecated
    Option<Map<String, String>> CATALOG_OPTIONS =
            Options.key("catalog")
                    .mapType()
                    .noDefaultValue()
                    .withDescription("configuration options for the catalog.");

    Option<String> NAME =
            Options.key("name").stringType().noDefaultValue().withDescription("catalog name");

    Option<List<String>> TABLE_NAMES =
            Options.key("table-names")
                    .listType()
                    .noDefaultValue()
                    .withDescription(
                            "List of table names of databases to capture."
                                    + "The table name needs to include the database name, for example: database_name.table_name");

    Option<String> DATABASE_PATTERN =
            Options.key("database-pattern")
                    .stringType()
                    .defaultValue(".*")
                    .withDescription("The database names RegEx of the database to capture.");

    Option<String> TABLE_PATTERN =
            Options.key("table-pattern")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The table names RegEx of the database to capture."
                                    + "The table name needs to include the database name, for example: database_.*\\.table_.*");
}
