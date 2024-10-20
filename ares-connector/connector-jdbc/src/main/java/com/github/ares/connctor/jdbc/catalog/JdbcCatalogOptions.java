package com.github.ares.connctor.jdbc.catalog;

import com.github.ares.common.configuration.Option;
import com.github.ares.common.configuration.Options;
import com.github.ares.common.configuration.utils.OptionRule;

public interface JdbcCatalogOptions {
    Option<String> BASE_URL =
            Options.key("base-url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "URL has to be with database, like \"jdbc:mysql://localhost:5432/db\" or"
                                    + "\"jdbc:mysql://localhost:5432/db?useSSL=true\".");

    Option<String> USERNAME =
            Options.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Name of the database to use when connecting to the database server.");

    Option<String> PASSWORD =
            Options.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Password to use when connecting to the database server.");

    Option<String> SCHEMA =
            Options.key("schema")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "for databases that support the schema parameter, give it priority.");

    Option<String> COMPATIBLE_MODE =
            Options.key("compatibleMode")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The compatible mode of database, required when the database supports multiple compatible modes. "
                                    + "For example, when using OceanBase database, you need to set it to 'mysql' or 'oracle'.");

    OptionRule.Builder BASE_RULE =
            OptionRule.builder().required(BASE_URL).required(USERNAME, PASSWORD).optional(SCHEMA);

    Option<String> TABLE_PREFIX =
            Options.key("tablePrefix")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The table prefix name added when the table is automatically created");

    Option<String> TABLE_SUFFIX =
            Options.key("tableSuffix")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The table suffix name added when the table is automatically created");
}
