package com.github.ares.api.common;

import com.github.ares.common.configuration.Option;
import com.github.ares.common.configuration.Options;

import java.util.List;

public interface CommonOptions {

    Option<String> PLUGIN_NAME =
            Options.key("plugin_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Name of the SPI plugin class.");

    Option<String> CONNECTOR =
            Options.key("connector")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Name of the connector plugin.");

    Option<String> DATA_SOURCE =
            Options.key("datasource")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Datasource name of the connector plugin.");

    Option<String> RESULT_TABLE_NAME =
            Options.key("result_table_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "When result_table_name is not specified, "
                                    + "the data processed by this plugin will not be registered as a data set (dataStream/dataset) "
                                    + "that can be directly accessed by other plugins, or called a temporary table (table)"
                                    + "When result_table_name is specified, "
                                    + "the data processed by this plugin will be registered as a data set (dataStream/dataset) "
                                    + "that can be directly accessed by other plugins, or called a temporary table (table) . "
                                    + "The data set (dataStream/dataset) registered here can be directly accessed by other plugins "
                                    + "by specifying source_table_name .");

    Option<List<String>> SOURCE_TABLE_NAME =
            Options.key("source_table_name")
                    .listType()
                    .noDefaultValue()
                    .withDescription(
                            "When source_table_name is not specified, "
                                    + "the current plug-in processes the data set dataset output by the previous plugin in the configuration file. "
                                    + "When source_table_name is specified, the current plug-in is processing the data set corresponding to this parameter.");

    Option<Integer> PARALLELISM =
            Options.key("parallelism")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "When parallelism is not specified, the parallelism in env is used by default. "
                                    + "When parallelism is specified, it will override the parallelism in env.");

    Option<String> SINK_TYPE =
            Options.key("sink_type").stringType().noDefaultValue().withDescription("INSERT, UPDATE, DELETE, MERGE ...");

    Option<List<String>> INSERT_COLUMNS =
            Options.key("insert_columns").listType().noDefaultValue().withDescription("insert columns");

    Option<List<String>> UPDATE_COLUMNS =
            Options.key("update_columns").listType().noDefaultValue().withDescription("update columns");

    Option<List<String>> WHERE_COLUMNS =
            Options.key("where_columns").listType().noDefaultValue().withDescription("where columns");

    Option<CriteriaClause> WHERE_CLAUSE =
            Options.key("where_clause").objectType(CriteriaClause.class).noDefaultValue().withDescription("where clause");
}
