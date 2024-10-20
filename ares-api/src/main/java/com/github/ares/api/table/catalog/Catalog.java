package com.github.ares.api.table.catalog;

import com.github.ares.common.configuration.ReadonlyConfig;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Interface for reading and writing table metadata from Ares. Each connector need to contain
 * the implementation of Catalog.
 */
public interface Catalog extends AutoCloseable {

    /*default Optional<Factory> getFactory() {
        return Optional.empty();
    }*/

    /**
     * Open the catalog. Used for any required preparation in initialization phase.
     */
    void open();

    /**
     * Close the catalog when it is no longer needed and release any resource that it might be
     * holding.
     */
    void close();

    // --------------------------------------------------------------------------------------------
    // database
    // --------------------------------------------------------------------------------------------

    /**
     * Get the name of the default database for this catalog. The default database will be the
     * current database for the catalog when user's session doesn't specify a current database. The
     * value probably comes from configuration, will not change for the life time of the catalog
     * instance.
     *
     * @return the name of the current database
     */
    String getDefaultDatabase();

    /**
     * Check if a database exists in this catalog.
     *
     * @param databaseName Name of the database
     * @return true if the given database exists in the catalog false otherwise
     */
    boolean databaseExists(String databaseName);

    /**
     * Get the names of all databases in this catalog.
     *
     * @return a list of the names of all databases
     */
    List<String> listDatabases();

    // --------------------------------------------------------------------------------------------
    // table
    // --------------------------------------------------------------------------------------------

    /**
     * Get names of all tables under this database. An empty list is returned if none exists.
     *
     * @return a list of the names of all tables in this database
     */
    List<String> listTables(String databaseName);

    /**
     * Check if a table exist in this catalog.
     *
     * @param tablePath Path of the table
     * @return true if the given table exists in the catalog false otherwise
     */
    boolean tableExists(TablePath tablePath);

    /**
     * Return a {@link CatalogTable} identified by the given {@link TablePath}. The framework will
     * resolve the metadata objects when necessary.
     *
     * @param tablePath Path of the table
     * @return The requested table
     */
    CatalogTable getTable(TablePath tablePath);

    default List<CatalogTable> getTables(ReadonlyConfig config) {
        // Get the list of specified tables
        List<String> tableNames = config.get(CatalogOptions.TABLE_NAMES);
        List<CatalogTable> catalogTables = new ArrayList<>();
        if (tableNames != null && !tableNames.isEmpty()) {
            for (String tableName : tableNames) {
                TablePath tablePath = TablePath.of(tableName);
                if (this.tableExists(tablePath)) {
                    catalogTables.add(this.getTable(tablePath));
                }
            }
            return catalogTables;
        }

        // Get the list of table pattern
        String tablePatternStr = config.get(CatalogOptions.TABLE_PATTERN);
        if (StringUtils.isEmpty(tablePatternStr)) {
            return Collections.emptyList();
        }
        Pattern databasePattern = Pattern.compile(config.get(CatalogOptions.DATABASE_PATTERN));
        Pattern tablePattern = Pattern.compile(config.get(CatalogOptions.TABLE_PATTERN));
        List<String> allDatabase = this.listDatabases();
        allDatabase.removeIf(s -> !databasePattern.matcher(s).matches());
        for (String databaseName : allDatabase) {
            tableNames = this.listTables(databaseName);
            for (String tableName : tableNames) {
                if (tablePattern.matcher(databaseName + "." + tableName).matches()) {
                    catalogTables.add(this.getTable(TablePath.of(databaseName, tableName)));
                }
            }
        }
        return catalogTables;
    }

    /**
     * Create a new table in this catalog.
     *
     * @param tablePath Path of the table
     * @param table The table definition
     * @param ignoreIfExists Flag to specify behavior when a table with the given name already exist
     */
    void createTable(TablePath tablePath, CatalogTable table, boolean ignoreIfExists);

    /**
     * Drop an existing table in this catalog.
     *
     * @param tablePath Path of the table
     * @param ignoreIfNotExists Flag to specify behavior when a table with the given name doesn't
     *     exist
     */
    void dropTable(TablePath tablePath, boolean ignoreIfNotExists);

    void createDatabase(TablePath tablePath, boolean ignoreIfExists);

    void dropDatabase(TablePath tablePath, boolean ignoreIfNotExists);

    /**
     * Truncate an existing table data in this catalog.
     *
     * @param tablePath Path of the table
     * @param ignoreIfNotExists Flag to specify behavior when a table with the given name doesn't
     *     exist
     */
    default void truncateTable(TablePath tablePath, boolean ignoreIfNotExists) {}

    default boolean isExistsData(TablePath tablePath) {
        return false;
    }

    default void executeSql(TablePath tablePath, String sql) {}

    // todo: Support for update table metadata

}
