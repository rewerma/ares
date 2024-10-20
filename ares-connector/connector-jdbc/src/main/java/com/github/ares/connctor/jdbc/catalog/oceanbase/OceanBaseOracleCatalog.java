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

package com.github.ares.connctor.jdbc.catalog.oceanbase;

import com.github.ares.api.table.catalog.CatalogTable;
import com.github.ares.api.table.catalog.TablePath;
import com.github.ares.api.table.catalog.exception.DatabaseNotExistException;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.connctor.jdbc.catalog.oracle.OracleCatalog;
import com.github.ares.connctor.jdbc.utils.JdbcUrlUtil;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.github.ares.com.google.common.base.Preconditions.checkNotNull;


public class OceanBaseOracleCatalog extends OracleCatalog {

    static {
        EXCLUDED_SCHEMAS =
                Collections.unmodifiableList(
                        Arrays.asList("oceanbase", "LBACSYS", "ORAAUDITOR", "SYS"));
    }

    public OceanBaseOracleCatalog(
            String catalogName,
            String username,
            String pwd,
            JdbcUrlUtil.UrlInfo urlInfo,
            String defaultSchema) {
        super(catalogName, username, pwd, urlInfo, defaultSchema);
    }

    @Override
    protected String getListDatabaseSql() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException {
        String dbUrl = getUrlFromDatabaseName(databaseName);
        try {
            return queryString(dbUrl, getListTableSql(databaseName), this::getTableName);
        } catch (Exception e) {
            throw new AresException(
                    String.format("Failed listing database in catalog %s", catalogName), e);
        }
    }

    @Override
    public boolean tableExists(TablePath tablePath) {
        try {
            return listTables(tablePath.getDatabaseName()).contains(getTableName(tablePath));
        } catch (DatabaseNotExistException e) {
            return false;
        }
    }

    @Override
    public void createTable(TablePath tablePath, CatalogTable table, boolean ignoreIfExists)
            throws DatabaseNotExistException {
        checkNotNull(tablePath, "Table path cannot be null");

        if (defaultSchema.isPresent()) {
            tablePath =
                    new TablePath(
                            tablePath.getDatabaseName(),
                            defaultSchema.get(),
                            tablePath.getTableName());
        }

        if (tableExists(tablePath)) {
            if (ignoreIfExists) {
                return;
            }
            throw new AresException(String.format("Table %s already exist in Catalog %s.", catalogName, tablePath));
        }

        createTableInternal(tablePath, table);
    }
}
