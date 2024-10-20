package com.github.ares.connctor.jdbc.catalog.oceanbase;

import com.github.ares.connctor.jdbc.catalog.mysql.MySqlCatalog;
import com.github.ares.connctor.jdbc.utils.JdbcUrlUtil;

public class OceanBaseMySqlCatalog extends MySqlCatalog {

    static {
        SYS_DATABASES.clear();
        SYS_DATABASES.add("information_schema");
        SYS_DATABASES.add("mysql");
        SYS_DATABASES.add("oceanbase");
        SYS_DATABASES.add("LBACSYS");
        SYS_DATABASES.add("ORAAUDITOR");
        SYS_DATABASES.add("SYS");
    }

    public OceanBaseMySqlCatalog(
            String catalogName, String username, String pwd, JdbcUrlUtil.UrlInfo urlInfo) {
        super(catalogName, username, pwd, urlInfo);
    }
}
