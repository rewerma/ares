package com.github.ares.connctor.jdbc.test;

import com.github.ares.api.table.catalog.Catalog;
import com.github.ares.api.table.catalog.CatalogTable;
import com.github.ares.api.table.catalog.TablePath;
import com.github.ares.api.table.factory.CatalogFactory;
import com.github.ares.api.table.factory.FactoryUtil;
import com.github.ares.common.configuration.ReadonlyConfig;
import org.junit.Test;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

public class CatalogTest {
    @Test
    public void test01() {

        Optional<CatalogFactory> catalogFactoryOp = FactoryUtil.discoverOptionalFactory(Thread.currentThread().getContextClassLoader(),
                CatalogFactory.class, "ORACLE");
        CatalogFactory catalogFactory = catalogFactoryOp.get();

        Map<String, Object> config = new LinkedHashMap<>();
//        config.put("base-url","jdbc:mysql://127.0.0.1:3306/mytest?useSSL=false");
//        config.put("driver", "com.mysql.cj.jdbc.Driver");
//        config.put("username", "root");
//        config.put("password", "121212");
        config.put("base-url","jdbc:oracle:thin:@127.0.0.1:1521:XE");
        config.put("driver", "oracle.jdbc.OracleDriver");
        config.put("username", "mytest");
        config.put("password", "m121212");


        ReadonlyConfig config1 = ReadonlyConfig.fromMap(config);
        Catalog catalog = catalogFactory.createCatalog("test", config1);
        CatalogTable catalogTable  = catalog.getTable(  TablePath.of("XE","mytest","t_user"));
        catalogTable = catalogTable;
    }
}
