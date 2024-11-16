package com.github.ares.web.config;

import io.ebean.Database;
import io.ebean.DatabaseFactory;
import io.ebean.config.DatabaseConfig;
import io.ebean.config.UnderscoreNamingConvention;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;

@Configuration
public class EbeanConfig {

    @Bean("ebeanServer")
    public Database ebeanServer(DataSource dataSource) {
        DatabaseConfig serverConfig = new DatabaseConfig();
        serverConfig.setDefaultServer(true);
        serverConfig.setNamingConvention(new UnderscoreNamingConvention());
        List<String> packages = new ArrayList<>();
        packages.add("com.github.ares.web.entity");
        serverConfig.setPackages(packages);
        serverConfig.setName("ebeanServer");
        serverConfig.setDataSource(dataSource);
        serverConfig.setDatabaseSequenceBatchSize(1);
        serverConfig.setDdlGenerate(false);
        serverConfig.setDdlRun(false);
        serverConfig.setDisableLazyLoading(true);
        return DatabaseFactory.create(serverConfig);
    }
}
