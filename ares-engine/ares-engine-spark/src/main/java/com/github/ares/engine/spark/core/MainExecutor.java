package com.github.ares.engine.spark.core;

import com.github.ares.api.common.EngineType;
import com.github.ares.api.common.EngineTypeVersion;
import com.github.ares.api.common.ExecutionEngineType;
import com.github.ares.api.source.SourceTableInfo;
import com.github.ares.api.table.factory.Factory;
import com.github.ares.api.table.factory.TableSinkFactory;
import com.github.ares.api.table.factory.TableSourceFactory;
import com.github.ares.com.google.inject.Inject;
import com.github.ares.com.google.inject.Injector;
import com.github.ares.common.configuration.DeployMode;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.connector.discovery.AresFactoryDiscovery;
import com.github.ares.connector.discovery.AresSinkPluginDiscovery;
import com.github.ares.connector.discovery.PluginIdentifier;
import com.github.ares.connector.utils.PluginUtil;
import com.github.ares.engine.core.AbstractRootExecutor;
import com.github.ares.engine.spark.config.SparkInjectorFactory;
import com.github.ares.parser.PlParser;
import com.github.ares.parser.config.PlProperties;
import com.github.ares.parser.datasource.PropertiesDataSourceComplement;
import com.github.ares.parser.datasource.SourceConfigComplementFactory;
import com.github.ares.parser.plan.LogicalCreateSinkTable;
import com.github.ares.parser.plan.LogicalCreateSourceTable;
import com.github.ares.parser.plan.LogicalOperation;
import com.github.ares.parser.plan.LogicalProject;
import com.github.ares.parser.plan.LogicalSetConfig;
import com.github.ares.parser.utils.Constants;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkFiles;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static com.github.ares.common.utils.Constants.DEPLOY_MODE_KEY;

public class MainExecutor {
    private Properties properties;
    private Path scriptFile;
    private LogicalProject logicalProject;

    @Inject
    private AbstractRootExecutor rootExecutor;
    @Inject
    private SparkExecutorManager sparkExecutorManager;
    @Inject
    private PlProperties plProperties;
    @Inject
    private PlParser plParser;

    public static MainExecutor getInstance() {
        Injector injector = SparkInjectorFactory.getInjector();
        return injector.getInstance(MainExecutor.class);
    }

    public void init(EngineTypeVersion engineTypeVersion, SparkSession sparkSession, Path sqlScriptPath, Properties properties) {
        ExecutionEngineType.init(EngineType.SPARK, engineTypeVersion);
        this.properties = properties;
        // register datasource patcher
        SourceConfigComplementFactory.register(Constants.DEFAULT_DATASOURCE_COMPLEMENT,
                new PropertiesDataSourceComplement(properties));

        plParser.init();

        DeployMode deployMode = (DeployMode) properties.remove(DEPLOY_MODE_KEY);
        if (deployMode == DeployMode.CLUSTER) {
            String scriptFilePath = SparkFiles.get(sqlScriptPath.getFileName().toString());
            this.scriptFile = Paths.get(scriptFilePath);
        } else {
            this.scriptFile = sqlScriptPath;
        }

        // parse script to logical plan
        this.logicalProject = parseScript();
        initProperties();

        sparkExecutorManager.init(this.plProperties, sparkSession);

        // initialize source plugins
        List<LogicalCreateSourceTable> sourceTables = new ArrayList<>();
        logicalProject.getLogicalOperations().forEach(logicalOperation -> {
            if (logicalOperation instanceof LogicalCreateSourceTable) {
                sourceTables.add((LogicalCreateSourceTable) logicalOperation);
            }
        });
        Map<String, SourceTableInfo> sourceTableMap = initializeSourcePlugins(sourceTables);
        sparkExecutorManager.getSourceTables().putAll(sourceTableMap);
        rootExecutor.init(sparkExecutorManager);

        // initialize sink plugins
        List<LogicalCreateSinkTable> sinkTables = new ArrayList<>();
        logicalProject.getLogicalOperations().forEach(logicalOperation -> {
            if (logicalOperation instanceof LogicalCreateSinkTable) {
                sinkTables.add((LogicalCreateSinkTable) logicalOperation);
            }
        });
        initializeSinkPlugins(sinkTables);
    }

    private void initProperties() {
        Properties innerProperties = new Properties();
        for (LogicalOperation operation : this.logicalProject.getLogicalOperations()) {
            if (operation instanceof LogicalSetConfig) {
                LogicalSetConfig setConfig = (LogicalSetConfig) operation;
                innerProperties.put(setConfig.getKey(), setConfig.getValue());
            }
        }
        innerProperties.putAll(this.properties);
        this.properties.putAll(innerProperties);

        plProperties.init(properties);
    }

    public void run() {
        // set log level
        String logLevel = properties.getProperty("spark.log.level");
        if (StringUtils.isNotEmpty(logLevel)) {
            SparkContext sc = sparkExecutorManager.getSparkSessionManager()
                    .getSparkSession().sparkContext();
            sc.setLogLevel(logLevel);
        }

        rootExecutor.execute(logicalProject);

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private LogicalProject parseScript() {
        try (InputStream in = Files.newInputStream(scriptFile)) {
            return plParser.parseToBaseBody(in);
        } catch (IOException e) {
            throw new AresException(e);
        }
    }

    protected Map<String, SourceTableInfo> initializeSourcePlugins(List<LogicalCreateSourceTable> createSourceTables) {
        AresFactoryDiscovery factoryDiscovery = new AresFactoryDiscovery(TableSourceFactory.class);

        Map<String, SourceTableInfo> sources = new LinkedHashMap<>();
        for (LogicalCreateSourceTable sourceTable : createSourceTables) {
            PluginIdentifier pluginIdentifier =
                    PluginIdentifier.of("source", sourceTable.getConnector());
            SourceTableInfo source =
                    PluginUtil.createSource(
                            factoryDiscovery,
                            pluginIdentifier,
                            sourceTable.getSourceTableConfig());
            sources.put(sourceTable.getTableName(), source);
        }
        return sources;
    }

    protected void initializeSinkPlugins(List<LogicalCreateSinkTable> sinkTables) {
        AresFactoryDiscovery factoryDiscovery =
                new AresFactoryDiscovery(TableSinkFactory.class);
        AresSinkPluginDiscovery sinkPluginDiscovery = new AresSinkPluginDiscovery();
        Map<String, Optional<? extends Factory>> sinks = new LinkedHashMap<>();
        for (LogicalCreateSinkTable sinkTable : sinkTables) {
            if (sinks.containsKey(sinkTable.getTableName())) {
                continue;
            }
            Optional<? extends Factory> factory = PluginUtil.createSinkFactory(factoryDiscovery, sinkPluginDiscovery,
                    sinkTable.getConnector(), new ArrayList<>());
            sinks.put(sinkTable.getTableName(), factory);
            sparkExecutorManager.getSinkPluginManager().registerPlugin(sinkTable.getTableName(), factory);
        }
    }

    public void close() {
        sparkExecutorManager.getSparkSessionManager().close();
    }
}
