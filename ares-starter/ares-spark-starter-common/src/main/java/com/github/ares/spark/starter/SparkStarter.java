package com.github.ares.spark.starter;

import com.github.ares.com.google.inject.Guice;
import com.github.ares.com.google.inject.Injector;
import com.github.ares.common.configuration.DeployMode;
import com.github.ares.common.exceptions.ParseException;
import com.github.ares.common.utils.InjectorFactory;
import com.github.ares.connector.discovery.AresSinkPluginDiscovery;
import com.github.ares.connector.discovery.AresSourcePluginDiscovery;
import com.github.ares.connector.discovery.PluginIdentifier;
import com.github.ares.core.starter.Starter;
import com.github.ares.core.starter.command.Common;
import com.github.ares.core.starter.enums.EngineType;
import com.github.ares.core.starter.enums.PluginType;
import com.github.ares.core.starter.utils.CommandLineUtils;
import com.github.ares.parser.PlParser;
import com.github.ares.parser.config.ParserServiceModule;
import com.github.ares.parser.datasource.PropertiesDataSourceComplement;
import com.github.ares.parser.datasource.SourceConfigComplementFactory;
import com.github.ares.parser.model.TableWith;
import com.github.ares.parser.plan.LogicalOperation;
import com.github.ares.parser.plan.LogicalProject;
import com.github.ares.parser.plan.LogicalSetConfig;
import com.github.ares.parser.utils.Constants;
import com.github.ares.spark.starter.args.SparkCommandArgs;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SparkStarter implements Starter {
    /**
     * original commandline args
     */
    protected String[] args;

    /**
     * args parsed from {@link #args}
     */
    protected SparkCommandArgs commandArgs;

    /**
     * jars to include on the spark driver and executor classpaths
     */
    protected List<Path> jars = new ArrayList<>();

    /**
     * files to be placed in the working directory of each spark executor
     */
    protected List<Path> files = new ArrayList<>();

    protected PlParser plParser;

    /**
     * spark configuration properties
     */
    protected Map<String, String> sparkConf;

    private SparkStarter(String[] args, SparkCommandArgs commandArgs) {
        this.args = args;
        this.commandArgs = commandArgs;
        Injector injector = InjectorFactory.init(Guice.createInjector(new ParserServiceModule()));
        this.plParser = injector.getInstance(PlParser.class);
        this.plParser.init();
    }

    public static void main(String[] args) throws Exception {
        SparkStarter starter = getInstance(args);
        List<String> command = starter.buildCommands();
        System.out.println(String.join(" ", command));
    }

    static SparkStarter getInstance(String[] args) {
        SparkCommandArgs commandArgs =
                CommandLineUtils.parse(
                        args,
                        new SparkCommandArgs(),
                        "",
                        true);
        DeployMode deployMode = commandArgs.getDeployMode();
        switch (deployMode) {
            case CLUSTER:
                return new ClusterModeSparkStarter(args, commandArgs);
            case CLIENT:
                return new ClientModeSparkStarter(args, commandArgs);
            default:
                throw new IllegalArgumentException("Deploy mode " + deployMode + " not supported");
        }
    }

    @Override
    public List<String> buildCommands() throws Exception {
        setSparkConf();
        Common.setDeployMode(commandArgs.getDeployMode());
        Common.setStarter(true);
        this.jars.addAll(Common.getPluginsJarDependencies());
        this.jars.addAll(Common.getLibJars());
        this.jars.addAll(getConnectorJarDependencies());

        return buildFinal();
    }


    /**
     * parse spark configurations from Ares config file
     */
    private void setSparkConf() {
        commandArgs.getVariables().stream()
                .filter(Objects::nonNull)
                .map(variable -> variable.split("=", 2))
                .filter(pair -> pair.length == 2)
                .forEach(pair -> System.setProperty(pair[0], pair[1]));
        this.sparkConf = new LinkedHashMap<>();
        String driverJavaOpts = this.sparkConf.getOrDefault("spark.driver.extraJavaOptions", "");
        String executorJavaOpts =
                this.sparkConf.getOrDefault("spark.executor.extraJavaOptions", "");
        if (!commandArgs.getVariables().isEmpty()) {
            String properties =
                    commandArgs.getVariables().stream()
                            .map(v -> "-D" + v)
                            .collect(Collectors.joining(" "));
            driverJavaOpts += " " + properties;
            executorJavaOpts += " " + properties;
            this.sparkConf.put("spark.driver.extraJavaOptions", driverJavaOpts.trim());
            this.sparkConf.put("spark.executor.extraJavaOptions", executorJavaOpts.trim());
        }
    }

    /**
     * append spark configurations to StringBuilder
     */
    protected void appendSparkConf(List<String> commands, Map<String, String> sparkConf) {
        for (Map.Entry<String, String> entry : sparkConf.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            appendOption(commands, "--conf", key + "=" + value);
        }
    }

    /**
     * append option to StringBuilder
     */
    protected void appendOption(List<String> commands, String option, String value) {
        commands.add(option);
        commands.add("\"" + value.replace("\"", "\\\"") + "\"");
    }

    /**
     * append jars option to StringBuilder
     */
    protected void appendJars(List<String> commands, List<Path> paths) {
        appendPaths(commands, "--jars", paths);
    }

    /**
     * append comma-split paths option to StringBuilder
     */
    protected void appendPaths(List<String> commands, String option, List<Path> paths) {
        if (!paths.isEmpty()) {
            String values = paths.stream().map(Path::toString).collect(Collectors.joining(","));
            appendOption(commands, option, values);
        }
    }

    /**
     * append files option to StringBuilder
     */
    protected void appendFiles(List<String> commands, List<Path> paths) {
        Path scriptPath = Paths.get(this.commandArgs.getSqlFile());
        if (!paths.contains(scriptPath)) {
            paths.add(scriptPath);
        }
        appendPaths(commands, "--files", paths);
    }

    /**
     * append appJar to StringBuilder
     */
    protected void appendAppJar(List<String> commands) {
        commands.add(
                Common.appStarterDir().resolve(EngineType.SPARK3.getStarterJarName()).toString());
    }

    protected List<String> buildFinal() {
        List<String> commands = new ArrayList<>();
        commands.add("${SPARK_HOME}/bin/spark-submit");
        appendOption(commands, "--class", "com.github.ares.spark.starter.AresSparkStarter");
        appendOption(commands, "--name", this.commandArgs.getJobName());
        appendOption(commands, "--master", this.commandArgs.getMaster());
        appendOption(commands, "--deploy-mode", this.commandArgs.getDeployMode().getDeployMode());
        for (String conf : this.commandArgs.getConfList()) {
            appendOption(commands, "--conf", conf);
        }
        appendJars(commands, this.jars);
        appendFiles(commands, this.files);
        appendSparkConf(commands, this.sparkConf);
        appendAppJar(commands);
        appendOption(commands, "--sql", this.commandArgs.getSqlFile());
        appendOption(commands, "--master", this.commandArgs.getMaster());
        appendOption(commands, "--deploy-mode", this.commandArgs.getDeployMode().getDeployMode());
        appendOption(commands, "--name", this.commandArgs.getJobName());
        for (String conf : this.commandArgs.getConfList()) {
            appendOption(commands, "--conf", conf);
        }
        if (commandArgs.isEncrypt()) {
            commands.add("--encrypt");
        }
        if (commandArgs.isDecrypt()) {
            commands.add("--decrypt");
        }
        if (this.commandArgs.isCheckConfig()) {
            commands.add("--check");
        }
        return commands;
    }

    private List<Path> getConnectorJarDependencies() {
        Path pluginRootDir = Common.connectorDir();
        if (!Files.exists(pluginRootDir) || !Files.isDirectory(pluginRootDir)) {
            return Collections.emptyList();
        }
        Properties innerProperties = new Properties();
        SourceConfigComplementFactory.register(Constants.DEFAULT_DATASOURCE_COMPLEMENT,
                new PropertiesDataSourceComplement(innerProperties));

        LogicalProject logicalProject;
        try (InputStream in = Files.newInputStream(Paths.get(commandArgs.getSqlFile()))) {
            logicalProject = plParser.parseToBaseBody(in);
        } catch (IOException e) {
            throw new ParseException(e.getMessage(), e);
        }
        for (LogicalOperation operation : logicalProject.getLogicalOperations()) {
            if (operation instanceof LogicalSetConfig) {
                LogicalSetConfig setConfig = (LogicalSetConfig) operation;
                innerProperties.put(setConfig.getKey(), setConfig.getValue());
            }
        }

        Set<URL> pluginJars = new HashSet<>();
        AresSourcePluginDiscovery aresSourcePluginDiscovery =
                new AresSourcePluginDiscovery(pluginRootDir);
        AresSinkPluginDiscovery aresSinkPluginDiscovery =
                new AresSinkPluginDiscovery(pluginRootDir);
        pluginJars.addAll(
                aresSourcePluginDiscovery.getPluginJarPaths(
                        getPluginIdentifiers(logicalProject, PluginType.SOURCE)));
        pluginJars.addAll(
                aresSinkPluginDiscovery.getPluginJarPaths(
                        getPluginIdentifiers(logicalProject, PluginType.SINK)));
        return pluginJars.stream()
                .map(url -> new File(url.getPath()).toPath())
                .collect(Collectors.toList());
    }

    private static List<PluginIdentifier> getPluginIdentifiers(LogicalProject logicalProject, PluginType... pluginTypes) {
        return Arrays.stream(pluginTypes)
                .flatMap(
                        (Function<PluginType, Stream<PluginIdentifier>>)
                                pluginType -> {
                                    List<TableWith> tableConfigs = new ArrayList<>();
                                    if (pluginType == PluginType.SOURCE) {
                                        tableConfigs.addAll(logicalProject.getSourceTables());
                                    } else {
                                        tableConfigs.addAll(logicalProject.getSinkTables());
                                    }
                                    return tableConfigs.stream().map(sourceTableConf ->
                                            PluginIdentifier.of(
                                                    pluginType.getType(),
                                                    sourceTableConf.getConnector()));
                                })
                .collect(Collectors.toList());
    }


    private static class ClientModeSparkStarter extends SparkStarter {

        /**
         * client mode specified spark options
         */
        private enum ClientModeSparkConfigs {

            /**
             * Memory for driver in client mode
             */
            DriverMemory("--driver-memory", "spark.driver.memory"),

            /**
             * Extra Java options to pass to the driver in client mode
             */
            DriverJavaOptions("--driver-java-options", "spark.driver.extraJavaOptions"),

            /**
             * Extra library path entries to pass to the driver in client mode
             */
            DriverLibraryPath(" --driver-library-path", "spark.driver.extraLibraryPath"),

            /**
             * Extra class path entries to pass to the driver in client mode
             */
            DriverClassPath("--driver-class-path", "spark.driver.extraClassPath");

            private final String optionName;

            private final String propertyName;

            private static final Map<String, ClientModeSparkConfigs> PROPERTY_NAME_MAP =
                    new HashMap<>();

            static {
                for (ClientModeSparkConfigs config : values()) {
                    PROPERTY_NAME_MAP.put(config.propertyName, config);
                }
            }

            ClientModeSparkConfigs(String optionName, String propertyName) {
                this.optionName = optionName;
                this.propertyName = propertyName;
            }
        }

        private ClientModeSparkStarter(String[] args, SparkCommandArgs commandArgs) {
            super(args, commandArgs);
        }

        @Override
        protected void appendSparkConf(List<String> commands, Map<String, String> sparkConf) {
            for (ClientModeSparkConfigs config : ClientModeSparkConfigs.values()) {
                String driverJavaOptions = this.sparkConf.get(config.propertyName);
                if (!StringUtils.isEmpty(driverJavaOptions)) {
                    appendOption(commands, config.optionName, driverJavaOptions);
                }
            }
            for (Map.Entry<String, String> entry : sparkConf.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                if (ClientModeSparkConfigs.PROPERTY_NAME_MAP.containsKey(key)) {
                    continue;
                }
                appendOption(commands, "--conf", key + "=" + value);
            }
        }
    }

    /**
     * a Starter for building spark-submit commands with cluster mode options
     */
    private static class ClusterModeSparkStarter extends SparkStarter {

        private ClusterModeSparkStarter(String[] args, SparkCommandArgs commandArgs) {
            super(args, commandArgs);
        }

        @Override
        public List<String> buildCommands() throws Exception {
            Common.setDeployMode(commandArgs.getDeployMode());
            Common.setStarter(true);
//            Path pluginTarball = Common.pluginTarball();
//            CompressionUtils.tarGzip(Common.pluginRootDir(), pluginTarball);
//            this.files.add(pluginTarball);
            this.files.add(Paths.get(commandArgs.getSqlFile()));
            return super.buildCommands();
        }
    }
}
