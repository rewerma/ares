package com.github.ares.spark.starter;

import com.github.ares.common.exceptions.ParseException;
import com.github.ares.connector.discovery.AresSinkPluginDiscovery;
import com.github.ares.connector.discovery.PluginIdentifier;
import com.github.ares.core.starter.command.Common;
import com.github.ares.core.starter.enums.PluginType;
import com.github.ares.parser.paimon.PaimonTables;
import com.github.ares.parser.plan.LogicalProject;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/** Adds the Paimon Spark jar, and Hadoop jars when the warehouse is on HDFS. */
public final class PaimonStarterSupport {
    private PaimonStarterSupport() {}

    public static void rejectFilesystemOnSpark2(LogicalProject project) {
        if (PaimonTables.projectUsesFilesystem(project)) {
            throw new ParseException("Paimon filesystem catalog requires Spark 3");
        }
    }

    public static void addFilesystemDependencies(List<Path> jars, LogicalProject project) {
        if (!PaimonTables.projectUsesFilesystem(project)) {
            return;
        }
        List<Path> connectorJars = connectorJars();
        if (connectorJars.isEmpty()) {
            throw new ParseException(
                    "Paimon filesystem catalog requires connectors/connector-paimon.jar");
        }
        addAll(jars, connectorJars);
        if (PaimonTables.projectUsesHdfs(project)) {
            addAll(jars, Common.getThirdPartyHadoopJars());
        }
    }

    private static void addAll(List<Path> jars, List<Path> extra) {
        for (Path jar : extra) {
            if (!jars.contains(jar)) {
                jars.add(jar);
            }
        }
    }

    private static List<Path> connectorJars() {
        Path pluginRootDir = Common.connectorDir();
        if (!Files.exists(pluginRootDir) || !Files.isDirectory(pluginRootDir)) {
            return Collections.emptyList();
        }
        AresSinkPluginDiscovery discovery = new AresSinkPluginDiscovery(pluginRootDir);
        return discovery
                .getPluginJarPaths(
                        Collections.singletonList(
                                PluginIdentifier.of(
                                        PluginType.SINK.getType(), PaimonTables.CONNECTOR)))
                .stream()
                .map(url -> new File(url.getPath()).toPath())
                .collect(Collectors.toList());
    }
}
