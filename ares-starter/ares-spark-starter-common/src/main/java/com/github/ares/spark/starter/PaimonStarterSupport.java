package com.github.ares.spark.starter;

import com.github.ares.common.exceptions.ParseException;
import com.github.ares.core.starter.command.Common;
import com.github.ares.parser.paimon.PaimonTables;
import com.github.ares.parser.plan.LogicalProject;
import java.nio.file.Path;
import java.util.List;

/**
 * Adds the Paimon Spark jar from thirdparty/paimon, and Hadoop jars when the warehouse is on HDFS.
 */
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
        List<Path> paimonJars = Common.getThirdPartyPaimonJars();
        if (paimonJars.isEmpty()) {
            throw new ParseException("Paimon filesystem catalog requires thirdparty/paimon");
        }
        addAll(jars, paimonJars);
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
}
