package com.github.ares.spark.starter;

import com.github.ares.common.exceptions.ParseException;
import com.github.ares.core.starter.command.Common;
import com.github.ares.parser.lake.LakeFormat;
import com.github.ares.parser.lake.LakeTables;
import com.github.ares.parser.plan.LogicalProject;
import java.nio.file.Path;
import java.util.List;

/** Adds lake Spark jars from thirdparty, and Hadoop jars when a warehouse is on HDFS. */
public final class PaimonStarterSupport {
    private PaimonStarterSupport() {}

    public static void rejectFilesystemOnSpark2(LogicalProject project) {
        for (LakeFormat format : LakeFormat.values()) {
            if (LakeTables.projectUsesFilesystem(project, format)) {
                throw new ParseException(
                        format.displayName() + " filesystem catalog requires Spark 3");
            }
        }
    }

    public static void addFilesystemDependencies(List<Path> jars, LogicalProject project) {
        boolean hdfs = false;
        for (LakeFormat format : LakeFormat.values()) {
            if (!LakeTables.projectUsesFilesystem(project, format)) {
                continue;
            }
            List<Path> formatJars = Common.getThirdPartyComponentJars(format.connector());
            if (formatJars.isEmpty()) {
                throw new ParseException(
                        format.displayName()
                                + " filesystem catalog requires thirdparty/"
                                + format.connector());
            }
            addAll(jars, formatJars);
            if (LakeTables.projectUsesHdfs(project, format)) {
                hdfs = true;
            }
        }
        if (hdfs) {
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
