package com.github.ares.core.starter.utils;

import com.github.ares.common.exceptions.AresException;
import com.github.ares.core.starter.command.AbstractCommandArgs;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FileUtils {
    public static Path getSqlPath(AbstractCommandArgs args) {
        switch (args.getDeployMode()) {
            case RUN:
            case CLIENT:
                return Paths.get(args.getSqlFile());
            case RUN_APPLICATION:
            case CLUSTER:
                return Paths.get(getFileName(args.getSqlFile()));
            default:
                throw new IllegalArgumentException(
                        "Unsupported deploy mode: " + args.getDeployMode());
        }
    }

    private static String getFileName(String filePath) {
        return filePath.substring(filePath.lastIndexOf(File.separatorChar) + 1);
    }

    public static void checkSqlScriptExist(Path sqlFile) {
        if (!sqlFile.toFile().exists()) {
            String message = "Can't find SQL script file: " + sqlFile;
            throw new AresException(message);
        }
    }
}
