package com.github.ares.common.configuration;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public enum DeployMode {
    /** Spark */
    CLIENT("client"),
    CLUSTER("cluster"),

    /** Flink */
    RUN("run"),
    RUN_APPLICATION("run-application");

    private final String deployMode;

    DeployMode(String deployMode) {
        this.deployMode = deployMode;
    }

    public String getDeployMode() {
        return deployMode;
    }

    private static final Map<String, DeployMode> NAME_MAP =
            Arrays.stream(DeployMode.values())
                    .collect(Collectors.toMap(DeployMode::getDeployMode, Function.identity()));

    public static Optional<DeployMode> from(String deployMode) {
        return Optional.ofNullable(NAME_MAP.get(deployMode.toLowerCase()));
    }
}
