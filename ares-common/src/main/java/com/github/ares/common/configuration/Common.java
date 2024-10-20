package com.github.ares.common.configuration;

import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Common {
    private static String ARES_HOME;

    private static DeployMode MODE = DeployMode.CLIENT;

    private static boolean STARTER = false;

    public static void setDeployMode(DeployMode mode) {
        MODE = mode;
    }

    public static void setStarter(boolean inStarter) {
        STARTER = inStarter;
    }

    public static DeployMode getDeployMode() {
        return MODE;
    }

    public static String getAresHome() {

        if (StringUtils.isNotEmpty(ARES_HOME)) {
            return ARES_HOME;
        }
        String aresHome = System.getProperty("ARES_HOME");
        if (StringUtils.isBlank(aresHome)) {
            aresHome = System.getenv("ARES_HOME");
        }
        if (StringUtils.isBlank(aresHome)) {
            aresHome = appRootDir4Debug().toString();
        }
        ARES_HOME = aresHome;
        return ARES_HOME;
    }

    public static Path appRootDir4Debug() {
        if (DeployMode.CLIENT == MODE || DeployMode.RUN == MODE || STARTER) {
            try {
                String path =
                        Common.class
                                .getProtectionDomain()
                                .getCodeSource()
                                .getLocation()
                                .toURI()
                                .getPath();
                path = new File(path).getPath();
                return Paths.get(path).getParent().getParent().getParent();
            } catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }
        } else if (DeployMode.CLUSTER == MODE || DeployMode.RUN_APPLICATION == MODE) {
            return Paths.get("");
        } else {
            throw new IllegalStateException("deploy mode not support : " + MODE);
        }
    }

    public static Path connectorDir() {
        return Paths.get(getAresHome(), "connectors");
    }
}
