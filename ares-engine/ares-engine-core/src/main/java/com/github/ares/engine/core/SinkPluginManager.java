package com.github.ares.engine.core;

import com.github.ares.api.table.factory.Factory;
import java.io.Serializable;
import java.util.Optional;

public class SinkPluginManager extends AbstractBaseExecutor implements Serializable {
    private static final long serialVersionUID = -1L;

    public void registerPlugin(String key, Optional<? extends Factory> pluginFactory) {
        executorManager.getSinkPlugins().put(key, pluginFactory);
    }
}
