package com.github.ares.engine.core;

import com.github.ares.api.table.factory.Factory;
import com.github.ares.com.google.inject.Singleton;
import lombok.Getter;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

@Getter
public class SinkPluginManager implements Serializable {
    private static final long serialVersionUID = -1L;

    private ExecutorManager executorManager;

    public void init(ExecutorManager executorManager) {
        this.executorManager = executorManager;
    }

    public void registerPlugin(String key, Optional<? extends Factory> pluginFactory) {
        executorManager.getSinkPlugins().put(key, pluginFactory);
    }
}
