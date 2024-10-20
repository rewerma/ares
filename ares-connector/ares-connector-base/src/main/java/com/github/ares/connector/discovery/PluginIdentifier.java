package com.github.ares.connector.discovery;

import java.util.Objects;

/**
 * Used to identify a plugin.
 */
public class PluginIdentifier {
    private final String pluginType;
    private final String pluginName;

    private PluginIdentifier(String pluginType, String pluginName) {
        this.pluginType = pluginType;
        this.pluginName = pluginName;
    }

    public static PluginIdentifier of(String pluginType, String pluginName) {
        return new PluginIdentifier(pluginType, pluginName);
    }

    public String getPluginName() {
        return pluginName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PluginIdentifier that = (PluginIdentifier) o;

        if (!Objects.equals(pluginType, that.pluginType)) return false;
        return Objects.equals(pluginName, that.pluginName);
    }

    @Override
    public int hashCode() {
        int result = pluginType != null ? pluginType.hashCode() : 0;
        result = 31 * result + (pluginName != null ? pluginName.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "PluginIdentifier{" +
                "pluginType='" + pluginType + '\'' +
                ", pluginName='" + pluginName + '\'' +
                '}';
    }
}
