package com.typesafe.config;

import java.io.Serializable;

/**
 * Copy from {@link ConfigMergeable}, in order to make the {@link Config} can be
 * serialized
 */
public interface ConfigMergeable extends Serializable {
    ConfigMergeable withFallback(ConfigMergeable configMergeable);
}
