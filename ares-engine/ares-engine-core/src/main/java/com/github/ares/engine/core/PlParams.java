package com.github.ares.engine.core;

import com.github.ares.common.engine.PlType;
import lombok.Getter;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

@Getter
public class PlParams implements Serializable {
    private static final long serialVersionUID = -1L;

    private final Map<String, Serializable> allParams;

    private final Map<String, PlType> paramTypes;

    public PlParams(){
        this.allParams = new LinkedHashMap<>();
        this.paramTypes = new LinkedHashMap<>();
    }
    public PlParams(Map<String, Serializable> allParams, Map<String, PlType> paramTypes) {
        this.allParams = allParams;
        this.paramTypes = paramTypes;
    }

    public void put(String key, Serializable value, PlType type) {
        allParams.put(key, value);
        paramTypes.put(key, type);
    }

    public Serializable get(String key) {
        return allParams.get(key);
    }

    public PlType getType(String key) {
        return paramTypes.get(key);
    }

    public void putAll(Map<String, Serializable> params) {
        allParams.putAll(params);
    }

    public void putAllTypes(Map<String, PlType> types) {
        paramTypes.putAll(types);
    }

    public Object remove(String key) {
        return allParams.remove(key);
    }

    public boolean containsKey(String key) {
        return allParams.containsKey(key);
    }

    public Set<Map.Entry<String, Serializable>> entrySet() {
        return allParams.entrySet();
    }

    public PlParams copy() {
        return new PlParams(new LinkedHashMap<>(this.allParams), new LinkedHashMap<>(this.paramTypes));
    }

    @Override
    public String toString() {
        if (!allParams.isEmpty()) {
            return allParams.toString();
        } else {
            return "{}";
        }
    }
}
