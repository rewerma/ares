package com.github.ares.common.configuration;

import com.github.ares.com.fasterxml.jackson.core.type.TypeReference;
import lombok.Getter;

import java.util.List;

public class SingleChoiceOption<T> extends Option<T> {

    @Getter private final List<T> optionValues;

    public SingleChoiceOption(
            String key, TypeReference<T> typeReference, List<T> optionValues, T defaultValue) {
        super(key, typeReference, defaultValue);
        this.optionValues = optionValues;
    }

    @Override
    public SingleChoiceOption<T> withDescription(String description) {
        this.description = description;
        return this;
    }
}
