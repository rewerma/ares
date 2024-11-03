package com.github.ares.common.utils;

import java.io.Serializable;

public class Tuple2<T1, T2> implements Serializable {
    private static final long serialVersionUID = 1L;

    private final T1 value1;
    private final T2 value2;

    public Tuple2(T1 value1, T2 value2) {
        this.value1 = value1;
        this.value2 = value2;
    }

    public static <T1, T2> Tuple2<T1, T2> of(T1 value1, T2 value2) {
        return new Tuple2<>(value1, value2);
    }

    public T1 _1() {
        return value1;
    }

    public T2 _2() {
        return value2;
    }
}
