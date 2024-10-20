package com.github.ares.common.utils;

import java.io.Serializable;

public class Tuple2<T1, T2> implements Serializable {
    private static final long serialVersionUID = 1L;

    private final T1 _1;
    private final T2 _2;

    public Tuple2(T1 _1, T2 _2) {
        this._1 = _1;
        this._2 = _2;
    }

    public static <T1, T2> Tuple2<T1, T2> of(T1 _1, T2 _2) {
        return new Tuple2<>(_1, _2);
    }

    public T1 _1() {
        return _1;
    }

    public T2 _2() {
        return _2;
    }
}
