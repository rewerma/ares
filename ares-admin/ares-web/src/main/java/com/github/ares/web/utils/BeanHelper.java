package com.github.ares.web.utils;

import org.springframework.cglib.beans.BeanCopier;

public class BeanHelper {
    public static <T> T convert(Object source, Class<T> targetClass) {
        try {
            T target = targetClass.newInstance();
            BeanCopier beanCopier = BeanCopier.create(source.getClass(), targetClass, false);
            beanCopier.copy(source, target, null);
            return target;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
