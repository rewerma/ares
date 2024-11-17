package com.github.ares.web.utils;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.stereotype.Component;

@Component
public class SpringContext implements ApplicationContextAware {

    private static ApplicationContext context;

    public void setApplicationContext(final ApplicationContext context) throws BeansException {
        SpringContext.context = context;
    }

    @SuppressWarnings("unchecked")
    public static <T> T getBean(final String beanName) {
        return (T) SpringContext.context.getBean(beanName);
    }

    public static <T> T getBean(final Class<T> clazz) {
        return context.getBean(clazz);
    }

    public void close() {
        ((AbstractApplicationContext) context).close();
    }
}
