package com.github.ares.common.configuration.utils;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Documented
@Target(ElementType.FIELD)
public @interface OptionMark {

    /**
     * The key of the option, if not configured, we will default convert `lowerCamelCase` to
     * `under_score_case` and provide it to users
     */
    String name() default "";

    /** The description of the option */
    String description() default "";
}
