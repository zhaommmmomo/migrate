package com.beust.jcommander.internal;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.PARAMETER;

@Retention(java.lang.annotation.RetentionPolicy.RUNTIME)
@Target({FIELD, PARAMETER})
public @interface Nullable {
}
