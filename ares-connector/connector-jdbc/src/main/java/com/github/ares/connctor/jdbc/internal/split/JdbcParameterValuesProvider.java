package com.github.ares.connctor.jdbc.internal.split;

import java.io.Serializable;

/**
 * This interface is used by the {@link
 * com.github.ares.connctor.jdbc.source.JdbcSource} to compute the list of parallel
 * query to run (i.e. splits). Each query will be parameterized using a row of the matrix provided
 * by each {@link JdbcParameterValuesProvider} implementation.
 */
public interface JdbcParameterValuesProvider {

    /** Returns the necessary parameters array to use for query in parallel a table. */
    Serializable[][] getParameterValues();
}
