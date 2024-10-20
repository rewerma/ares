package com.github.ares.api.table.factory;

import com.github.ares.api.table.catalog.Catalog;
import com.github.ares.common.configuration.ReadonlyConfig;

public interface CatalogFactory extends Factory {

    /** Creates a {@link Catalog} using the options. */
    Catalog createCatalog(String catalogName, ReadonlyConfig options);
}
