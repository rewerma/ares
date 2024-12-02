package com.github.ares.web.config;

import com.github.ares.com.google.inject.Guice;
import com.github.ares.com.google.inject.Injector;
import com.github.ares.common.utils.InjectorFactory;
import com.github.ares.parser.PlParser;
import com.github.ares.parser.config.ParserServiceModule;
import com.github.ares.parser.datasource.PropertiesDataSourceComplement;
import com.github.ares.parser.datasource.SourceConfigComplementFactory;
import com.github.ares.parser.utils.Constants;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

@Configuration
public class PLParserConfig {
    @Bean
    public PlParser plParser() {
        Injector injector = Guice.createInjector(new ParserServiceModule());
        InjectorFactory.init(injector);
        PlParser plParser = injector.getInstance(PlParser.class);
        plParser.init();
        SourceConfigComplementFactory.register(Constants.DEFAULT_DATASOURCE_COMPLEMENT,
                new PropertiesDataSourceComplement(new Properties()));
        return plParser;
    }
}
