package com.github.ares.parser.test;

import com.github.ares.com.google.inject.Guice;
import com.github.ares.com.google.inject.Injector;
import com.github.ares.common.utils.InjectorFactory;
import com.github.ares.parser.PlParser;
import com.github.ares.parser.config.ParserServiceModule;
import com.github.ares.parser.plan.LogicalProject;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

public class PlParserTest {
    @Test
    public void test00() throws IOException {
        Injector injector = Guice.createInjector(new ParserServiceModule());
        InjectorFactory.init(injector);
        PlParser plTransformation = injector.getInstance(PlParser.class);
        plTransformation.init();
        InputStream in = this.getClass().getClassLoader().getResourceAsStream("mysql.sql");
        LogicalProject baseBody = plTransformation.parseToBaseBody(in);
        Assert.assertFalse(baseBody.getLogicalOperations().isEmpty());
        in.close();
    }
}
