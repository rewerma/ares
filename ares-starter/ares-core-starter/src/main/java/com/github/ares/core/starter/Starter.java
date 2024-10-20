package com.github.ares.core.starter;

import java.util.List;

public interface Starter {
    /** Return the Ares job commandline start commands */
    List<String> buildCommands() throws Exception;
}
