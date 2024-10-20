/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.ares.core.starter.command;

import com.beust.jcommander.Parameter;
import com.github.ares.api.common.EngineTypeVersion;

import java.util.List;

public abstract class CommandArgs {

    /** Help parameter */
    @Parameter(
            names = {"-h", "--help"},
            help = true,
            description = "Show the usage message")
    protected boolean help = false;

    /** Undefined parameters parsed will be stored here as engine original command parameters. */
    protected List<String> originalParameters;

    public abstract Command<?> buildCommand(EngineTypeVersion engineTypeVersion);

    public boolean isHelp() {
        return help;
    }

    public void setHelp(boolean help) {
        this.help = help;
    }

    public List<String> getOriginalParameters() {
        return originalParameters;
    }

    public void setOriginalParameters(List<String> originalParameters) {
        this.originalParameters = originalParameters;
    }
}
