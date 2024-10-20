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
import com.github.ares.common.configuration.DeployMode;
import com.github.ares.common.utils.Constants;

import java.util.Collections;
import java.util.List;

public abstract class AbstractCommandArgs extends CommandArgs {

    /** sql file path */
    @Parameter(
            names = {"-s", "--sql"},
            description = "SQL script file")
    protected String sqlFile;

    @Parameter(
            names = {"-c", "--conf"},
            splitter = ParameterSplitter.class,
            description = "Command conf")
    protected List<String> confList = Collections.emptyList();

    /** user-defined parameters */
    @Parameter(
            names = {"-i", "--variable"},
            splitter = ParameterSplitter.class,
            description = "Variable substitution, such as -i city=beijing, or -i date=20190318")
    protected List<String> variables = Collections.emptyList();

    /** check config flag */
    @Parameter(
            names = {"--check"},
            description = "Whether check config")
    protected boolean checkConfig = false;

    /** Ares job name */
    @Parameter(
            names = {"-n", "--name"},
            description = "Ares job name")
    protected String jobName = Constants.LOGO;

    @Parameter(
            names = {"--encrypt"},
            description =
                    "Encrypt config file, when both --decrypt and --encrypt are specified, only --encrypt will take effect")
    protected boolean encrypt = false;

    @Parameter(
            names = {"--decrypt"},
            description =
                    "Decrypt config file, When both --decrypt and --encrypt are specified, only --encrypt will take effect")
    protected boolean decrypt = false;

    public abstract DeployMode getDeployMode();

    public String getSqlFile() {
        return sqlFile;
    }

    public void setSqlFile(String sqlFile) {
        this.sqlFile = sqlFile;
    }
    public List<String> getVariables() {
        return variables;
    }

    public void setVariables(List<String> variables) {
        this.variables = variables;
    }

    public boolean isCheckConfig() {
        return checkConfig;
    }

    public void setCheckConfig(boolean checkConfig) {
        this.checkConfig = checkConfig;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public boolean isEncrypt() {
        return encrypt;
    }

    public void setEncrypt(boolean encrypt) {
        this.encrypt = encrypt;
    }

    public boolean isDecrypt() {
        return decrypt;
    }

    public void setDecrypt(boolean decrypt) {
        this.decrypt = decrypt;
    }

    public List<String> getConfList() {
        return confList;
    }

    public void setConfList(List<String> confList) {
        this.confList = confList;
    }
}
