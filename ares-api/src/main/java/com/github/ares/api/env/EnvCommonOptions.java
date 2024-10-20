package com.github.ares.api.env;

import com.github.ares.common.configuration.Option;
import com.github.ares.common.configuration.Options;
import com.github.ares.common.utils.JobMode;

import java.util.Map;

public interface EnvCommonOptions {
    Option<Integer> PARALLELISM =
            Options.key("parallelism")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "When parallelism is not specified in connector, the parallelism in env is used by default. "
                                    + "When parallelism is specified, it will override the parallelism in env.");

    Option<String> JOB_NAME =
            Options.key("job.name")
                    .stringType()
                    .defaultValue("Ares_Job")
                    .withDescription("The job name of this job");

    Option<JobMode> JOB_MODE =
            Options.key("job.mode")
                    .enumType(JobMode.class)
                    .defaultValue(JobMode.BATCH)
                    .withDescription("The job mode of this job, support Batch and Stream");

    Option<Long> CHECKPOINT_INTERVAL =
            Options.key("checkpoint.interval")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "The interval (in milliseconds) between two consecutive checkpoints.");

    Option<Integer> READ_LIMIT_ROW_PER_SECOND =
            Options.key("read_limit.rows_per_second")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The each parallelism row limit per second for read data from source.");

    Option<Integer> READ_LIMIT_BYTES_PER_SECOND =
            Options.key("read_limit.bytes_per_second")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The each parallelism bytes limit per second for read data from source.");
    Option<Long> CHECKPOINT_TIMEOUT =
            Options.key("checkpoint.timeout")
                    .longType()
                    .noDefaultValue()
                    .withDescription("The timeout (in milliseconds) for a checkpoint.");

    Option<String> JARS =
            Options.key("jars")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("third-party packages can be loaded via `jars`");

    Option<Map<String, String>> CUSTOM_PARAMETERS =
            Options.key("custom_parameters")
                    .mapType()
                    .noDefaultValue()
                    .withDescription("custom parameters for run engine");
}
