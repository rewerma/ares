package com.github.ares.worker.shell.util;

import com.github.ares.common.enums.StatusType;
import com.github.ares.worker.model.TaskRequest;
import com.github.ares.worker.model.TaskResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

@Slf4j
public class ShellCommandExecutor {

    private static final ExecutorService PROCESS_TASK_LOG_COLLECT_THREAD_POOL = Executors.newFixedThreadPool(100);

    protected Process process;

    protected TaskRequest taskRequest;

    private volatile boolean isKilled = false;

    public ShellCommandExecutor(TaskRequest taskRequest) {
        this.taskRequest = taskRequest;
    }

    private void buildProcess(String commandFile) throws IOException, InterruptedException {
        // setting up user to run commands
        List<String> command = new LinkedList<>();

        // init process builder
        ProcessBuilder processBuilder = new ProcessBuilder();
        // setting up a working directory
        processBuilder.directory(new File(taskRequest.getExecutePath()));
        // merge error information to standard output stream
        processBuilder.redirectErrorStream(true);

        if (taskRequest.getEnvironments() != null) {
            processBuilder.environment().putAll(taskRequest.getEnvironments());
        }


        command.add(commandInterpreter());
        command.addAll(Collections.emptyList());
        command.add(commandFile);

        // setting commands
        processBuilder.command(command);
        process = processBuilder.start();

        printCommand(command);
    }

    public TaskResponse run(String execCommand) throws IOException, InterruptedException {
        TaskResponse response = new TaskResponse();
        if (StringUtils.isEmpty(execCommand)) {
            return response;
        }

        String commandFilePath = buildCommandFilePath();

        // create command file if not exists
        createCommandFileIfNotExists(execCommand, commandFilePath);

        // build process
        buildProcess(commandFilePath);

        ProcessLogCollectTask logCollectTask = new ProcessLogCollectTask(
                taskRequest.getLogPath(),
                taskRequest.getTaskInstanceId(),
                process);
        PROCESS_TASK_LOG_COLLECT_THREAD_POOL.execute(logCollectTask::start);

        int processId = getProcessId(process);

        response.setProcessId(processId);

        // cache processId
        taskRequest.setProcessId(processId);

        // print process id
        log.info("Shell process: {} start", processId);

        // if timeout occurs, exit directly
        // waiting for the run to finish
        int exitCode = process.waitFor();
        log.info("Process: {} is finished exit: {}, wait ProcessTaskLogCollectTask finish", processId, process.exitValue());

        logCollectTask.waitFinish();
        log.info("ProcessTaskLogCollectTask: {} is finished", processId);

        response.setOutputParams(logCollectTask.getTaskOutputParams());
        response.setLastResult(logCollectTask.getTaskLastResult());
        if (logCollectTask.getErrorMessage() != null) {
            response.setErrorMessage(logCollectTask.getErrorMessage());
            response.setStatus(StatusType.FAILED);
        } else if (exitCode != 0) {
            response.setStatus(StatusType.FAILED);
        }

        response.setExitStatusCode(process.exitValue());
        log.info("Shell process: {} has finished, exist with : {}", processId, process.exitValue());
        return response;

    }

    public void cancelApplication() throws IOException {
        if (process == null || !process.isAlive()) {
            log.info("The shell process is not alive, no need to cancel");
            return;
        }

        killProcess(taskRequest);

        log.info("Kill shell process: {} successfully", taskRequest.getProcessId());
    }

    private void printCommand(List<String> commands) {
        log.info("Task run command: {}", String.join(" ", commands));
    }

    /**
     * get process id
     *
     * @param process process
     * @return process id
     */
    private int getProcessId(Process process) {
        int processId = 0;

        try {
            Field f = process.getClass().getDeclaredField("pid");
            f.setAccessible(true);

            processId = f.getInt(process);
        } catch (Throwable e) {
            log.error("Get processId failed", e);
        }

        return processId;
    }

    protected String buildCommandFilePath() {
        return String.format("%s/%s.%s", taskRequest.getExecutePath(), taskRequest.getTaskInstanceId(), "command");
    }

    protected void createCommandFileIfNotExists(String execCommand, String commandFile) throws IOException {
        log.info("Begin to create command file: {}", commandFile);

        Path commandFilePath = Paths.get(commandFile);
        if (Files.exists(commandFilePath)) {
            log.warn("The command file: {} is already exist, will not create a again", commandFile);
            return;
        }

        StringBuilder sb = new StringBuilder();

        sb.append("#!/bin/sh\n");
        sb.append(execCommand);
        String commandContent = sb.toString();

        Files.createFile(commandFilePath);
        Files.setPosixFilePermissions(commandFilePath, PosixFilePermissions.fromString("rwxr-xr-x"));
        Files.write(commandFilePath, commandContent.getBytes(), StandardOpenOption.APPEND);

        log.info("Success create command file:\n {}", commandContent);
    }

    protected String commandInterpreter() {
        return "sh";
    }

    public static Set<Integer> getAllRelatedPid(int pid) throws InterruptedException, IOException {
        Set<Integer> childPidList = new HashSet<>();
        childPidList.add(pid);

        Process process = new ProcessBuilder("pgrep", "-P", String.valueOf(pid)).start();

        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));

        String childPid;
        while ((childPid = reader.readLine()) != null) {
            int cpid = Integer.parseInt(childPid);
            childPidList.add(cpid);
            childPidList.addAll(getAllRelatedPid(cpid));
        }

        process.waitFor();
        return childPidList;
    }

    public void killProcess(TaskRequest taskRequest) throws IOException {
        int processId = taskRequest.getProcessId();
        if (processId <= 0) {
            log.info("The processId: {} is not a active process, no need to kill", processId);
            return;
        }
        String relatedProcessIds = String.valueOf(processId);

        // whether kill sub process create by current shell process
        try {
            Set<Integer> relatedPids = getAllRelatedPid(processId);
            // desc sort by pid to follow the process tree
            relatedProcessIds = relatedPids.stream().sorted((o1, o2) -> o2 - o1).map(String::valueOf)
                    .collect(Collectors.joining(" "));
        } catch (Exception e) {
            log.error("Get all related processId failed, please make sure `pgrep` command valid in your worker node." +
                    "Try to kill parent processId alone.", e);
            Thread.currentThread().interrupt();
        }

        String cmd = String.format("kill -9 %s", relatedProcessIds);
        log.info("Begin to kill process : {}", cmd);

        StringTokenizer st = new StringTokenizer(cmd);
        String[] cmdArray = new String[st.countTokens()];
        for (int i = 0; st.hasMoreTokens(); i++) {
            cmdArray[i] = st.nextToken();
        }
        String killOutput = ShellExecutor.execCommand(cmdArray);
        isKilled = true;
        log.info("kill process : {}, output : {}", relatedProcessIds, killOutput);
    }

    public boolean isKilled() {
        return isKilled;
    }
}
