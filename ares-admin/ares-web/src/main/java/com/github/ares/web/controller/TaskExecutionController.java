package com.github.ares.web.controller;

import com.github.ares.web.dto.TaskExecutionDto;
import com.github.ares.web.service.TaskExecutionService;
import com.github.ares.web.utils.Result;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/task/execution")
public class TaskExecutionController {
    @Autowired
    private TaskExecutionService taskExecutionService;

    @PostMapping("/start")
    public Result<Long> startTask(@RequestBody(required = false) TaskExecutionDto taskExecutionDto) {
        Long instanceId = taskExecutionService.start(taskExecutionDto);

        return Result.success(instanceId);
    }

    @PostMapping("/{code}/start")
    public Result<Long> startTask(@PathVariable("code") String code,
                                  @RequestBody(required = false) TaskExecutionDto taskExecutionDto) {
        Long instanceId = taskExecutionService.start(code, taskExecutionDto);

        return Result.success(instanceId);
    }

    @PostMapping("/{instanceId}/stop")
    public Result<Long> stopTask(@PathVariable("instanceId") Long instanceId) {
        taskExecutionService.stop(instanceId);
        return Result.success();
    }

    @GetMapping("/{instanceId}/log")
    public Result<String> taskLog(@PathVariable("instanceId") Long instanceId) {
        return Result.success(taskExecutionService.getFullLog(instanceId));
    }
}
