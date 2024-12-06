package com.github.ares.web.controller;

import com.github.ares.web.dto.TaskExecutionDto;
import com.github.ares.web.service.TaskExecutionService;
import com.github.ares.web.utils.Result;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

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
    public Result<Long> stopTask(@PathVariable("instanceId") Long instanceId,
                                 @RequestParam(value = "from-rpc", required = false) Boolean fromRpc) {
        taskExecutionService.stop(instanceId, fromRpc);
        return Result.success();
    }

    @GetMapping("/{instanceId}/log")
    public Result<String> taskLog(@PathVariable("instanceId") Long instanceId,
                                  @RequestParam(value = "from-rpc", required = false) Boolean fromRpc) {
        return Result.success(taskExecutionService.getFullLog(instanceId, fromRpc));
    }
}
