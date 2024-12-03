package com.github.ares.web.controller;

import com.github.ares.web.dto.Pager;
import com.github.ares.web.dto.TaskInstanceDto;
import com.github.ares.web.service.TaskInstanceService;
import com.github.ares.web.utils.Result;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/task/instance")
public class TaskInstanceController {
    @Autowired
    private TaskInstanceService taskInstanceService;

    @GetMapping("/{id}")
    public Result<TaskInstanceDto> detail(@PathVariable(value = "id") Long id) {
        TaskInstanceDto taskInstanceDto = taskInstanceService.detail(id);
        return Result.success(taskInstanceDto);
    }

    @GetMapping("/list")
    public Result<Pager<TaskInstanceDto>> list(TaskInstanceDto params, Pager<TaskInstanceDto> pager) {
        Pager<TaskInstanceDto> result = taskInstanceService.listPage(params, pager);
        return Result.success(result);
    }
}
