package com.github.ares.web.controller;

import com.github.ares.web.dto.Pager;
import com.github.ares.web.dto.TaskDefinitionDto;
import com.github.ares.web.entity.TaskDefinition;
import com.github.ares.web.service.TaskDefinitionService;
import com.github.ares.web.utils.Result;
import io.ebean.DB;
import io.ebean.Database;
import io.ebean.SqlQuery;
import io.ebean.SqlRow;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cglib.beans.BeanCopier;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/task/definition")
@CrossOrigin
public class TaskDefinitionController {
    @Autowired
    private TaskDefinitionService taskDefinitionService;

    @PostMapping()
    public Result<String> create(@RequestBody TaskDefinitionDto taskDefinitionDto) {
        String code = taskDefinitionService.save(taskDefinitionDto);
        return Result.success(code);
    }

    @PatchMapping("/{code}")
    public Result<String> update(@PathVariable(value = "code") String code, @RequestBody TaskDefinitionDto taskDefinitionDto) {
        taskDefinitionDto.setCode(code);
        taskDefinitionService.update(taskDefinitionDto);
        return Result.success(code);
    }

    @DeleteMapping("/{code}")
    public Result<String> delete(@PathVariable(value = "code") String code) {
        taskDefinitionService.delete(code);
        return Result.success(code);
    }

    @GetMapping("/{code}")
    public Result<TaskDefinitionDto> detail(@PathVariable(value = "code") String code) {
        TaskDefinitionDto taskDefinitionDto = taskDefinitionService.detail(code);
        return Result.success(taskDefinitionDto);
    }

    @GetMapping("/list")
    public Result<Pager<TaskDefinitionDto>> list(TaskDefinitionDto params, Pager<TaskDefinitionDto> pager) {
        Pager<TaskDefinitionDto> result = taskDefinitionService.listPage(params, pager);
        return Result.success(result);
    }
}
