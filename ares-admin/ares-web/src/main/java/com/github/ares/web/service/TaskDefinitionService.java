package com.github.ares.web.service;

import com.github.ares.web.dto.Pager;
import com.github.ares.web.dto.TaskDefinitionDto;
import com.github.ares.web.entity.BaseModel;
import com.github.ares.web.entity.TaskDefinition;
import com.github.ares.web.utils.BeanHelper;
import com.github.ares.web.utils.CodeGenerator;
import com.github.ares.web.utils.ServiceException;
import io.ebean.Query;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
public class TaskDefinitionService {
    public String save(TaskDefinitionDto taskDefinitionDto) {
        TaskDefinition taskDefinition = BeanHelper.convert(taskDefinitionDto, TaskDefinition.class);

        int count = taskDefinition.finder().query()
                .select("id").where()
                .eq("name", taskDefinition.getName()).findCount();
        if (count > 0) {
            throw new ServiceException("the name of task definition already exists");
        }
        taskDefinition.setCode(CodeGenerator.generateCode());
        taskDefinition.save();
        return taskDefinition.getCode();
    }

    public void update(TaskDefinitionDto taskDefinitionDto) {
        TaskDefinition taskDefinition = BeanHelper.convert(taskDefinitionDto, TaskDefinition.class);

        TaskDefinition oldTaskDefinition = taskDefinition.finder().query()
                .select("id").where()
                .eq("code", taskDefinition.getCode()).findOne();
        if (oldTaskDefinition == null) {
            throw new ServiceException("task definition not found");
        }
        taskDefinition.setId(oldTaskDefinition.getId());
        taskDefinition.update("name", "nullable:dsCode", "nullable:envParams", "nullable:taskContent",
                "nullable:inParams", "nullable:outParams", "nullable:dsCode");
    }

    public void delete(String code) {
        TaskDefinition taskDefinition = BaseModel.finder(TaskDefinition.class).query()
                .select("id").where()
                .eq("code", code).findOne();
        if (taskDefinition == null) {
            throw new ServiceException("task definition not found");
        }
        taskDefinition.delete();
    }

    public TaskDefinitionDto detail(String code) {
        TaskDefinition taskDefinition = BaseModel.finder(TaskDefinition.class).query()
                .where()
                .eq("code", code).findOne();
        if (taskDefinition == null) {
            throw new ServiceException("task definition not found");
        }
        return BeanHelper.convert(taskDefinition, TaskDefinitionDto.class);
    }

    public Pager<TaskDefinitionDto> listPage(TaskDefinitionDto params, Pager<TaskDefinitionDto> pager) {
        Query<TaskDefinition> query = BaseModel.query(TaskDefinition.class);
        if (params.getName() != null) {
            query.where().ilike("name", "%" + params.getName() + "%");
        }
        if (params.getDsCode() != null) {
            query.where().eq("dsCode", params.getDsCode());
        }
        if (params.getTaskContent() != null) {
            query.where().ilike("taskContent", "%" + params.getTaskContent() + "%");
        }
        Query<TaskDefinition> queryCount = query.copy();
        List<TaskDefinition> list = query.select("id, code,name, dsCode, envParams, inParams, outParams, cTime")
                .orderBy().desc("id").setFirstRow(pager.getOffset()).setMaxRows(pager.getSize()).findList();

        int count = queryCount.select("id").findCount();

        pager.setCount(count);
        List<TaskDefinitionDto> listDto = list.stream().map(taskDefinition ->
                        BeanHelper.convert(taskDefinition, TaskDefinitionDto.class))
                .collect(Collectors.toList());
        pager.setItems(listDto);

        return pager;
    }
}
