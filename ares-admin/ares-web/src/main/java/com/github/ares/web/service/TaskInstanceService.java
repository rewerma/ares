package com.github.ares.web.service;

import com.github.ares.web.dto.Pager;
import com.github.ares.web.dto.TaskInstanceDto;
import com.github.ares.web.dto.TaskInstanceDto;
import com.github.ares.web.dto.TaskInstanceDto;
import com.github.ares.web.entity.BaseModel;
import com.github.ares.web.entity.TaskInstance;
import com.github.ares.web.entity.TaskInstance;
import com.github.ares.web.entity.TaskInstance;
import com.github.ares.web.utils.BeanHelper;
import com.github.ares.web.utils.ServiceException;
import io.ebean.Query;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
public class TaskInstanceService {

    public TaskInstanceDto detail(Long id) {
        TaskInstance taskInstance = BaseModel.finder(TaskInstance.class).query()
                .where()
                .eq("id", id).findOne();
        if (taskInstance == null) {
            throw new ServiceException("task instance not found");
        }
        return TaskInstanceDto.of(taskInstance);
    }

    public Pager<TaskInstanceDto> listPage(TaskInstanceDto params, Pager<TaskInstanceDto> pager) {
        Query<TaskInstance> query = BaseModel.query(TaskInstance.class);
//        if (params.getName() != null) {
//            query.where().ilike("name", "%" + params.getName() + "%");
//        }
//        if (params.getDsCode() != null) {
//            query.where().eq("dsCode", params.getDsCode());
//        }
//        if (params.getTaskContent() != null) {
//            query.where().ilike("taskContent", "%" + params.getTaskContent() + "%");
//        }
        Query<TaskInstance> queryCount = query.copy();
        List<TaskInstance> list = query.select("id, batchCode, taskCode, taskName, startTime, endTime, status, executorHost, createTime")
                .orderBy().desc("id").setFirstRow(pager.getOffset()).setMaxRows(pager.getSize()).findList();

        int count = queryCount.select("id").findCount();

        pager.setCount(count);
        List<TaskInstanceDto> listDto = list.stream().map(TaskInstanceDto::of)
                .collect(Collectors.toList());
        pager.setItems(listDto);

        return pager;
    }
}
