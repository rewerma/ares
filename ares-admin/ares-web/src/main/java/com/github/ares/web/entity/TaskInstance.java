package com.github.ares.web.entity;

import io.ebean.annotation.WhenCreated;
import lombok.Getter;
import lombok.Setter;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.time.LocalDateTime;

@Getter
@Setter
@Entity
@Table(name = "t_task_instance")
public class TaskInstance extends BaseModel<TaskInstance> {

    public TaskInstance() {
        super(TaskInstance.class);
    }

    @Id
    private Long id;

    @Column(name = "batch_code")
    private String batchCode;

    @Column(name = "task_code")
    private String taskCode;

    @Column(name = "task_name")
    private String taskName;

    @Column(name = "start_time")
    private LocalDateTime startTime;

    @Column(name = "end_time")
    private LocalDateTime endTime;

    private Integer status;

    @Column(name = "executor_host")
    private String executorHost;

    @Column(name = "log_path")
    private String logPath;

    @Column(name = "in_params")
    private String inParams;

    @Column(name = "out_params")
    private String outParams;

    @Column(name = "exe_result")
    private String exeResult;

    @WhenCreated
    @Column(name = "c_time")
    private LocalDateTime createTime;
}
