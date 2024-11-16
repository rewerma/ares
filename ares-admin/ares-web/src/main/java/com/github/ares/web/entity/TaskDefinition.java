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
@Table(name = "t_task_definition")
public class TaskDefinition extends BaseModel<TaskDefinition> {

    public TaskDefinition() {
        super(TaskDefinition.class);
    }

    @Id
    private Long id;

    private String code;

    private String name;

    @Column(name = "ds_code")
    private String dsCode;

    @Column(name = "env_params")
    private String envParams;

    @Column(name = "task_content")
    private String taskContent;

    @Column(name = "in_params")
    private String inParams;

    @Column(name = "out_params")
    private String outParams;

    @WhenCreated
    @Column(name = "c_time")
    private LocalDateTime cTime;

}
