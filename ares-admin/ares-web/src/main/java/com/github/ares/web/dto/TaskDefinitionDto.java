package com.github.ares.web.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;

import java.time.LocalDateTime;

@Data
public class TaskDefinitionDto {
    private String code;

    private String name;

    private String dsCode;

    private String envParams;

    private String taskContent;

    private String inParams;

    private String outParams;

    @JsonFormat( pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime createTime;
}
