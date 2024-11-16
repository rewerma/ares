package com.github.ares.web.dto;

import lombok.Data;

@Data
public class TaskDefinitionDto {
    private String code;

    private String name;

    private String dsCode;

    private String envParams;

    private String taskContent;

    private String inParams;

    private String outParams;
}
