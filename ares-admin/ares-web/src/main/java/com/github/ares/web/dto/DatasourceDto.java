package com.github.ares.web.dto;

import lombok.Data;

@Data
public class DatasourceDto {
    private Long id;

    private String code;

    private String name;

    private String params;
}
