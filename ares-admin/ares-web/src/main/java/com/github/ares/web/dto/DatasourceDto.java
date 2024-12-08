package com.github.ares.web.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;

import java.time.LocalDateTime;

@Data
public class DatasourceDto {
    private Long id;

    private String code;

    private String dsType;

    private String name;

    private String params;

    @JsonFormat( pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime createTime;
}
