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
@Table(name = "t_datasource")
public class Datasource  extends BaseModel<Datasource> {

    public Datasource() {
        super(Datasource.class);
    }

    @Id
    private Long id;

    private String code;

    @Column(name = "ds_type")
    private String dsType;

    private String name;

    private String params;

    @WhenCreated
    @Column(name = "c_time")
    private LocalDateTime createTime;
}
