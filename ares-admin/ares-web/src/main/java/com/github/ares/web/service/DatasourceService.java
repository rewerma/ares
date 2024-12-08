package com.github.ares.web.service;

import com.github.ares.web.dto.DatasourceDto;
import com.github.ares.web.dto.Pager;
import com.github.ares.web.entity.BaseModel;
import com.github.ares.web.entity.Datasource;
import com.github.ares.web.utils.BeanHelper;
import com.github.ares.web.utils.CodeGenerator;
import com.github.ares.web.utils.ServiceException;
import io.ebean.Query;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
public class DatasourceService {
    public String save(DatasourceDto datasourceDto) {
        Datasource datasource = BeanHelper.convert(datasourceDto, Datasource.class);

        int count = datasource.finder().query()
                .select("id").where()
                .eq("name", datasource.getName()).findCount();
        if (count > 0) {
            throw new ServiceException("the name of datasource already exists");
        }
        datasource.setCode(CodeGenerator.generateCode());
        datasource.save();
        return datasource.getCode();
    }

    public void update(DatasourceDto datasourceDto) {
        Datasource datasource = BeanHelper.convert(datasourceDto, Datasource.class);

        Datasource oldTaskDefinition = datasource.finder().query()
                .select("id").where()
                .eq("code", datasource.getCode()).findOne();
        if (oldTaskDefinition == null) {
            throw new ServiceException("datasource not found");
        }
        datasource.setId(oldTaskDefinition.getId());
        datasource.update("name", "params");
    }

    public void delete(String code) {
        Datasource datasource = BaseModel.finder(Datasource.class).query()
                .select("id").where()
                .eq("code", code).findOne();
        if (datasource == null) {
            throw new ServiceException("datasource not found");
        }
        datasource.delete();
    }

    public DatasourceDto detail(String code) {
        Datasource datasource = BaseModel.finder(Datasource.class).query()
                .where()
                .eq("code", code).findOne();
        if (datasource == null) {
            throw new ServiceException("datasource not found");
        }
        return BeanHelper.convert(datasource, DatasourceDto.class);
    }

    public Pager<DatasourceDto> listPage(DatasourceDto params, Pager<DatasourceDto> pager) {
        Query<Datasource> query = BaseModel.query(Datasource.class);
        if (params.getName() != null) {
            query.where().ilike("name", "%" + params.getName() + "%");
        }
        Query<Datasource> queryCount = query.copy();
        List<Datasource> list = query.select("id, code, name, dsType, createTime")
                .orderBy().desc("id").setFirstRow(pager.getOffset()).setMaxRows(pager.getSize()).findList();

        int count = queryCount.select("id").findCount();

        pager.setCount(count);
        List<DatasourceDto> listDto = list.stream().map(datasource ->
                        BeanHelper.convert(datasource, DatasourceDto.class))
                .collect(Collectors.toList());
        pager.setItems(listDto);

        return pager;
    }
}
