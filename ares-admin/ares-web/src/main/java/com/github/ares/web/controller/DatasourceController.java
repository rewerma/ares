package com.github.ares.web.controller;

import com.github.ares.web.dto.Pager;
import com.github.ares.web.dto.DatasourceDto;
import com.github.ares.web.service.DatasourceService;
import com.github.ares.web.utils.Result;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/datasource")
@CrossOrigin
public class DatasourceController {
    @Autowired
    private DatasourceService datasourceService;

    @PostMapping()
    public Result<String> create(@RequestBody DatasourceDto datasourceDto) {
        String code = datasourceService.save(datasourceDto);
        return Result.success(code);
    }

    @PatchMapping("/{code}")
    public Result<String> update(@PathVariable(value = "code") String code, @RequestBody DatasourceDto datasourceDto) {
        datasourceDto.setCode(code);
        datasourceService.update(datasourceDto);
        return Result.success(code);
    }

    @DeleteMapping("/{code}")
    public Result<String> delete(@PathVariable(value = "code") String code) {
        datasourceService.delete(code);
        return Result.success(code);
    }

    @GetMapping("/{code}")
    public Result<DatasourceDto> detail(@PathVariable(value = "code") String code) {
        DatasourceDto datasourceDto = datasourceService.detail(code);
        return Result.success(datasourceDto);
    }

    @GetMapping("/list")
    public Result<Pager<DatasourceDto>> list(DatasourceDto params, Pager<DatasourceDto> pager) {
        Pager<DatasourceDto> result = datasourceService.listPage(params, pager);
        return Result.success(result);
    }
}
