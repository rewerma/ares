package com.github.ares.web.config;

import com.github.ares.web.utils.Result;
import com.github.ares.web.utils.ServiceException;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;


@Slf4j
@ControllerAdvice(annotations = ResponseBody.class)
public class CustomExceptionHandler {

    /**
     * 通用异常处理
     *
     * @param e 异常
     * @return
     */
    @ResponseBody
    @ResponseStatus(HttpStatus.OK)
    @ExceptionHandler(value = Exception.class)
    public Result<String> commonExceptionHandle(Exception e) {
        if (e instanceof ServiceException) {
            log.error(e.getMessage());
        } else {
            log.error(e.getMessage(), e);
        }
        return Result.error(e.getMessage());
    }
}
