package com.github.ares.engine.core;

import com.github.ares.common.exceptions.AresException;
import com.github.ares.sql.function.DynamicFunction;
import com.github.ares.sql.function.UdfInterface;
import lombok.Getter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

public abstract class UdfManager extends AbstractBaseExecutor implements Serializable {
    private static final long serialVersionUID = 1L;

    @Getter
    public final Map<String, DynamicFunction> dynamicFunctions = new ConcurrentHashMap<>();

    public void init(ExecutorManager executorManager) {
        super.init(executorManager);
        List<UdfInterface> udfList = new ArrayList<>();
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        ServiceLoader.load(UdfInterface.class, classLoader).forEach(udfList::add);
        registerUdf(udfList);
    }

    public void registerUdf(List<UdfInterface> udfList) {
        for (UdfInterface udfInterface : udfList) {
            DynamicFunction dynamicFunction = new DynamicFunction();
            dynamicFunction.fromUdfInterface(udfInterface);
            registerDynamicFunction(dynamicFunction);
        }

        for (DynamicFunction dynamicFunction : dynamicFunctions.values()) {
            registerUdf(dynamicFunction);
        }
    }

    public void registerDynamicFunction(DynamicFunction dynamicFunction) {
        if (dynamicFunctions.containsKey(dynamicFunction.getFunctionName())) {
            throw new AresException("Procedure function " + dynamicFunction.getFunctionName() + " already registered");
        }
        dynamicFunctions.put(dynamicFunction.getFunctionName().toLowerCase(), dynamicFunction);
    }

    public abstract void registerUdf(DynamicFunction dynamicFunction);
}
