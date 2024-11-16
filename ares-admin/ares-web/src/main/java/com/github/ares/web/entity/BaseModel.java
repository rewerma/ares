package com.github.ares.web.entity;


import io.ebean.DB;
import io.ebean.Database;
import io.ebean.Finder;
import io.ebean.Model;
import io.ebean.Query;
import io.ebean.UpdateQuery;
import org.springframework.cglib.beans.BeanMap;

import javax.persistence.MappedSuperclass;
import javax.persistence.Transient;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@MappedSuperclass
public abstract class BaseModel<T> extends Model {
    private static final Map<Class<?>, Finder<?, ?>> FINDER_MAP = new ConcurrentHashMap<>();

    @Transient
    private Class<T> clazz;

    public BaseModel(Class<T> clazz) {
        this.clazz = clazz;
    }

    @SuppressWarnings("unchecked")
    public static <T> Finder<Long, T> finder(Class<T> clazz) {
        return (Finder<Long, T>) FINDER_MAP.computeIfAbsent(clazz, k -> new Finder<>(clazz));
    }

    public static <T> Query<T> query(Class<T> clazz) {
        return finder(clazz).query().setDisableLazyLoading(true);
    }

    public Finder<Long, T> finder() {
        return finder(clazz);
    }

    public Query<T> query() {
        return finder().query().setDisableLazyLoading(true);
    }

    public void init() {
    }

    @Override
    public void save() {
        init();
        super.save();
    }

    @Override
    public void insert() {
        init();
        super.insert();
    }

    public void saveOrUpdate() {
        Database database = DB.getDefault();
        Object id = database.beanId(this);
        if (id == null) {
            init();
            this.save();
        } else {
            this.update();
        }
    }

    public void merge() {
        Database database = DB.getDefault();
        database.merge(this);
    }

    public int update(String... fieldNames) {
        Database database = DB.getDefault();
        Object id = database.beanId(this);
        if (id == null) {
            return 0;
        }
        UpdateQuery<T> updateQuery = DB.update(clazz);
        BeanMap beanMap = BeanMap.create(this);
        for (String fieldName : fieldNames) {
            boolean isNullable = false;
            if (fieldName.startsWith("nullable:")) {
                fieldName = fieldName.substring("nullable:".length()).trim();
                isNullable = true;
            }
            Object val = beanMap.get(fieldName);
            if (val != null || isNullable) {
                updateQuery.set(fieldName, val);
            }
        }
        updateQuery.where().idEq(id);
        return updateQuery.update();
    }
}
