package com.github.ares.web.dto;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Pager<T> implements Serializable {

    private static final long serialVersionUID = -1L;

    private Integer count = 0;
    private List<T> items = new ArrayList<>();
    private Integer page = 1;
    private Integer size = 20;
    private Integer offset = 0;

    public Pager() {

    }

    public Pager(Integer page, Integer size) {
        this.page = page;
        this.size = size;
    }

    public Pager(Integer count, List<T> items) {
        this.count = count;
        this.items = items;
    }

    public String toString() {
        return "PageResult[count=" + this.count + ", items=" + this.items + "]";
    }

    public Integer getCount() {
        return this.count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    public List<T> getItems() {
        return items;
    }

    public void setItems(List<T> items) {
        this.items = items;
    }

    public Integer getPage() {
        if (page == null) {
            page = 1;
        }
        return page;
    }

    public void setPage(Integer page) {
        this.page = page;
    }

    public Integer getSize() {
        if (size == null) {
            size = 20;
        }
        return size;
    }

    public void setSize(Integer size) {
        this.size = size;
    }

    public Integer getOffset() {
        offset = (Integer) (getPage() - 1) * (Integer) getSize();
        return offset;
    }

    public void setOffset(Integer offset) {
        this.offset = offset;
    }
}
