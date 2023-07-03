package com.antgroup.geaflow.console.common.util.type;

public enum CatalogType {
    MEMORY("memory"),
    CONSOLE("console");

    private String value;

    CatalogType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
