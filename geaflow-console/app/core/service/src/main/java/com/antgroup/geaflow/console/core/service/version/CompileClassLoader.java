package com.antgroup.geaflow.console.core.service.version;

public interface CompileClassLoader {

    <T> T newInstance(Class<T> clazz, Object... parameters);

}
