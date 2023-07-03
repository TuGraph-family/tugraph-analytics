package com.antgroup.geaflow.console.test;

import com.antgroup.geaflow.console.common.service.integration.engine.CompileContext;
import com.antgroup.geaflow.console.common.service.integration.engine.CompileResult;
import com.antgroup.geaflow.console.common.service.integration.engine.GeaflowCompiler;
import com.antgroup.geaflow.console.common.util.FileUtil;
import com.antgroup.geaflow.console.common.util.LoaderSwitchUtil;
import com.antgroup.geaflow.console.common.util.exception.GeaflowException;
import com.antgroup.geaflow.console.common.util.proxy.ProxyUtil;
import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
public class GeaflowCompilerTest {

    private final static String ENGINE_JAR_PATH;

    static {
        ENGINE_JAR_PATH = "/tmp/geaflow.jar";
    }

    private static String SCRIPT;

    @BeforeClass
    public static void beforeClass() throws Exception {
        log.info(ENGINE_JAR_PATH);
        if (!FileUtil.exist(ENGINE_JAR_PATH)) {
            throw new GeaflowException("Prepare engine jar at path {}", ENGINE_JAR_PATH);
        }
        SCRIPT = IOUtils.resourceToString("/compile.sql", StandardCharsets.UTF_8);
    }

    private URLClassLoader createClassLoader() throws Exception {
        List<URL> urlList = new ArrayList<>();
        urlList.add(new File(ENGINE_JAR_PATH).toURI().toURL());
        ClassLoader extClassLoader = ClassLoader.getSystemClassLoader().getParent();
        return new URLClassLoader(urlList.toArray(new URL[]{}), extClassLoader);
    }

    @Test(enabled = false)
    public void testCompile() throws Exception {
        try (URLClassLoader classLoader = createClassLoader()) {
            LoaderSwitchUtil.run(classLoader, () -> {
                Object context = classLoader.loadClass("com.antgroup.geaflow.dsl.common.compile.CompileContext")
                    .newInstance();
                Object compiler = classLoader.loadClass("com.antgroup.geaflow.dsl.runtime.QueryClient").newInstance();

                Method method = compiler.getClass().getMethod("compile", String.class, context.getClass());
                Object result = method.invoke(compiler, SCRIPT, context);

                String physicPlan = result.getClass().getMethod("getPhysicPlan").invoke(result).toString();
                log.info(physicPlan);
            });
        }
    }

    @Test(enabled = false)
    public void testProxyCompile() throws Exception {
        try (URLClassLoader classLoader = createClassLoader()) {
            CompileContext context = ProxyUtil.newInstance(classLoader, CompileContext.class);
            GeaflowCompiler compiler = ProxyUtil.newInstance(classLoader, GeaflowCompiler.class);
            CompileResult result = compiler.compile(SCRIPT, context);
            log.info(result.getPhysicPlan().toString());
        }
    }

}
