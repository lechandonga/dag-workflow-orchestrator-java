package com.github.lechandonga.dagworkfloworchestratorjava.tests;

import com.github.lechandonga.dagworkfloworchestratorjava.tests.support.BusinessLogging;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * 业务：Spring 容器可加载，引擎与控制器 Bean 装配正确。
 */
@ExtendWith(BusinessLogging.class)
@SpringBootTest
@DisplayName("应用装配")
class ApplicationTests {

    @Test
    @DisplayName("Spring 上下文启动成功")
    void contextLoads() {
    }
}
