package com.github.lechandonga.dagworkfloworchestratorjava.tests.support;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Prints the covered business group and every case result, so {@code mvn test}
 * logs show which business scenarios are covered and how each case performed.
 */
public class BusinessLogging implements BeforeAllCallback, BeforeEachCallback,
        AfterTestExecutionCallback, AfterAllCallback {

    private static final Logger log = LoggerFactory.getLogger("BUSINESS-COVERAGE");

    @Override
    public void beforeAll(ExtensionContext context) {
        log.info("========== 业务测试分组开始: {} ==========", context.getDisplayName());
    }

    @Override
    public void beforeEach(ExtensionContext context) {
        log.info(">>> [{}] 用例开始: {}",
                context.getParent().map(ExtensionContext::getDisplayName).orElse(""),
                context.getDisplayName());
    }

    @Override
    public void afterTestExecution(ExtensionContext context) {
        boolean failed = context.getExecutionException().isPresent();
        log.info("<<< [{}] 用例{}: {}",
                context.getParent().map(ExtensionContext::getDisplayName).orElse(""),
                failed ? "失败" : "通过",
                context.getDisplayName());
    }

    @Override
    public void afterAll(ExtensionContext context) {
        log.info("========== 业务测试分组结束: {} ==========", context.getDisplayName());
    }
}
