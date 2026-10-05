package com.github.lechandonga.dagworkfloworchestratorjava.tests.support;

import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.TestPlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Prints a business-oriented coverage summary during {@code mvn test}:
 * each test class is treated as a business module (its {@code @DisplayName}),
 * and each test method as a use case with its result.
 */
public class BusinessCoverageListener implements TestExecutionListener {

    private static final Logger log = LoggerFactory.getLogger("BUSINESS_COVERAGE");

    private final Map<String, BusinessStats> businesses = new LinkedHashMap<>();
    private final Map<String, String> testToBusiness = new LinkedHashMap<>();

    @Override
    public void testPlanExecutionStarted(TestPlan testPlan) {
        log.info("==================== 业务测试覆盖报告开始 ====================");
        for (TestIdentifier classId : testPlan.getRoots().stream()
                .flatMap(root -> testPlan.getDescendants(root).stream())
                .filter(TestIdentifier::isContainer)
                .filter(id -> id.getSource()
                        .map(source -> source instanceof org.junit.platform.engine.support.descriptor.ClassSource)
                        .orElse(false))
                .toList()) {
            String business = classId.getDisplayName();
            BusinessStats stats = new BusinessStats(business);
            businesses.put(classId.getUniqueId(), stats);
            for (TestIdentifier test : testPlan.getDescendants(classId)) {
                if (test.isTest()) {
                    testToBusiness.put(test.getUniqueId(), classId.getUniqueId());
                    stats.total++;
                }
            }
        }
        businesses.values().forEach(stats ->
                log.info("业务模块发现: {} (用例数 {})", stats.name, stats.total));
    }

    @Override
    public void executionStarted(TestIdentifier testIdentifier) {
        if (testIdentifier.isTest()) {
            String classId = testToBusiness.get(testIdentifier.getUniqueId());
            if (classId != null) {
                BusinessStats stats = businesses.get(classId);
                log.info("[业务用例开始] {} :: {}", stats.name, testIdentifier.getDisplayName());
            }
        }
    }

    @Override
    public void executionFinished(TestIdentifier testIdentifier, TestExecutionResult result) {
        if (!testIdentifier.isTest()) {
            return;
        }
        String classId = testToBusiness.get(testIdentifier.getUniqueId());
        if (classId == null) {
            return;
        }
        BusinessStats stats = businesses.get(classId);
        String verdict;
        switch (result.getStatus()) {
            case SUCCESSFUL -> {
                stats.passed++;
                verdict = "PASS";
            }
            case FAILED -> {
                stats.failed++;
                verdict = "FAIL";
            }
            case ABORTED -> {
                stats.aborted++;
                verdict = "ABORTED";
            }
            default -> verdict = result.getStatus().name();
        }
        log.info("[业务用例结果] {} :: {} -> {}", stats.name, testIdentifier.getDisplayName(), verdict);
        result.getThrowable().ifPresent(error ->
                log.info("[业务用例失败原因] {} :: {} -> {}",
                        stats.name, testIdentifier.getDisplayName(), error.toString()));
    }

    @Override
    public void testPlanExecutionFinished(TestPlan testPlan) {
        log.info("-------------------- 业务测试覆盖汇总 --------------------");
        int total = 0;
        int passed = 0;
        int failed = 0;
        int aborted = 0;
        for (BusinessStats stats : businesses.values()) {
            log.info("业务: {} | 用例总数 {} | 通过 {} | 失败 {} | 跳过/中止 {}",
                    stats.name, stats.total, stats.passed, stats.failed, stats.aborted);
            total += stats.total;
            passed += stats.passed;
            failed += stats.failed;
            aborted += stats.aborted;
        }
        log.info("总计: 用例 {} | 通过 {} | 失败 {} | 跳过/中止 {}", total, passed, failed, aborted);
        log.info("==================== 业务测试覆盖报告结束 ====================");
    }

    private static final class BusinessStats {
        private final String name;
        private int total;
        private int passed;
        private int failed;
        private int aborted;

        private BusinessStats(String name) {
            this.name = name;
        }
    }
}
