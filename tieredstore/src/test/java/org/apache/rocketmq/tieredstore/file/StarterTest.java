package org.apache.rocketmq.tieredstore.file;

import org.junit.Test;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

public class StarterTest {
    @Test
    public void testOriginIndexService() throws RunnerException {
        Options opt = new OptionsBuilder()
            .include(OriginIndexServiceBenchMark.class.getSimpleName())
            //.include(NewIndexServiceBenchMark.class.getSimpleName())// 要导入的测试类
            .build();
        new Runner(opt).run(); // 执行测试
    }

    @Test
    public void testNewIndexService() throws RunnerException {
        Options opt = new OptionsBuilder().include(NewIndexServiceBenchMark.class.getSimpleName()) // 要导入的测试类
            .build();
        new Runner(opt).run(); // 执行测试
    }
}
