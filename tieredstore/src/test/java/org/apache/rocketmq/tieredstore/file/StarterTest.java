package org.apache.rocketmq.tieredstore.file;

import org.junit.Test;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

public class StarterTest {
    public static int slotNum = 500000;
    public static int indexNum = 2000000;
    @Test
    public void testOriginIndexService() throws RunnerException {
        Options opt = new OptionsBuilder()
            .include(OriginIndexServiceBenchMark.class.getSimpleName())
            .include(NewIndexServiceBenchMark.class.getSimpleName())//
            .build();
        new Runner(opt).run();
    }

//    @Test
//    public void testNewIndexService() throws RunnerException {
//        Options opt = new OptionsBuilder().include(NewIndexServiceBenchMark.class.getSimpleName())
//            .build();
//        new Runner(opt).run();
//    }
}
