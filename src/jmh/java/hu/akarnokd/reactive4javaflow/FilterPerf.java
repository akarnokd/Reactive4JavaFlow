/*
 * Copyright 2017 David Karnok
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package hu.akarnokd.reactive4javaflow;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

/**
 * Example benchmark. Run from command line as
 * <br>
 * gradle jmh -Pjmh="FilterPerf"
 */
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 5)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(value = 1)
@State(Scope.Thread)
public class FilterPerf {

    Folyam<Integer> folyamTrue = Folyam.just(1).filter(v -> v == 1);
    Folyam<Integer> folyamFalse = Folyam.just(1).filter(v -> v != 1);

    Esetleg<Integer> esetlegTrue = Esetleg.just(1).filter(v -> v == 1);
    Esetleg<Integer> esetlegFalse = Esetleg.just(1).filter(v -> v != 1);

    @Benchmark
    public void folyamTrue(Blackhole bh) {
        folyamTrue.subscribe(new PerfConsumer(bh));
    }

    @Benchmark
    public void folyamFalse(Blackhole bh) {
        folyamFalse.subscribe(new PerfConsumer(bh));
    }

    @Benchmark
    public void esetlegTrue(Blackhole bh) {
        esetlegTrue.subscribe(new PerfConsumer(bh));
    }

    @Benchmark
    public void esetlegFalse(Blackhole bh) {
        esetlegFalse.subscribe(new PerfConsumer(bh));
    }

}