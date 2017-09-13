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
 * gradle jmh -Pjmh="MapPerf"
 */
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 5)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(value = 1)
@State(Scope.Thread)
public class MapPerf {

    Folyam<Integer> folyam = Folyam.just(1).map(v -> v + 1);

    Esetleg<Integer> esetleg = Esetleg.just(1).map(v -> v + 1);

    @Benchmark
    public void folyam(Blackhole bh) {
        folyam.subscribe(new PerfConsumer(bh));
    }

    @Benchmark
    public void esetleg(Blackhole bh) {
        esetleg.subscribe(new PerfConsumer(bh));
    }

}