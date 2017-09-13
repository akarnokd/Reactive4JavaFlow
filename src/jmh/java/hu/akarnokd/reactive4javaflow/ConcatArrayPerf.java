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

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * Example benchmark. Run from command line as
 * <br>
 * gradle jmh -Pjmh="ConcatArrayPerf"
 */
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 5)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(value = 1)
@State(Scope.Thread)
public class ConcatArrayPerf {

    @Param({"1", "10", "100", "1000", "10000", "100000", "1000000"})
    public int count;

    Folyam<Integer>[] sources;

    @Setup
    public void setup() {
        int sourceLength = 1_000_000 / count;
        sources = new Folyam[count];
        Integer[] data = new Integer[sourceLength];
        Arrays.fill(data, 777);
        Arrays.fill(sources, Folyam.fromArray(data));
    }

    @Benchmark
    public void crossConcat(Blackhole bh) {
        Folyam.concatArray(sources).subscribe(new PerfConsumer(bh));
    }
}