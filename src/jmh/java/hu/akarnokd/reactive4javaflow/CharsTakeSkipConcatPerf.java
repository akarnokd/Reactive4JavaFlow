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
 * gradle jmh -Pjmh="CharsTakeSkipConcatPerf"
 */
@BenchmarkMode(Mode.SampleTime)
@Warmup(iterations = 5)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(value = 1)
@State(Scope.Thread)
public class CharsTakeSkipConcatPerf {

    @Benchmark
    public void chars(Blackhole bh) {
        Folyam.characters("jezebel").subscribe(new PerfConsumer(bh));
    }

    @Benchmark
    public void take(Blackhole bh) {
        Folyam.characters("jezebel").take(3).subscribe(new PerfConsumer(bh));
    }

    @Benchmark
    public void skip(Blackhole bh) {
        Folyam.characters("jezebel").skip(3).subscribe(new PerfConsumer(bh));
    }

    @Benchmark
    public void concat(Blackhole bh) {
        Folyam.concatArray(Folyam.characters("jezebel").take(3),
                Folyam.characters("jezebel").skip(3))
                .subscribe(new PerfConsumer(bh));
    }

}