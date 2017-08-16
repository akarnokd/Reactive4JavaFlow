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

import hu.akarnokd.reactive4javaflow.processors.*;
import org.openjdk.jmh.annotations.*;

import java.util.concurrent.*;

/**
 * Example benchmark. Run from command line as
 * <br>
 * gradle jmh -Pjmh="CompletableFutureComparisonPerf"
 */
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 5)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(value = 1)
@State(Scope.Thread)
public class CompletableFutureComparisonPerf {

    final CompletableFuture<Integer> completedFuture = CompletableFuture.completedFuture(1);

    LastProcessor<Integer> lastProcessor;

    @Setup
    public void setup() {
        lastProcessor = new LastProcessor<>();
        lastProcessor.onNext(1);
        lastProcessor.onComplete();
    }

    @Benchmark
    public Object completableFuture() throws Exception {
        CompletableFuture<Integer> cf = new CompletableFuture<>();
        cf.complete(1);
        return cf.get();
    }

    @Benchmark
    public Object completableFutureNow() throws Exception {
        CompletableFuture<Integer> cf = new CompletableFuture<>();
        cf.complete(1);
        return cf.getNow(null);
    }

    @Benchmark
    public Object completableFutureCompleted() throws Exception {
        return completedFuture.get();
    }

    @Benchmark
    public Object completableFutureCompletedNow() throws Exception {
        return completedFuture.getNow(null);
    }

    @Benchmark
    public Object directProcessor() {
        DirectProcessor<Integer> dp = new DirectProcessor<>();
        dp.onNext(1);
        dp.onComplete();

        return dp.blockingLast();
    }

    @Benchmark
    public Object lastProcessor() {
        LastProcessor<Integer> dp = new LastProcessor<>();
        dp.onNext(1);
        dp.onComplete();

        return dp.blockingLast();
    }

    @Benchmark
    public Object lastProcessorValue() {
        LastProcessor<Integer> dp = new LastProcessor<>();
        dp.onNext(1);
        dp.onComplete();

        return dp.getValue();
    }

    @Benchmark
    public Object lastProcessorCompleted() {
        return lastProcessor.blockingLast();
    }

    @Benchmark
    public Object lastProcessorCompletedValue() {
        return lastProcessor.getValue();
    }
}