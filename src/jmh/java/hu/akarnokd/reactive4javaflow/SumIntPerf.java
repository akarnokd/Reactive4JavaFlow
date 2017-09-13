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

import hu.akarnokd.reactive4javaflow.impl.DeferredScalarSubscription;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Arrays;
import java.util.concurrent.*;

/**
 * Example benchmark. Run from command line as
 * <br>
 * gradle jmh -Pjmh="SumIntPerf"
 */
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 5)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(value = 1, jvmArgs = {
        "-XX:MaxInlineLevel=20"
//            , "-XX:+UnlockDiagnosticVMOptions",
//            , "-XX:+PrintAssembly",
//            , "-XX:+TraceClassLoading",
//            , "-XX:+LogCompilation"
})
@State(Scope.Thread)
public class SumIntPerf {

    @Param({"1", "10", "100", "1000", "10000", "100000", "1000000"})
    public int count;

    Folyam<Integer> array;

    Esetleg<Integer> sum;

    Esetleg<Integer> sum2;

    Esetleg<Integer> sum3;

    @Setup
    public void setup() {
        Integer[] values = new Integer[count];
        Arrays.fill(values, 777);

        array = Folyam.fromArray(values);
        sum = array.sumInt(v -> v);

        sum2 = new SumInt(array);

        sum3 = new SumInt2(array);
    }

    //@Benchmark
    public void sumInt(Blackhole bh) {
        sum.subscribe(new PerfConsumer(bh));
    }


    //@Benchmark
    public void sumInt2(Blackhole bh) {
        sum2.subscribe(new PerfConsumer(bh));
    }

    @Benchmark
    public void sumInt3(Blackhole bh) {
        sum3.subscribe(new PerfConsumer(bh));
    }

    static final class SumInt extends Esetleg<Integer> {

        final Folyam<? extends Number> source;

        SumInt(Folyam<? extends Number> source) {
            this.source = source;
        }

        @Override
        protected void subscribeActual(FolyamSubscriber<? super Integer> s) {
            source.subscribe(new SumIntSubscriber(s));
        }
    }

    static final class SumIntSubscriber extends DeferredScalarSubscription<Integer> implements FolyamSubscriber<Number> {

        Flow.Subscription upstream;

        boolean hasValue;
        int sum;

        public SumIntSubscriber(FolyamSubscriber<? super Integer> actual) {
            super(actual);
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            upstream = subscription;
            actual.onSubscribe(this);
            subscription.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(Number item) {
            if (!hasValue) {
                hasValue = true;
            }
            sum += item.intValue();
        }

        @Override
        public void onError(Throwable throwable) {
            error(throwable);
        }

        @Override
        public void onComplete() {
            if (hasValue) {
                complete(sum);
            } else {
                complete();
            }
        }

        @Override
        public void cancel() {
            super.cancel();
            upstream.cancel();
        }
    }


    static final class SumInt2 extends Esetleg<Integer> {

        final Folyam<? extends Number> source;

        SumInt2(Folyam<? extends Number> source) {
            this.source = source;
        }

        @Override
        protected void subscribeActual(FolyamSubscriber<? super Integer> s) {
            source.subscribe(new SumIntSubscriber2(s));
        }
    }

    static final class SumIntSubscriber2 implements FolyamSubscriber<Number>, Flow.Subscription {

        final FolyamSubscriber<? super Integer> actual;

        Flow.Subscription upstream;

        boolean hasValue;
        int sum;

        public SumIntSubscriber2(FolyamSubscriber<? super Integer> actual) {
            this.actual = actual;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            upstream = subscription;
            actual.onSubscribe(this);
        }

        @Override
        public void onNext(Number item) {
            if (!hasValue) {
                hasValue = true;
            }
            sum += item.intValue();
        }

        @Override
        public void onError(Throwable throwable) {
            actual.onError(throwable);
        }

        @Override
        public void onComplete() {
            if (hasValue) {
                actual.onNext(sum);
            }
            actual.onComplete();
        }

        @Override
        public void cancel() {
            upstream.cancel();
        }

        @Override
        public void request(long n) {
            upstream.request(Long.MAX_VALUE);
        }
    }

}