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

import hu.akarnokd.reactive4javaflow.impl.VH;
import org.openjdk.jmh.annotations.*;

import java.lang.invoke.*;
import java.util.concurrent.TimeUnit;

/**
 * Example benchmark. Run from command line as
 * <br>
 * gradle jmh -Pjmh="VolatilePerf"
 */
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 5)
@Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(value = 1)
@State(Scope.Thread)
public class VolatilePerf {

    Object ref;
    static final VarHandle REF = VH.find(MethodHandles.lookup(), VolatilePerf.class, "ref", Object.class);


    int[] value = new int[512];
    static final VarHandle VALUE = MethodHandles.arrayElementVarHandle(int[].class);

    @Benchmark
    public void setRelease() {
        REF.setRelease(this, this);
    }

    @Benchmark
    public void setVolatile() {
        REF.setVolatile(this, this);
    }

    @Benchmark
    public void getAndSet() {
        REF.getAndSet(this, this);
    }

    @Benchmark
    public void getAndAdd() {
        VALUE.getAndAdd(value, 256, 0);
    }

    @Benchmark
    public void releaseGetAndAdd() {
        REF.setRelease(this, this);
        VALUE.getAndAdd(value, 256, 0);
    }
}