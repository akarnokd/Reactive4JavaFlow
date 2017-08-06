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

package hu.akarnokd.reactive4javaflow.impl.opeators;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import hu.akarnokd.reactive4javaflow.*;
import hu.akarnokd.reactive4javaflow.functionals.*;
import org.junit.Test;

public class ParallelMapTest {

    @Test
    public void subscriberCount() {
        ParallelFolyamTest.checkSubscriberCount(Folyam.range(1, 5).parallel()
        .map(v -> v));
    }

    @Test
    public void doubleFilter() {
        Folyam.range(1, 10)
        .parallel()
        .map(v -> v)
        .filter(new CheckedPredicate<Integer>() {
            @Override
            public boolean test(Integer v) throws Exception {
                return v % 2 == 0;
            }
        })
        .filter(new CheckedPredicate<Integer>() {
            @Override
            public boolean test(Integer v) throws Exception {
                return v % 3 == 0;
            }
        })
        .sequential()
        .test()
        .assertResult(6);
    }

    @Test
    public void doubleFilterAsync() {
        Folyam.range(1, 10)
        .parallel()
        .runOn(SchedulerServices.computation())
        .map(v -> v)
        .filter(new CheckedPredicate<Integer>() {
            @Override
            public boolean test(Integer v) throws Exception {
                return v % 2 == 0;
            }
        })
        .filter(new CheckedPredicate<Integer>() {
            @Override
            public boolean test(Integer v) throws Exception {
                return v % 3 == 0;
            }
        })
        .sequential()
        .test()
        .awaitDone(5, TimeUnit.SECONDS)
        .assertResult(6);
    }

    @Test
    public void doubleError() {
        TestHelper.withErrorTracking(errors -> {
            new ParallelInvalid()
                    .map(v -> v)
                    .sequential()
                    .test()
                    .assertFailure(IOException.class);

            assertFalse(errors.isEmpty());
            for (Throwable ex : errors) {
                assertTrue(ex.toString(), ex instanceof IOException);
            }
        });
    }

    @Test
    public void doubleError2() {
        TestHelper.withErrorTracking(errors -> {
            new ParallelInvalid()
                    .map(v -> v)
                    .filter(v -> true)
                    .sequential()
                    .test()
                    .assertFailure(IOException.class);

            assertFalse(errors.isEmpty());
            for (Throwable ex : errors) {
                assertTrue(ex.toString(), ex instanceof IOException);
            }
        });
    }

    @Test
    public void error() {
        Folyam.error(new IOException())
        .parallel()
        .map(v -> v)
        .sequential()
        .test()
        .assertFailure(IOException.class);
    }

    @Test
    public void mapCrash() {
        Folyam.just(1)
        .parallel()
        .map(new CheckedFunction<Integer, Object>() {
            @Override
            public Object apply(Integer v) throws Exception {
                throw new IOException();
            }
        })
        .sequential()
        .test()
        .assertFailure(IOException.class);
    }

    @Test
    public void mapCrashConditional() {
        Folyam.just(1)
        .parallel()
        .map(new CheckedFunction<Integer, Object>() {
            @Override
            public Object apply(Integer v) throws Exception {
                throw new IOException();
            }
        })
        .filter(v -> true)
        .sequential()
        .test()
        .assertFailure(IOException.class);
    }

    @Test
    public void mapCrashConditional2() {
        Folyam.just(1)
        .parallel()
        .runOn(SchedulerServices.computation())
        .map(new CheckedFunction<Integer, Object>() {
            @Override
            public Object apply(Integer v) throws Exception {
                throw new IOException();
            }
        })
        .filter(v -> true)
        .sequential()
        .test()
        .awaitDone(5, TimeUnit.SECONDS)
        .assertFailure(IOException.class);
    }
}
