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
import java.util.*;
import java.util.concurrent.Callable;

import hu.akarnokd.reactive4javaflow.*;
import hu.akarnokd.reactive4javaflow.functionals.CheckedBiConsumer;
import hu.akarnokd.reactive4javaflow.hot.DirectProcessor;
import org.junit.Test;

public class ParallelCollectTest {

    @Test
    public void subscriberCount() {
        ParallelFolyamTest.checkSubscriberCount(Folyam.range(1, 5).parallel()
        .collect(new Callable<List<Integer>>() {
            @Override
            public List<Integer> call() throws Exception {
                return new ArrayList<Integer>();
            }
        }, new CheckedBiConsumer<List<Integer>, Integer>() {
            @Override
            public void accept(List<Integer> a, Integer b) throws Exception {
                a.add(b);
            }
        }));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void initialCrash() {
        Folyam.range(1, 5)
        .parallel()
        .collect(new Callable<List<Integer>>() {
            @Override
            public List<Integer> call() throws Exception {
                throw new IOException();
            }
        }, new CheckedBiConsumer<List<Integer>, Integer>() {
            @Override
            public void accept(List<Integer> a, Integer b) throws Exception {
                a.add(b);
            }
        })
        .sequential()
        .test()
        .assertFailure(IOException.class);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void reducerCrash() {
        Folyam.range(1, 5)
        .parallel()
        .collect(new Callable<List<Integer>>() {
            @Override
            public List<Integer> call() throws Exception {
                return new ArrayList<Integer>();
            }
        }, new CheckedBiConsumer<List<Integer>, Integer>() {
            @Override
            public void accept(List<Integer> a, Integer b) throws Exception {
                if (b == 3) {
                    throw new IOException();
                }
                a.add(b);
            }
        })
        .sequential()
        .test()
        .assertFailure(IOException.class);
    }

    @Test
    public void cancel() {
        DirectProcessor<Integer> pp = new DirectProcessor<>();

        TestConsumer<List<Integer>> ts = pp
        .parallel()
        .collect(new Callable<List<Integer>>() {
            @Override
            public List<Integer> call() throws Exception {
                return new ArrayList<Integer>();
            }
        }, new CheckedBiConsumer<List<Integer>, Integer>() {
            @Override
            public void accept(List<Integer> a, Integer b) throws Exception {
                a.add(b);
            }
        })
        .sequential()
        .test();

        assertTrue(pp.hasSubscribers());

        ts.cancel();

        assertFalse(pp.hasSubscribers());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void error() {
        Folyam.<Integer>error(new IOException())
        .parallel()
        .collect(new Callable<List<Integer>>() {
            @Override
            public List<Integer> call() throws Exception {
                return new ArrayList<Integer>();
            }
        }, new CheckedBiConsumer<List<Integer>, Integer>() {
            @Override
            public void accept(List<Integer> a, Integer b) throws Exception {
                a.add(b);
            }
        })
        .sequential()
        .test()
        .assertFailure(IOException.class);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void doubleError() {
        TestHelper.withErrorTracking(errors -> {
            new ParallelInvalid()
                    .collect(new Callable<List<Object>>() {
                        @Override
                        public List<Object> call() throws Exception {
                            return new ArrayList<Object>();
                        }
                    }, new CheckedBiConsumer<List<Object>, Object>() {
                        @Override
                        public void accept(List<Object> a, Object b) throws Exception {
                            a.add(b);
                        }
                    })
                    .sequential()
                    .test()
                    .assertFailure(IOException.class);

            assertFalse(errors.isEmpty());
            for (Throwable ex : errors) {
                assertTrue(ex.toString(), ex instanceof IOException);
            }
        });
    }
}
