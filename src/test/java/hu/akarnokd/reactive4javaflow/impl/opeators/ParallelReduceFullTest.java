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

import hu.akarnokd.reactive4javaflow.*;
import hu.akarnokd.reactive4javaflow.functionals.CheckedBiFunction;
import hu.akarnokd.reactive4javaflow.hot.DirectProcessor;
import org.junit.Test;

public class ParallelReduceFullTest {

    @Test
    public void cancel() {
        DirectProcessor<Integer> pp = new DirectProcessor<>();

        TestConsumer<Integer> ts = pp
        .parallel()
        .reduce(new CheckedBiFunction<Integer, Integer, Integer>() {
            @Override
            public Integer apply(Integer a, Integer b) throws Exception {
                return a + b;
            }
        })
        .test();

        assertTrue(pp.hasSubscribers());

        ts.cancel();

        assertFalse(pp.hasSubscribers());
    }

    @Test
    public void error() {
        TestHelper.withErrorTracking(errors -> {
            Folyam.<Integer>error(new IOException())
                    .parallel()
                    .reduce(new CheckedBiFunction<Integer, Integer, Integer>() {
                        @Override
                        public Integer apply(Integer a, Integer b) throws Exception {
                            return a + b;
                        }
                    })
                    .test()
                    .assertFailure(IOException.class);

            assertTrue(errors.isEmpty());
        });
    }

    @SuppressWarnings("unchecked")
    @Test
    public void error2() {
        TestHelper.withErrorTracking(errors -> {
            ParallelFolyam.fromArray(Folyam.<Integer>error(new IOException()), Folyam.<Integer>error(new IOException()))
            .reduce(new CheckedBiFunction<Integer, Integer, Integer>() {
                @Override
                public Integer apply(Integer a, Integer b) throws Exception {
                    return a + b;
                }
            })
            .test()
            .assertFailure(IOException.class);

            TestHelper.assertError(errors, 0, IOException.class);
        });
    }

    @Test
    public void empty() {
        Folyam.<Integer>empty()
        .parallel()
        .reduce(new CheckedBiFunction<Integer, Integer, Integer>() {
            @Override
            public Integer apply(Integer a, Integer b) throws Exception {
                return a + b;
            }
        })
        .test()
        .assertResult();
    }

    @Test
    public void doubleError() {
        TestHelper.withErrorTracking(errors -> {
            new ParallelInvalid()
            .reduce(new CheckedBiFunction<Object, Object, Object>() {
                @Override
                public Object apply(Object a, Object b) throws Exception {
                    return "" + a + b;
                }
            })
            .test()
            .assertFailure(IOException.class);

            assertFalse(errors.isEmpty());
            for (Throwable ex : errors) {
                assertTrue(ex.toString(), ex instanceof IOException);
            }
        });
    }

    @Test
    public void reducerCrash() {
        Folyam.range(1, 4)
        .parallel(2)
        .reduce(new CheckedBiFunction<Integer, Integer, Integer>() {
            @Override
            public Integer apply(Integer a, Integer b) throws Exception {
                if (b == 3) {
                    throw new IOException();
                }
                return a + b;
            }
        })
        .test()
        .assertFailure(IOException.class);
    }

    @Test
    public void reducerCrash2() {
        Folyam.range(1, 4)
        .parallel(2)
        .reduce(new CheckedBiFunction<Integer, Integer, Integer>() {
            @Override
            public Integer apply(Integer a, Integer b) throws Exception {
                if (a == 1 + 3) {
                    throw new IOException();
                }
                return a + b;
            }
        })
        .test()
        .assertFailure(IOException.class);
    }
}
