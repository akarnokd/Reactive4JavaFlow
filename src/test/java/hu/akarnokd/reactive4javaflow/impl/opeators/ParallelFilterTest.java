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
import hu.akarnokd.reactive4javaflow.functionals.CheckedPredicate;
import hu.akarnokd.reactive4javaflow.impl.opeators.*;
import org.junit.Test;

public class ParallelFilterTest {

    @Test
    public void subscriberCount() {
        ParallelFolyamTest.checkSubscriberCount(Folyam.range(1, 5).parallel()
        .filter(v -> true));
    }

    @Test
    public void doubleFilter() {
        Folyam.range(1, 10)
        .parallel()
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
    public void doubleError() {
        TestHelper.withErrorTracking(errors -> {
            new ParallelInvalid()
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
    public void doubleError2() {
        TestHelper.withErrorTracking(errors -> {
            new ParallelInvalid()
                    .filter(v -> true)
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
        .filter(v -> true)
        .sequential()
        .test()
        .assertFailure(IOException.class);
    }

    @Test
    public void predicateThrows() {
        Folyam.just(1)
        .parallel()
        .filter(new CheckedPredicate<Integer>() {
            @Override
            public boolean test(Integer v) throws Exception {
                throw new IOException();
            }
        })
        .filter(v -> true)
        .sequential()
        .test()
        .assertFailure(IOException.class);
    }
}
