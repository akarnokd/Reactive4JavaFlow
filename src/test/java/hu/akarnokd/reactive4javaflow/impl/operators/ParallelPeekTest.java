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

package hu.akarnokd.reactive4javaflow.impl.operators;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.concurrent.Flow;

import hu.akarnokd.reactive4javaflow.*;
import hu.akarnokd.reactive4javaflow.functionals.*;
import org.junit.Test;

public class ParallelPeekTest {

    @Test
    public void subscriberCount() {
        ParallelFolyamTest.checkSubscriberCount(Folyam.range(1, 5).parallel()
        .doOnNext(v -> { }));
    }

    @Test
    public void onSubscribeCrash() {
        Folyam.range(1, 5)
        .parallel()
        .doOnSubscribe(new CheckedConsumer<Flow.Subscription>() {
            @Override
            public void accept(Flow.Subscription s) throws Exception {
                throw new IOException();
            }
        })
        .sequential()
        .test()
        .assertFailure(IOException.class);
    }

    @Test
    public void doubleError() {
        TestHelper.withErrorTracking(errors -> {
            new ParallelInvalid()
                    .doOnNext(v -> { })
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
    public void requestCrash() {
        TestHelper.withErrorTracking(errors -> {
            Folyam.range(1, 5)
                    .parallel()
                    .doOnRequest(new CheckedConsumer<Long>() {
                        @Override
                        public void accept(Long n) throws Exception {
                            throw new IOException();
                        }
                    })
                    .sequential()
                    .test()
                    .assertResult(1, 2, 3, 4, 5);

            assertFalse(errors.isEmpty());

            for (Throwable ex : errors) {
                assertTrue(ex.toString(), ex instanceof IOException);
            }
        });
    }

    @Test
    public void cancelCrash() {
        TestHelper.withErrorTracking(errors -> {
            Folyam.<Integer>never()
            .parallel()
            .doOnCancel(new CheckedRunnable() {
                @Override
                public void run() throws Exception {
                    throw new IOException();
                }
            })
            .sequential()
            .test()
            .cancel();

            assertFalse(errors.isEmpty());

            for (Throwable ex : errors) {
                assertTrue(ex.toString(), ex instanceof IOException);
            }
        });
    }

    @Test
    public void onCompleteCrash() {
        TestHelper.withErrorTracking(errors -> {
            Folyam.just(1)
            .parallel()
            .doOnComplete(new CheckedRunnable() {
                @Override
                public void run() throws Exception {
                    throw new IOException();
                }
            })
            .sequential()
            .test()
            .assertFailure(IOException.class, 1);

            for (int i = 0; i < errors.size(); i++) {
                TestHelper.assertError(errors, i, IOException.class);
            }
        });
    }
}
