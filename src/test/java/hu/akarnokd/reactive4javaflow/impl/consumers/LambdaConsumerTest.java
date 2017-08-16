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

package hu.akarnokd.reactive4javaflow.impl.consumers;

import hu.akarnokd.reactive4javaflow.*;
import hu.akarnokd.reactive4javaflow.errors.CompositeThrowable;
import hu.akarnokd.reactive4javaflow.impl.BooleanSubscription;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertTrue;

public class LambdaConsumerTest {

    @Test
    public void onSubscribeCrash() {
        TestConsumer<Integer> tc = new TestConsumer<>();

        LambdaConsumer<Integer> lc = new LambdaConsumer<>(
                tc::onNext,
                tc::onError,
                tc::onComplete,
                s -> {
                    tc.onSubscribe(s);
                    throw new IOException();
                }
        );

        lc.onSubscribe(new BooleanSubscription());
        tc.assertFailure(IOException.class);
    }

    @Test
    public void onNextCrash() {
        TestConsumer<Integer> tc = new TestConsumer<>();

        LambdaConsumer<Integer> lc = new LambdaConsumer<Integer>(
                e -> {
                    throw new IOException();
                },
                tc::onError,
                tc::onComplete,
                tc::onSubscribe
        );

        lc.onSubscribe(new BooleanSubscription());
        lc.onNext(1);
        lc.onNext(2);
        tc.assertFailure(IOException.class);
    }


    @Test
    public void onErrorCrash() {
        TestHelper.withErrorTracking(errors -> {
            TestConsumer<Integer> tc = new TestConsumer<>();

            LambdaConsumer<Integer> lc = new LambdaConsumer<Integer>(
                    tc::onNext,
                    e -> {
                        throw new IOException();
                    },
                    tc::onComplete,
                    tc::onSubscribe
            );

            lc.onSubscribe(new BooleanSubscription());
            lc.onError(new IllegalArgumentException("1"));
            lc.onError(new IllegalArgumentException("2"));
            tc.assertEmpty();

            TestHelper.assertError(errors, 0, CompositeThrowable.class);
            TestHelper.assertError(Arrays.asList(errors.get(0).getSuppressed()), 0, IllegalArgumentException.class, "1");
            TestHelper.assertError(Arrays.asList(errors.get(0).getSuppressed()), 1, IOException.class);
            TestHelper.assertError(errors, 1, IllegalArgumentException.class, "2");
        });
    }


    @Test
    public void onCompleteCrash() {
        TestHelper.withErrorTracking(errors -> {
            TestConsumer<Integer> tc = new TestConsumer<>();

            LambdaConsumer<Integer> lc = new LambdaConsumer<Integer>(
                    tc::onNext,
                    tc::onError,
                    () -> {
                        throw new IOException();
                    },
                    tc::onSubscribe
            );

            lc.onSubscribe(new BooleanSubscription());
            lc.onComplete();
            lc.onComplete();

            tc.assertEmpty();

            TestHelper.assertError(errors, 0, IOException.class);
        });
    }

    @Test
    public void normalError() {
        TestConsumer<Integer> tc = new TestConsumer<>();

        LambdaConsumer<Integer> lc = new LambdaConsumer<>(
                tc::onNext,
                tc::onError,
                tc::onComplete,
                tc::onSubscribe
        );

        lc.onSubscribe(new BooleanSubscription());
        lc.onError(new IOException());

        tc.assertFailure(IOException.class);
    }

    @Test
    public void normalComplete() {
        TestConsumer<Integer> tc = new TestConsumer<>();

        LambdaConsumer<Integer> lc = new LambdaConsumer<>(
                tc::onNext,
                tc::onError,
                tc::onComplete,
                tc::onSubscribe
        );

        lc.onSubscribe(new BooleanSubscription());
        lc.onNext(1);
        lc.onNext(2);
        lc.onComplete();

        tc.assertResult(1, 2);
    }


    @Test
    public void close() {
        TestConsumer<Integer> tc = new TestConsumer<>();

        LambdaConsumer<Integer> lc = new LambdaConsumer<>(
                tc::onNext,
                tc::onError,
                tc::onComplete,
                tc::onSubscribe
        );

        BooleanSubscription bc = new BooleanSubscription();
        lc.onSubscribe(bc);
        lc.close();

        assertTrue(bc.isCancelled());
    }

}
