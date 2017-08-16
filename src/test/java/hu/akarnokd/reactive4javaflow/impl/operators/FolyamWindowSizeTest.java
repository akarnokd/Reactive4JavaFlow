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

import hu.akarnokd.reactive4javaflow.*;
import hu.akarnokd.reactive4javaflow.errors.CompositeThrowable;
import hu.akarnokd.reactive4javaflow.processors.DirectProcessor;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FolyamWindowSizeTest {

    @Test
    public void standard() {
        TestHelper.assertResult(
                Folyam.range(1, 5).window(2)
                .flatMap(v -> v),
                1, 2, 3, 4, 5
        );
    }


    @Test
    public void standardSkip() {
        TestHelper.assertResult(
                Folyam.range(1, 5).window(1, 2)
                        .flatMap(v -> v),
                1, 3, 5
        );
    }

    @Test
    public void standardSkip2() {
        TestHelper.assertResult(
                Folyam.range(1, 5).window(2, 3)
                        .flatMap(v -> v),
                1, 2, 4, 5
        );
    }

    @Test
    public void cancelUpstream() {
        DirectProcessor<Integer> dp = new DirectProcessor<>();

        TestConsumer<Integer> tc = dp.window(2)
                .take(1)
                .flatMap(v -> v.take(1))
                .test();

        assertTrue(dp.hasSubscribers());

        dp.onNext(1);

        assertFalse(dp.hasSubscribers());

        tc.assertResult(1);
    }

    @Test
    public void error() {
        DirectProcessor<Integer> dp = new DirectProcessor<>();

        TestConsumer<Integer> tc = dp.window(FolyamPlugins.defaultBufferSize() * 2)
                .flatMapDelayError(v -> v)
                .test();

        dp.onNext(1);
        dp.onError(new IOException());

        tc.assertFailure(CompositeThrowable.class, 1)
                .assertInnerErrors(errors -> {
                    TestHelper.assertError(errors, 0, IOException.class);
                    TestHelper.assertError(errors, 1, IOException.class);
                });
    }

    @Test
    public void cancelNoWindow() {
        DirectProcessor<Integer> dp = new DirectProcessor<>();

        TestConsumer<Integer> tc = dp.window(2)
                .flatMapDelayError(v -> v)
                .test()
                .cancel();

        assertFalse(dp.hasSubscribers());
    }

    @Test
    public void cancelAfterOneItem() {
        DirectProcessor<Integer> dp = new DirectProcessor<>();

        TestConsumer<Integer> tc = dp.window(2)
                .flatMapDelayError(v -> v)
                .test();

        dp.onNext(1);

        tc.cancel();

        assertFalse(dp.hasSubscribers());
    }

    @Test
    public void nextCancelRace() {
        for (int i = 0; i < 1000; i++) {
            DirectProcessor<Integer> dp = new DirectProcessor<>();

            TestConsumer<Folyam<Integer>> tc = dp.window(2)
                    .test();

            Runnable r1 = () -> dp.onNext(1);
            Runnable r2 = tc::cancel;

            TestHelper.race(r1, r2);

            if (tc.values().size() > 0) {
                tc.values().get(0).test().cancel();
            }

            assertFalse(dp.hasSubscribers());
        }
    }


    @Test
    public void standardOverlapping() {
        TestHelper.assertResult(
                Folyam.range(1, 5).window(2, 1)
                        .flatMap(v -> v),
                1, 2, 2, 3, 3, 4, 4, 5, 5
        );
    }

    @Test
    public void overlapNormal() {
        Folyam.range(1, 5).window(2, 1)
                .flatMap(v -> v)
                .test()
                .assertResult(1, 2, 2, 3, 3, 4, 4, 5, 5);
    }

    @Test
    public void overlapNormal2() {
        Folyam.range(1, 5).window(3, 1)
                .flatMap(v -> v)
                .test()
                .assertResult(1, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 5);
    }

    @Test
    public void overlapError() {
        Folyam.error(new IOException()).window(2, 1)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void overlapErrorBackpressured() {
        Folyam.error(new IOException()).window(2, 1)
                .test(0)
                .assertFailure(IOException.class);
    }

    @Test
    public void overlapErrorInner() {
        DirectProcessor<Integer> dp = new DirectProcessor<>();

        TestConsumer<Folyam<Integer>> tc = dp.window(2, 1).test();

        dp.onNext(1);
        dp.onError(new IOException());

        tc.assertError(IOException.class);
        tc.values().get(0).test().assertFailure(IOException.class, 1);
    }

    @Test
    public void overlapEmpty() {
        Folyam.empty().window(2, 1)
                .test()
                .assertResult();
    }

    @Test
    public void overlapEmptyBackpressured() {
        Folyam.empty().window(2, 1)
                .test(0)
                .assertResult();
    }

    @Test
    public void overlap32() {
        Folyam.range(1, 5).window(3, 2)
                .flatMap(v -> v)
                .test()
                .assertResult(1, 2, 3, 3, 4, 5, 5);
    }

    @Test
    public void overlap32Take1() {
        Folyam.range(1, 5).window(3, 2)
                .flatMap(v -> v, 1)
                .take(1)
                .test(1)
                .assertResult(1);
    }
}
