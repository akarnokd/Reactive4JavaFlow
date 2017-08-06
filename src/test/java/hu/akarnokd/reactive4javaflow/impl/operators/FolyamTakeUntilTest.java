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
import hu.akarnokd.reactive4javaflow.hot.DirectProcessor;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FolyamTakeUntilTest {

    @Test
    public void standard() {
        TestHelper.assertResult(
                Folyam.range(1, 5)
                .takeUntil(Folyam.never())
                , 1, 2, 3, 4, 5
        );
    }

    @Test
    public void standard1() {
        TestHelper.assertResult(
                Folyam.range(1, 5).hide()
                        .takeUntil(Folyam.never())
                , 1, 2, 3, 4, 5
        );
    }

    @Test
    public void error() {
        Folyam.error(new IOException())
                .takeUntil(Folyam.never())
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void empty() {
        Folyam.empty()
                .takeUntil(Folyam.never())
                .test()
                .assertResult();
    }

    @Test
    public void never() {
        Folyam.never()
                .takeUntil(Folyam.empty())
                .test()
                .assertResult();
    }

    @Test
    public void untilJust() {
        Folyam.never()
                .takeUntil(Folyam.just(1))
                .test()
                .assertResult();
    }


    @Test
    public void untilError() {
        Folyam.never()
                .takeUntil(Folyam.error(new IOException()))
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void mainCompleteCancelsOther() {
        DirectProcessor<Integer> dp1 = new DirectProcessor<>();
        DirectProcessor<Integer> dp2 = new DirectProcessor<>();

        TestConsumer<Integer> tc = dp1.takeUntil(dp2).test();

        assertTrue(dp1.hasSubscribers());
        assertTrue(dp2.hasSubscribers());

        dp1.onComplete();

        tc.assertResult();

        assertFalse(dp1.hasSubscribers());
        assertFalse(dp2.hasSubscribers());
    }


    @Test
    public void mainErrorCancelsOther() {
        DirectProcessor<Integer> dp1 = new DirectProcessor<>();
        DirectProcessor<Integer> dp2 = new DirectProcessor<>();

        TestConsumer<Integer> tc = dp1.takeUntil(dp2).test();

        assertTrue(dp1.hasSubscribers());
        assertTrue(dp2.hasSubscribers());

        dp1.onError(new IOException());

        tc.assertFailure(IOException.class);

        assertFalse(dp1.hasSubscribers());
        assertFalse(dp2.hasSubscribers());
    }

    @Test
    public void otherNextCancelsMain() {
        DirectProcessor<Integer> dp1 = new DirectProcessor<>();
        DirectProcessor<Integer> dp2 = new DirectProcessor<>();

        TestConsumer<Integer> tc = dp1.takeUntil(dp2).test();

        assertTrue(dp1.hasSubscribers());
        assertTrue(dp2.hasSubscribers());

        dp2.onNext(1);

        tc.assertResult();

        assertFalse(dp1.hasSubscribers());
        assertFalse(dp2.hasSubscribers());
    }

    @Test
    public void otherErrorCancelsMain() {
        DirectProcessor<Integer> dp1 = new DirectProcessor<>();
        DirectProcessor<Integer> dp2 = new DirectProcessor<>();

        TestConsumer<Integer> tc = dp1.takeUntil(dp2).test();

        assertTrue(dp1.hasSubscribers());
        assertTrue(dp2.hasSubscribers());

        dp2.onError(new IOException());

        tc.assertFailure(IOException.class);

        assertFalse(dp1.hasSubscribers());
        assertFalse(dp2.hasSubscribers());
    }

    @Test
    public void otherCompleteCancelsMain() {
        DirectProcessor<Integer> dp1 = new DirectProcessor<>();
        DirectProcessor<Integer> dp2 = new DirectProcessor<>();

        TestConsumer<Integer> tc = dp1.takeUntil(dp2).test();

        assertTrue(dp1.hasSubscribers());
        assertTrue(dp2.hasSubscribers());

        dp2.onComplete();

        tc.assertResult();

        assertFalse(dp1.hasSubscribers());
        assertFalse(dp2.hasSubscribers());
    }

    @Test
    public void errorConditional() {
        Folyam.error(new IOException())
                .takeUntil(Folyam.never())
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void emptyConditional() {
        Folyam.empty()
                .takeUntil(Folyam.never())
                .filter(v -> true)
                .test()
                .assertResult();
    }

    @Test
    public void neverConditional() {
        Folyam.never()
                .takeUntil(Folyam.empty())
                .filter(v -> true)
                .test()
                .assertResult();
    }

    @Test
    public void untilJustConditional() {
        Folyam.never()
                .takeUntil(Folyam.just(1))
                .filter(v -> true)
                .test()
                .assertResult();
    }


    @Test
    public void untilErrorConditional() {
        Folyam.never()
                .takeUntil(Folyam.error(new IOException()))
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void mainCompleteCancelsOtherConditional() {
        DirectProcessor<Integer> dp1 = new DirectProcessor<>();
        DirectProcessor<Integer> dp2 = new DirectProcessor<>();

        TestConsumer<Integer> tc = dp1.takeUntil(dp2)
                .filter(v -> true)
                .test();

        assertTrue(dp1.hasSubscribers());
        assertTrue(dp2.hasSubscribers());

        dp1.onComplete();

        tc.assertResult();

        assertFalse(dp1.hasSubscribers());
        assertFalse(dp2.hasSubscribers());
    }


    @Test
    public void mainErrorCancelsOtherConditional() {
        DirectProcessor<Integer> dp1 = new DirectProcessor<>();
        DirectProcessor<Integer> dp2 = new DirectProcessor<>();

        TestConsumer<Integer> tc = dp1.takeUntil(dp2)
                .filter(v -> true)
                .test();

        assertTrue(dp1.hasSubscribers());
        assertTrue(dp2.hasSubscribers());

        dp1.onError(new IOException());

        tc.assertFailure(IOException.class);

        assertFalse(dp1.hasSubscribers());
        assertFalse(dp2.hasSubscribers());
    }

    @Test
    public void otherNextCancelsMainConditional() {
        DirectProcessor<Integer> dp1 = new DirectProcessor<>();
        DirectProcessor<Integer> dp2 = new DirectProcessor<>();

        TestConsumer<Integer> tc = dp1.takeUntil(dp2)
                .filter(v -> true)
                .test();

        assertTrue(dp1.hasSubscribers());
        assertTrue(dp2.hasSubscribers());

        dp2.onNext(1);

        tc.assertResult();

        assertFalse(dp1.hasSubscribers());
        assertFalse(dp2.hasSubscribers());
    }

    @Test
    public void otherErrorCancelsMainConditional() {
        DirectProcessor<Integer> dp1 = new DirectProcessor<>();
        DirectProcessor<Integer> dp2 = new DirectProcessor<>();

        TestConsumer<Integer> tc = dp1.takeUntil(dp2)
                .filter(v -> true)
                .test();

        assertTrue(dp1.hasSubscribers());
        assertTrue(dp2.hasSubscribers());

        dp2.onError(new IOException());

        tc.assertFailure(IOException.class);

        assertFalse(dp1.hasSubscribers());
        assertFalse(dp2.hasSubscribers());
    }

    @Test
    public void otherCompleteCancelsMainConditional() {
        DirectProcessor<Integer> dp1 = new DirectProcessor<>();
        DirectProcessor<Integer> dp2 = new DirectProcessor<>();

        TestConsumer<Integer> tc = dp1.takeUntil(dp2)
                .filter(v -> true)
                .test();

        assertTrue(dp1.hasSubscribers());
        assertTrue(dp2.hasSubscribers());

        dp2.onComplete();

        tc.assertResult();

        assertFalse(dp1.hasSubscribers());
        assertFalse(dp2.hasSubscribers());
    }
}
