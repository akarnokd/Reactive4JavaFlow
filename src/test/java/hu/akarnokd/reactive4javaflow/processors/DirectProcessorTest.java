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

package hu.akarnokd.reactive4javaflow.processors;

import hu.akarnokd.reactive4javaflow.*;
import hu.akarnokd.reactive4javaflow.impl.BooleanSubscription;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class DirectProcessorTest {

    @Test
    public void normal() {
        DirectProcessor<Integer> dp = new DirectProcessor<>();

        assertFalse(dp.hasSubscribers());
        assertFalse(dp.hasComplete());
        assertFalse(dp.hasThrowable());
        assertNull(dp.getThrowable());

        TestConsumer<Integer> tc = dp.test();

        assertTrue(dp.hasSubscribers());

        tc.assertEmpty();

        for (int i = 1; i < 6; i++) {
            dp.onNext(i);
        }
        dp.onComplete();

        tc.assertResult(1, 2, 3, 4, 5);

        dp.test().assertResult();

        assertFalse(dp.hasSubscribers());
        assertTrue(dp.hasComplete());
        assertFalse(dp.hasThrowable());
        assertNull(dp.getThrowable());

        dp.test(0, true, 0).assertEmpty();
    }

    @Test
    public void error() {
        DirectProcessor<Integer> dp = new DirectProcessor<>();

        assertFalse(dp.hasSubscribers());
        assertFalse(dp.hasComplete());
        assertFalse(dp.hasThrowable());
        assertNull(dp.getThrowable());

        TestConsumer<Integer> tc = dp.test();

        tc.assertEmpty();

        for (int i = 1; i < 6; i++) {
            dp.onNext(i);
        }
        dp.onError(new IOException());

        tc.assertFailure(IOException.class, 1, 2, 3, 4, 5);

        assertFalse(dp.hasSubscribers());
        assertFalse(dp.hasComplete());
        assertTrue(dp.hasThrowable());
        assertTrue("" + dp.getThrowable(), dp.getThrowable() instanceof IOException);

        dp.test().assertFailure(IOException.class);

        dp.test(0, true, 0).assertEmpty();
    }

    @Test
    public void offer() {
        DirectProcessor<Integer> dp = new DirectProcessor<>();

        TestConsumer<Integer> tc = dp.test(0);

        tc.assertEmpty();

        assertFalse(dp.tryOnNext(1));

        tc.assertEmpty();

        tc.requestMore(1);

        assertTrue(dp.tryOnNext(1));

        tc.assertValues(1)
                .assertNotComplete()
                .assertNoErrors();

        dp.onComplete();

        tc.assertResult(1);

        dp.test().assertResult();
    }

    @Test
    public void onSubscribe() {
        DirectProcessor<Integer> dp = new DirectProcessor<>();

        Folyam.range(1, 5).subscribe(dp);

        assertTrue(dp.hasComplete());

        BooleanSubscription bs = new BooleanSubscription();

        dp.onSubscribe(bs);

        assertTrue(bs.isCancelled());
    }

    @Test
    public void doubleOnError() {
        TestHelper.withErrorTracking(errors -> {
            DirectProcessor<Integer> dp = new DirectProcessor<>();

            dp.onError(new IOException());

            dp.test().assertFailure(IOException.class);

            dp.onError(new IllegalArgumentException("Forced failure"));

            dp.test().assertFailure(IOException.class);

            TestHelper.assertError(errors, 0, IllegalArgumentException.class, "Forced failure");
        });
    }

    @Test
    public void multipleSubscribers() {
        DirectProcessor<Integer> dp = new DirectProcessor<>();

        TestConsumer<Integer> tc1 = dp.test();
        TestConsumer<Integer> tc2 = dp.test();

        dp.onNext(1);

        tc1.cancel();

        dp.onNext(2);

        tc2.cancel();

        assertTrue(dp.tryOnNext(3));
        dp.onComplete();

        tc1.assertOnSubscribe()
                .assertValues(1)
                .assertNoErrors()
                .assertNotComplete();

        tc2.assertOnSubscribe()
                .assertValues(1, 2)
                .assertNoErrors()
                .assertNotComplete();
    }

    @Test
    public void overflow() {
        DirectProcessor<Integer> dp = new DirectProcessor<>();

        TestConsumer<Integer> tc1 = dp.test(0);

        assertTrue(dp.hasSubscribers());

        dp.onNext(1);
        dp.onNext(2);

        tc1.assertFailure(IllegalStateException.class);

        assertFalse(dp.hasSubscribers());

    }

    @Test
    public void subscriberRace() {
        for (int i = 0; i < 1000; i++) {
            DirectProcessor<Integer> dp = new DirectProcessor<>();

            TestConsumer<Integer> tc1 = new TestConsumer<>();
            TestConsumer<Integer> tc2 = new TestConsumer<>();

            Runnable r1 = () -> dp.subscribe(tc1);
            Runnable r2 = () -> dp.subscribe(tc2);

            TestHelper.race(r1, r2);

            dp.onNext(1);
            dp.onComplete();

            tc1.assertResult(1);
            tc2.assertResult(1);
        }
    }


    @Test
    public void completeCancelRace() {
        for (int i = 0; i < 1000; i++) {
            DirectProcessor<Integer> dp = new DirectProcessor<>();

            TestConsumer<Integer> tc1 = dp.test();

            Runnable r1 = dp::onComplete;
            Runnable r2 = tc1::cancel;

            TestHelper.race(r1, r2);
        }
    }
}
