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

public class FirstProcessorTest {

    @Test
    public void standard() {
        FirstProcessor<Integer> p = new FirstProcessor<>();
        p.onNext(1);
        p.onComplete();
        TestHelper.assertResult(p, 1);
    }

    @Test
    public void normal() {
        FirstProcessor<Integer> p = new FirstProcessor<>();

        assertFalse(p.hasValue());
        assertNull(p.getValue());

        TestConsumer<Integer> tc = p.test();

        assertTrue(p.hasSubscribers());

        p.onNext(1);

        tc.assertResult(1);

        assertFalse(p.hasSubscribers());

        p.onComplete();

        p.test().assertResult(1);

        TestHelper.withErrorTracking(errors -> {
            p.onNext(2);

            p.test().assertResult(1);

            p.onError(new IOException());

            p.test().assertResult(1);

            p.onComplete();

            p.test().assertResult(1);

            TestHelper.assertError(errors, 0, IOException.class);
        });

        assertTrue(p.hasValue());
        assertEquals(1, p.getValue().intValue());
    }

    @Test
    public void cancel() {
        FirstProcessor<Integer> p = new FirstProcessor<>();

        TestConsumer<Integer> tc = p.test();

        assertTrue(p.hasSubscribers());

        tc.cancel();

        assertFalse(p.hasSubscribers());

        p.onComplete();

        tc.assertEmpty();

        p.test().assertResult();

        assertFalse(p.hasSubscribers());
        assertTrue(p.hasComplete());
        assertFalse(p.hasThrowable());
        assertNull(p.getThrowable());
    }


    @Test
    public void error() {
        FirstProcessor<Integer> p = new FirstProcessor<>();

        TestConsumer<Integer> tc = p.test();

        assertTrue(p.hasSubscribers());

        p.onError(new IOException());

        assertFalse(p.hasSubscribers());

        tc.assertFailure(IOException.class);

        p.test().assertFailure(IOException.class);

        assertFalse(p.hasSubscribers());
        assertFalse(p.hasComplete());
        assertTrue(p.hasThrowable());
        assertNotNull(p.getThrowable());
    }

    @Test
    public void empty() {

        FirstProcessor<Integer> p = new FirstProcessor<>();

        TestConsumer<Integer> tc = p.test();

        assertTrue(p.hasSubscribers());

        TestConsumer<Integer> tc2 = p.test();

        assertTrue(p.hasSubscribers());

        TestConsumer<Integer> tc3 = p.test();

        tc2.cancel();

        p.onComplete();

        tc.assertResult();
        tc2.assertEmpty();
        tc3.assertResult();

        p.test().assertResult();
    }

    @Test
    public void consumeOther() {
        TestHelper.withErrorTracking(errors -> {
            FirstProcessor<Integer> p = new FirstProcessor<>();

            Folyam.range(1, 5).subscribe(p);

            p.test().assertResult(1);

            BooleanSubscription bs = new BooleanSubscription();
            p.onSubscribe(bs);
            assertTrue(bs.isCancelled());

            p.onNext(6);
            p.onError(new IOException());
            p.onComplete();

            p.test().assertResult(1);

            TestHelper.assertError(errors, 0, IOException.class);
        });
    }

    @Test
    public void onSubscribe() {
        DirectProcessor<Integer> dp = new DirectProcessor<>();

        FirstProcessor<Integer> ep = new FirstProcessor<>();

        dp.subscribe(ep);

        BooleanSubscription bs = new BooleanSubscription();

        ep.onSubscribe(bs);

        assertTrue(bs.isCancelled());

        TestConsumer<Integer> tc = ep.test();

        tc.assertEmpty();

        dp.onNext(1);

        assertFalse(dp.hasSubscribers());
        assertFalse(ep.hasSubscribers());

        tc.assertResult(1);
    }

    @Test
    public void cancelUpfront() {
        FirstProcessor<Integer> ep = new FirstProcessor<>();

        TestConsumer<Integer> tc = ep.test();
        ep.test(0, true, 0);

        tc.cancel();

        assertFalse(ep.hasSubscribers());
    }

    @Test
    public void toSerializedIsSelf() {
        FirstProcessor<Integer> ep = new FirstProcessor<>();

        assertSame(ep, ep.toSerialized());
    }

    @Test
    public void addRemoveRace() {
        for (int i = 0; i < 1000; i++) {
            FirstProcessor<Integer> ep = new FirstProcessor<>();

            TestConsumer<Integer> tc1 = ep.test();
            TestConsumer<Integer> tc2 = ep.test();
            TestConsumer<Integer> tc3 = new TestConsumer<>();

            Runnable r1 = tc1::cancel;
            Runnable r2 = () -> ep.subscribe(tc3);

            TestHelper.race(r1, r2);

            ep.onNext(1);

            tc1.assertEmpty();
            tc2.assertResult(1);
            tc3.assertResult(1);
        }
    }
}
