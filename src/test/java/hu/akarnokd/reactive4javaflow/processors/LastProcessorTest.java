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

import hu.akarnokd.reactive4javaflow.Folyam;
import hu.akarnokd.reactive4javaflow.TestConsumer;
import hu.akarnokd.reactive4javaflow.TestHelper;
import hu.akarnokd.reactive4javaflow.impl.BooleanSubscription;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class LastProcessorTest {

    @Test
    public void standard() {
        LastProcessor<Integer> p = new LastProcessor<>();
        p.onNext(1);
        p.onComplete();
        TestHelper.assertResult(p, 1);
    }

    @Test
    public void normal() {
        LastProcessor<Integer> p = new LastProcessor<>();

        TestConsumer<Integer> tc = p.test();

        assertTrue(p.hasSubscribers());

        p.onNext(1);

        tc.assertEmpty();

        p.onComplete();

        assertFalse(p.hasSubscribers());

        tc.assertResult(1);

        p.test().assertResult(1);
    }

    @Test
    public void cancel() {
        LastProcessor<Integer> p = new LastProcessor<>();

        TestConsumer<Integer> tc = p.test();

        assertTrue(p.hasSubscribers());

        p.onNext(1);

        tc.assertEmpty();

        tc.cancel();

        assertFalse(p.hasSubscribers());

        p.onComplete();

        tc.assertEmpty();

        p.test().assertResult(1);

        assertFalse(p.hasSubscribers());
        assertTrue(p.hasComplete());
        assertFalse(p.hasThrowable());
        assertNull(p.getThrowable());
    }


    @Test
    public void error() {
        LastProcessor<Integer> p = new LastProcessor<>();

        TestConsumer<Integer> tc = p.test();

        assertTrue(p.hasSubscribers());

        p.onNext(1);

        tc.assertEmpty();

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

        LastProcessor<Integer> p = new LastProcessor<>();

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
            LastProcessor<Integer> p = new LastProcessor<>();

            Folyam.range(1, 5).subscribe(p);

            p.test().assertResult(5);

            BooleanSubscription bs = new BooleanSubscription();
            p.onSubscribe(bs);
            assertTrue(bs.isCancelled());

            p.onNext(6);
            p.onError(new IOException());
            p.onComplete();

            p.test().assertResult(5);

            TestHelper.assertError(errors, 0, IOException.class);
        });
    }
}
