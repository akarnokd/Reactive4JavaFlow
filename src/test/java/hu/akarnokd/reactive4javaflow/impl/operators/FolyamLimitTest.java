/*
 * Copyright 2017 David Karnok
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
 * the License for the specific language governing permissions and limitations under the License.
 */

package hu.akarnokd.reactive4javaflow.impl.operators;

import hu.akarnokd.reactive4javaflow.*;
import hu.akarnokd.reactive4javaflow.functionals.*;
import hu.akarnokd.reactive4javaflow.impl.BooleanSubscription;
import hu.akarnokd.reactive4javaflow.processors.DirectProcessor;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

public class FolyamLimitTest implements CheckedConsumer<Long>, CheckedRunnable {

    final List<Long> requests = new ArrayList<>();

    static final Long CANCELLED = -100L;

    @Override
    public void accept(Long t) throws Exception {
        requests.add(t);
    }

    @Override
    public void run() throws Exception {
        requests.add(CANCELLED);
    }

    @Test
    public void shorterSequence() {
        Folyam.range(1, 5)
        .doOnRequest(this)
        .limit(6)
        .test()
        .assertResult(1, 2, 3, 4, 5);

        assertEquals(6, requests.get(0).intValue());
    }

    @Test
    public void exactSequence() {
        Folyam.range(1, 5)
        .doOnRequest(this)
        .doOnCancel(this)
        .limit(5)
        .test()
        .assertResult(1, 2, 3, 4, 5);

        assertEquals(2, requests.size());
        assertEquals(5, requests.get(0).intValue());
        assertEquals(CANCELLED, requests.get(1));
    }

    @Test
    public void longerSequence() {
        Folyam.range(1, 6)
        .doOnRequest(this)
        .limit(5)
        .test()
        .assertResult(1, 2, 3, 4, 5);

        assertEquals(5, requests.get(0).intValue());
    }

    @Test
    public void error() {
        Folyam.error(new IOException())
        .limit(5)
        .test()
        .assertFailure(IOException.class);
    }

    @Test
    public void limitZero() {
        Folyam.range(1, 5)
        .doOnCancel(this)
        .doOnRequest(this)
        .limit(0)
        .test()
        .assertResult();

        assertEquals(1, requests.size());
        assertEquals(CANCELLED, requests.get(0));
    }

    @Test
    public void limitStep() {
        TestConsumer<Integer> ts = Folyam.range(1, 6)
        .doOnRequest(this)
        .limit(5)
        .test(0L);

        assertEquals(0, requests.size());

        ts.requestMore(1);
        ts.assertValues(1);

        ts.requestMore(2);
        ts.assertValues(1, 2, 3);

        ts.requestMore(3);
        ts.assertResult(1, 2, 3, 4, 5);

        assertEquals(Arrays.asList(1L, 2L, 2L), requests);
    }

    @Test
    public void limitAndTake() {
        Folyam.range(1, 5)
        .doOnCancel(this)
        .doOnRequest(this)
        .limit(6)
        .take(5)
        .test()
        .assertResult(1, 2, 3, 4, 5);

        assertEquals(Arrays.asList(6L, CANCELLED), requests);
    }

    @Test
    public void noOverrequest() {
        DirectProcessor<Integer> pp = new DirectProcessor<>();

        TestConsumer<Integer> ts = pp
                .doOnRequest(this)
                .limit(5)
                .test(0L);

        ts.requestMore(5);
        ts.requestMore(10);

        assertTrue(pp.tryOnNext(1));
        pp.onComplete();

        ts.assertResult(1);
    }

    @Test
    public void cancelIgnored() {
        TestHelper.withErrorTracking(errors -> {
            new Folyam<Integer>() {
                @Override
                protected void subscribeActual(FolyamSubscriber<? super Integer> s) {
                    BooleanSubscription bs = new BooleanSubscription();
                    s.onSubscribe(bs);

                    assertTrue(bs.isCancelled());

                    s.onNext(1);
                    s.onComplete();
                    s.onError(new IOException());

                    s.onSubscribe(null);
                }
            }
            .limit(0)
            .test()
            .assertResult();

            TestHelper.assertError(errors, 0, IOException.class);
            TestHelper.assertError(errors, 1, NullPointerException.class);
        });
    }

    @Test
    public void requestRace() {
        for (int i = 0; i < 1000; i++) {
            final TestConsumer<Integer> ts = Folyam.range(1, 10)
                    .limit(5)
                    .test(0L);

            Runnable r = () -> ts.requestMore(3);

            TestHelper.race(r, r);

            ts.assertResult(1, 2, 3, 4, 5);
        }
    }
}