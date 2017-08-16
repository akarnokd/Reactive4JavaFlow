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

package hu.akarnokd.reactive4javaflow.impl;

import hu.akarnokd.reactive4javaflow.*;
import hu.akarnokd.reactive4javaflow.fused.ConditionalSubscriber;
import org.junit.Test;

import java.lang.invoke.*;
import java.util.*;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

public class SubscriptionHelperTest {

    Flow.Subscription d;
    static final VarHandle D;

    long requested;
    static final VarHandle R;

    boolean cancelled;
    static final VarHandle C;

    static {
        try {
            D = MethodHandles.lookup().findVarHandle(SubscriptionHelperTest.class, "d", Flow.Subscription.class);
            R = MethodHandles.lookup().findVarHandle(SubscriptionHelperTest.class, "requested", long.class);
            C = MethodHandles.lookup().findVarHandle(SubscriptionHelperTest.class, "cancelled", boolean.class);
        } catch (Throwable ex) {
            throw new InternalError(ex);
        }
    }

    @Test
    public void closedReplace() {
        AtomicReference<Flow.Subscription> ref = new AtomicReference<>();

        BooleanSubscription d = new BooleanSubscription();

        SubscriptionHelper.cancel(ref);

        assertFalse(SubscriptionHelper.replace(ref, null));

        assertFalse(SubscriptionHelper.replace(ref, d));

        assertTrue(d.isCancelled());
    }

    @Test
    public void replaceRace() {
        for (int i = 0; i < 1000; i++) {
            AtomicReference<Flow.Subscription> ref = new AtomicReference<>();

            BooleanSubscription d1 = new BooleanSubscription();
            BooleanSubscription d2 = new BooleanSubscription();

            Runnable r1 = () -> SubscriptionHelper.replace(ref, d1);
            Runnable r2 = () -> SubscriptionHelper.replace(ref, d2);

            TestHelper.race(r1, r2);
        }
    }

    @Test
    public void replaceRaceVarHandle() {
        for (int i = 0; i < 1000; i++) {

            BooleanSubscription d1 = new BooleanSubscription();
            BooleanSubscription d2 = new BooleanSubscription();

            Runnable r1 = () -> SubscriptionHelper.replace(this, D, d1);
            Runnable r2 = () -> SubscriptionHelper.replace(this, D, d2);

            TestHelper.race(r1, r2);
        }
    }

    @Test
    public void requestProducedVarHandleRace() {
        for (int i = 0; i < 1000; i++) {

            requested = 1;

            Runnable r1 = () -> SubscriptionHelper.addRequested(this, R, 1);
            Runnable r2 = () -> SubscriptionHelper.produced(this, R, 1);

            TestHelper.race(r1, r2);
        }
    }

    @Test
    public void deferredRequestRace() {
        for (int i = 0; i < 50000; i++) {
            BooleanSubscription bs = new BooleanSubscription();

            Runnable r1 = () -> {
                D.setVolatile(this, null);
                D.setVolatile(this, bs);
            };
            Runnable r2 = () -> SubscriptionHelper.deferredRequest(this, D, R, 1);

            TestHelper.race(r1, r2);
        }
    }

    @Test
    public void postCompleteTwice() {
        TestConsumer<Integer> tc = new TestConsumer<>();
        tc.onSubscribe(new BooleanSubscription());

        requested = 1;

        SubscriptionHelper.postComplete(tc, new ArrayDeque<>(), this, R, C);
        SubscriptionHelper.postComplete(tc, new ArrayDeque<>(), this, R, C);

        tc.assertResult();
    }


    @Test
    public void postCompleteRequestRace() {
        for (int i = 0; i < 1000; i++) {
            TestConsumer<Integer> tc = new TestConsumer<>();
            tc.onSubscribe(new BooleanSubscription());
            ArrayDeque<Integer> q = new ArrayDeque<>();

            requested = 0;

            Runnable r1 = () -> SubscriptionHelper.postComplete(tc, q, this, R, C);
            Runnable r2 = () -> SubscriptionHelper.postCompleteRequest(tc, q, this, R, C, 1);

            TestHelper.race(r1, r2);

            tc.assertResult();
        }
    }

    @Test
    public void postCompleteRequestMax() {
        TestConsumer<Integer> tc = new TestConsumer<>();
        tc.onSubscribe(new BooleanSubscription());
        ArrayDeque<Integer> q = new ArrayDeque<>();
        SubscriptionHelper.postCompleteRequest(tc, q, this, R, C, 100);
        SubscriptionHelper.postCompleteRequest(tc, q, this, R, C, Long.MAX_VALUE);
        SubscriptionHelper.postComplete(tc, q, this, R, C);

        tc.assertResult();
    }

    @Test
    public void postCompleteRequest2() {
        TestConsumer<Integer> tc = new TestConsumer<>();
        tc.onSubscribe(new BooleanSubscription());
        ArrayDeque<Integer> q = new ArrayDeque<>();
        SubscriptionHelper.postComplete(tc, q, this, R, C);
        SubscriptionHelper.postCompleteRequest(tc, q, this, R, C, 100);

        tc.assertResult();
    }

    @Test
    public void drainRequestRace() {
        for (int i = 0; i < 1000; i++) {
            TestConsumer<Integer> tc = new TestConsumer<>();
            tc.onSubscribe(new BooleanSubscription());
            ArrayDeque<Integer> q = new ArrayDeque<>(List.of(1, 2, 3));

            requested = 1;

            Runnable r1 = () -> SubscriptionHelper.postComplete(tc, q, this, R, C);
            Runnable r2 = () -> SubscriptionHelper.postCompleteRequest(tc, q, this, R, C, 2);

            TestHelper.race(r1, r2);

            tc.assertResult(1, 2, 3);
        }
    }


    @Test
    public void drainCancelRace() {
        for (int i = 0; i < 1000; i++) {
            TestConsumer<Integer> tc = new TestConsumer<>();
            tc.onSubscribe(new BooleanSubscription());
            ArrayDeque<Integer> q = new ArrayDeque<>(List.of(1, 2, 3));

            cancelled = false;
            requested = 1;

            Runnable r1 = () -> SubscriptionHelper.postComplete(tc, q, this, R, C);
            Runnable r2 = () -> C.setVolatile(this, true);

            TestHelper.race(r1, r2);

            if (tc.values().size() >= 1) {
                tc.assertValues(1);
            } else {
                tc.assertEmpty();
            }
        }
    }

    @Test
    public void cancelAfterFirst() {
        TestConsumer<Integer> tc = new TestConsumer<>() {
            @Override
            public void onNext(Integer item) {
                super.onNext(item);
                C.setVolatile(SubscriptionHelperTest.this, true);
            }
        };
        tc.onSubscribe(new BooleanSubscription());
        ArrayDeque<Integer> q = new ArrayDeque<>(List.of(1, 2, 3));

        requested = 1;
        SubscriptionHelper.postComplete(tc, q, this, R, C);

        tc.assertValues(1);
    }

    @Test
    public void postCompleteTwiceConditional() {
        TestConsumer<Integer> tc = new TestConsumer<>();
        tc.onSubscribe(new BooleanSubscription());

        requested = 1;
        ConditionalSubscriber<Integer> tcc = wrap(tc);

        SubscriptionHelper.postComplete(tcc, new ArrayDeque<>(), this, R, C);
        SubscriptionHelper.postComplete(tcc, new ArrayDeque<>(), this, R, C);

        tc.assertResult();
    }


    @Test
    public void postCompleteRequestRaceConditional() {
        for (int i = 0; i < 1000; i++) {
            TestConsumer<Integer> tc = new TestConsumer<>();
            tc.onSubscribe(new BooleanSubscription());
            ArrayDeque<Integer> q = new ArrayDeque<>();

            requested = 0;
            ConditionalSubscriber<Integer> tcc = wrap(tc);

            Runnable r1 = () -> SubscriptionHelper.postComplete(tcc, q, this, R, C);
            Runnable r2 = () -> SubscriptionHelper.postCompleteRequest(tcc, q, this, R, C, 1);

            TestHelper.race(r1, r2);

            tc.assertResult();
        }
    }

    @Test
    public void postCompleteRequestMaxConditional() {
        TestConsumer<Integer> tc = new TestConsumer<>();
        tc.onSubscribe(new BooleanSubscription());
        ArrayDeque<Integer> q = new ArrayDeque<>();

        ConditionalSubscriber<Integer> tcc = wrap(tc);

        SubscriptionHelper.postCompleteRequest(tcc, q, this, R, C, 100);
        SubscriptionHelper.postCompleteRequest(tcc, q, this, R, C, Long.MAX_VALUE);
        SubscriptionHelper.postComplete(tcc, q, this, R, C);

        tc.assertResult();
    }

    @Test
    public void postCompleteRequest2Conditional() {
        TestConsumer<Integer> tc = new TestConsumer<>();
        tc.onSubscribe(new BooleanSubscription());
        ArrayDeque<Integer> q = new ArrayDeque<>();

        ConditionalSubscriber<Integer> tcc = wrap(tc);

        SubscriptionHelper.postComplete(tcc, q, this, R, C);
        SubscriptionHelper.postCompleteRequest(tcc, q, this, R, C, 100);

        tc.assertResult();
    }

    @Test
    public void drainRequestRaceConditional() {
        for (int i = 0; i < 1000; i++) {
            TestConsumer<Integer> tc = new TestConsumer<>();
            tc.onSubscribe(new BooleanSubscription());
            ArrayDeque<Integer> q = new ArrayDeque<>(List.of(1, 2, 3));

            requested = 1;
            ConditionalSubscriber<Integer> tcc = wrap(tc);

            Runnable r1 = () -> SubscriptionHelper.postComplete(tcc, q, this, R, C);
            Runnable r2 = () -> SubscriptionHelper.postCompleteRequest(tcc, q, this, R, C, 2);

            TestHelper.race(r1, r2);

            tc.assertResult(1, 2, 3);
        }
    }


    @Test
    public void drainCancelRaceConditional() {
        for (int i = 0; i < 1000; i++) {
            TestConsumer<Integer> tc = new TestConsumer<>();
            tc.onSubscribe(new BooleanSubscription());
            ArrayDeque<Integer> q = new ArrayDeque<>(List.of(1, 2, 3));

            ConditionalSubscriber<Integer> tcc = wrap(tc);

            cancelled = false;
            requested = 1;

            Runnable r1 = () -> SubscriptionHelper.postComplete(tcc, q, this, R, C);
            Runnable r2 = () -> C.setVolatile(this, true);

            TestHelper.race(r1, r2);

            if (tc.values().size() >= 1) {
                tc.assertValues(1);
            } else {
                tc.assertEmpty();
            }
        }
    }

    @Test
    public void cancelAfterFirstConditional() {
        TestConsumer<Integer> tc = new TestConsumer<>() {
            @Override
            public void onNext(Integer item) {
                super.onNext(item);
                C.setVolatile(SubscriptionHelperTest.this, true);
            }
        };
        tc.onSubscribe(new BooleanSubscription());
        ArrayDeque<Integer> q = new ArrayDeque<>(List.of(1, 2, 3));

        requested = 1;
        SubscriptionHelper.postComplete(wrap(tc), q, this, R, C);

        tc.assertValues(1);
    }

    static <T> ConditionalSubscriber<T> wrap(FolyamSubscriber<T> a) {
        return new ConditionalSubscriber<>() {
            @Override
            public boolean tryOnNext(T item) {
                a.onNext(item);
                return true;
            }

            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                a.onSubscribe(subscription);
            }

            @Override
            public void onNext(T item) {
                a.onNext(item);
            }

            @Override
            public void onError(Throwable throwable) {
                a.onError(throwable);
            }

            @Override
            public void onComplete() {
                a.onComplete();
            }
        };
    }
}
