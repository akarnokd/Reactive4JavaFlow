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

import hu.akarnokd.reactive4javaflow.TestHelper;
import org.junit.Test;

import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;

public class SubscriptionArbiterTest {

    @Test
    public void cancelAndReplace() {
        SubscriptionArbiter sa = new SubscriptionArbiter();

        assertFalse(sa.arbiterIsCancelled());

        sa.cancel();

        assertTrue(sa.arbiterIsCancelled());

        sa.request(1);
        sa.arbiterProduced(1);

        sa.arbiterProduced(1);

        assertEquals(0, sa.requested);

        BooleanSubscription b1 = new BooleanSubscription();

        sa.arbiterReplace(b1);

        assertTrue(b1.isCancelled());
    }

    static final class RequestTracker extends AtomicLong implements Flow.Subscription {

        volatile boolean cancelled;

        @Override
        public void request(long n) {
            SubscriptionHelper.addRequested(this, n);
        }

        @Override
        public void cancel() {
            cancelled = true;
        }

        public boolean isCancelled() {
            return cancelled;
        }
    }

    @Test
    public void arbiterReplaceRequestRace() {
        for (int i = 0; i < 1000; i++) {
            SubscriptionArbiter sa = new SubscriptionArbiter();

            RequestTracker r  = new RequestTracker();

            Runnable r1 = () -> sa.arbiterReplace(r);

            Runnable r2 = () -> sa.request(1);

            TestHelper.race(r1, r2);

            assertEquals(1L, r.get());
        }
    }

    @Test
    public void arbiterReplaceProducedRace() {
        for (int i = 0; i < 1000; i++) {
            SubscriptionArbiter sa = new SubscriptionArbiter();

            RequestTracker r  = new RequestTracker();

            sa.request(1);

            Runnable r1 = () -> sa.arbiterReplace(r);

            Runnable r2 = () -> sa.arbiterProduced(1);

            TestHelper.race(r1, r2);

            assertEquals(0, sa.requested);
        }
    }

    @Test
    public void arbiterRequestRace() {
        for (int i = 0; i < 1000; i++) {
            SubscriptionArbiter sa = new SubscriptionArbiter();

            Runnable r1 = () -> sa.request(1);

            Runnable r2 = () -> sa.request(1);

            TestHelper.race(r1, r2);

            assertEquals(2L, sa.requested);
        }
    }

    @Test
    public void arbiterRequestCancelRace() {
        for (int i = 0; i < 1000; i++) {
            SubscriptionArbiter sa = new SubscriptionArbiter();

            Runnable r1 = () -> sa.request(1);

            Runnable r2 = sa::cancel;

            TestHelper.race(r1, r2);
        }
    }

    @Test
    public void cancelRequests() {
        SubscriptionArbiter sa = new SubscriptionArbiter();

        sa.arbiterReplace(new Flow.Subscription() {

            @Override
            public void request(long n) {
            }

            @Override
            public void cancel() {
                sa.request(1);
            }
        });

        sa.cancel();
    }


    @Test
    public void arbiterReplaceCancelRace() {
        for (int i = 0; i < 1000; i++) {
            SubscriptionArbiter sa = new SubscriptionArbiter();

            RequestTracker r  = new RequestTracker();

            Runnable r1 = () -> sa.arbiterReplace(r);

            Runnable r2 = sa::cancel;

            TestHelper.race(r1, r2);

            assertTrue(r.isCancelled());
        }
    }

    @Test
    public void requestAddition() {

        for (int i = 0; i < 1000; i++) {
            SubscriptionArbiter sa = new SubscriptionArbiter();

            Runnable r1 = () -> sa.request(Long.MAX_VALUE / 2);

            Runnable r2 = () -> sa.request(Long.MAX_VALUE / 2 + 2);

            TestHelper.race(r1, r2);

            assertEquals(Long.MAX_VALUE, sa.requested);
        }
    }


    @Test
    public void requestAddition2() {

        for (int i = 0; i < 1000; i++) {
            SubscriptionArbiter sa = new SubscriptionArbiter();

            sa.setPlain(1);

            Runnable r1 = () -> {
                sa.request(Long.MAX_VALUE / 2);
                sa.arbiterDrainLoop();
            };

            Runnable r2 = () -> sa.request(Long.MAX_VALUE / 2 + 2);

            TestHelper.race(r1, r2);

            assertEquals(Long.MAX_VALUE, sa.requested);
        }
    }
}
