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
import hu.akarnokd.reactive4javaflow.fused.FusedSubscription;
import hu.akarnokd.reactive4javaflow.impl.BooleanSubscription;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class ProcessorRefCountTest {

    @Test
    public void verify() {
        for (FlowProcessorSupport<Integer> fp : new FlowProcessorSupport[] {
            new DirectProcessor<Integer>(),
            new CachingProcessor<Integer>(),
            new MulticastProcessor<Integer>(),
            new LastProcessor<Integer>(),
            new FirstProcessor<Integer>(),
            new SolocastProcessor<Integer>()
        }) {

            FlowProcessorSupport<Integer> rc = fp.refCount().refCount();

            BooleanSubscription bs = new BooleanSubscription();
            rc.onSubscribe(bs);

            assertFalse(bs.isCancelled());

            assertFalse(rc.hasSubscribers());
            assertFalse(rc.hasComplete());
            assertFalse(rc.hasThrowable());
            assertNull(rc.getThrowable());

            TestConsumer<Integer> tc = new TestConsumer<>();

            rc.subscribe(tc);

            assertTrue(rc.hasSubscribers());
            assertFalse(rc.hasComplete());
            assertFalse(rc.hasThrowable());
            assertNull(rc.getThrowable());

            tc.cancel();

            assertTrue(bs.isCancelled());

            assertFalse(rc.hasSubscribers());
            assertFalse(rc.hasComplete());
            assertFalse(rc.hasThrowable());
            assertNull(rc.getThrowable());
        }
    }


    @Test
    public void verifyFused() {
        for (FlowProcessorSupport<Integer> fp : new FlowProcessorSupport[] {
                new DirectProcessor<Integer>(),
                new CachingProcessor<Integer>(),
                new MulticastProcessor<Integer>(),
                new LastProcessor<Integer>(),
                new FirstProcessor<Integer>(),
                new SolocastProcessor<Integer>()
        }) {

            FlowProcessorSupport<Integer> rc = fp.refCount().refCount();

            BooleanSubscription bs = new BooleanSubscription();
            rc.onSubscribe(bs);

            assertFalse(bs.isCancelled());

            assertFalse(rc.hasSubscribers());
            assertFalse(rc.hasComplete());
            assertFalse(rc.hasThrowable());
            assertNull(rc.getThrowable());

            TestConsumer<Integer> tc = new TestConsumer<>();
            tc.requestFusionMode(FusedSubscription.ANY);

            rc.subscribe(tc);

            assertTrue(rc.hasSubscribers());
            assertFalse(rc.hasComplete());
            assertFalse(rc.hasThrowable());
            assertNull(rc.getThrowable());

            tc.cancel();

            assertTrue(bs.isCancelled());

            assertFalse(rc.hasSubscribers());
            assertFalse(rc.hasComplete());
            assertFalse(rc.hasThrowable());
            assertNull(rc.getThrowable());
        }
    }

    @Test
    public void directRefCountNormal() {
        DirectProcessor<Integer> dp = new DirectProcessor<>();
        FolyamProcessor<Integer> rp = dp.refCount();

        BooleanSubscription bs1 = new BooleanSubscription();

        rp.onSubscribe(bs1);

        BooleanSubscription bs2 = new BooleanSubscription();

        rp.onSubscribe(bs2);

        assertFalse(bs1.isCancelled());
        assertTrue(bs2.isCancelled());

        TestConsumer<Integer> tc = rp.test(10, false, FusedSubscription.SYNC);

        tc.assertFusionMode(FusedSubscription.NONE);

        rp.onNext(1);

        tc.assertValues(1);

        rp.onNext(2);

        tc.assertValues(1, 2);

        rp.onNext(3);

        tc.assertValues(1, 2, 3);

        rp.onComplete();

        tc.assertResult(1, 2, 3);

        rp.test().assertResult();
    }


    @Test
    public void directRefCountError() {
        DirectProcessor<Integer> dp = new DirectProcessor<>();
        FolyamProcessor<Integer> rp = dp.refCount();

        BooleanSubscription bs1 = new BooleanSubscription();

        rp.onSubscribe(bs1);

        BooleanSubscription bs2 = new BooleanSubscription();

        rp.onSubscribe(bs2);

        assertFalse(bs1.isCancelled());
        assertTrue(bs2.isCancelled());

        TestConsumer<Integer> tc = rp.test(10, false, FusedSubscription.SYNC);

        tc.assertFusionMode(FusedSubscription.NONE);

        rp.onNext(1);

        tc.assertValues(1);

        rp.onNext(2);

        tc.assertValues(1, 2);

        rp.onNext(3);

        tc.assertValues(1, 2, 3);

        rp.onError(new IOException());

        tc.assertFailure(IOException.class, 1, 2, 3);

        rp.test().assertFailure(IOException.class);
    }

    @Test
    public void replayRefCountFused() {
        CachingProcessor<Integer> cp = new CachingProcessor<>();
        FolyamProcessor<Integer> rp = cp.refCount();

        TestConsumer<Integer> tc = rp.test(10, false, FusedSubscription.ANY);

        rp.test().cancel();

        rp.onNext(1);
        rp.onNext(2);
        rp.onNext(3);
        rp.onComplete();

        tc.assertResult(1, 2, 3);

        rp.test().assertResult();
    }


    @Test
    public void esetlegFused() {
        FirstProcessor<Integer> cp = new FirstProcessor<>();
        EsetlegProcessor<Integer> rp = cp.refCount();

        BooleanSubscription bs1 = new BooleanSubscription();

        rp.onSubscribe(bs1);

        BooleanSubscription bs2 = new BooleanSubscription();

        rp.onSubscribe(bs2);

        assertFalse(bs1.isCancelled());
        assertTrue(bs2.isCancelled());


        TestConsumer<Integer> tc = rp.test(10, false, FusedSubscription.ANY);

        rp.test().cancel();

        rp.onNext(1);

        tc.assertResult(1);

        rp.test().assertResult();
    }

    @Test
    public void esetlegError() {
        FirstProcessor<Integer> cp = new FirstProcessor<>();
        EsetlegProcessor<Integer> rp = cp.refCount();

        TestConsumer<Integer> tc = rp.test(10, false, FusedSubscription.ANY);

        rp.test().cancel();

        rp.onError(new IOException());

        tc.assertFailure(IOException.class);

        rp.test().assertFailure(IOException.class);
    }

    @Test
    public void esetlegComplete() {
        FirstProcessor<Integer> cp = new FirstProcessor<>();
        EsetlegProcessor<Integer> rp = cp.refCount();

        TestConsumer<Integer> tc = rp.test(10, false, FusedSubscription.ANY);

        rp.onComplete();

        tc.assertResult();

        rp.test().assertResult();
    }

    @Test
    public void addRemoveRace() {
        for (int i = 0; i < 1000; i++) {
            for (FlowProcessorSupport<Integer> fp : new FlowProcessorSupport[] {
                    new DirectProcessor<Integer>(),
                    new CachingProcessor<Integer>(),
                    new MulticastProcessor<Integer>(),
                    new LastProcessor<Integer>(),
                    new FirstProcessor<Integer>()
            }) {
                FlowProcessorSupport<Integer> rp = fp.refCount();

                rp.onSubscribe(new BooleanSubscription());

                TestConsumer<Integer> tc1 = new TestConsumer<>();
                TestConsumer<Integer> tc2 = new TestConsumer<>();
                TestConsumer<Integer> tc3 = new TestConsumer<>();

                rp.subscribe(tc1);
                rp.subscribe(tc2);

                Runnable r1 = () -> rp.subscribe(tc3);
                Runnable r2 = tc2::cancel;

                TestHelper.race(r1, r2);

                rp.onNext(1);
                rp.onComplete();

                tc1.assertResult(1);
                tc2.assertEmpty();
                tc3.assertResult(1);
            }
        }
    }


    @Test
    public void fusedIsEmptyClear() {
        CachingProcessor<Integer> cp = new CachingProcessor<>();
        FolyamProcessor<Integer> rp = cp.refCount();

        TestConsumer<Integer> tc = rp
                .map(v -> {
                    if (v == 2) {
                        throw new IOException();
                    }
                    return v;
                })
                .test(0, false, FusedSubscription.ANY);

        rp.onNext(1);
        tc.requestMore(1);

        rp.onNext(2);

        tc.assertFailure(IOException.class, 1);

        rp.test().assertResult();
    }

    @Test
    public void fusedAsync() {
        CachingProcessor<Integer> cp = new CachingProcessor<>();
        FolyamProcessor<Integer> rp = cp.refCount();

        TestConsumer<Integer> tc = rp.observeOn(SchedulerServices.trampoline(), 1)
        .test(0);

        rp.onComplete();

        tc.assertResult();
    }
}
