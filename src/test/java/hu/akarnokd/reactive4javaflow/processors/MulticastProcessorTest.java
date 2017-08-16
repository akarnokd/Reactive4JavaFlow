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
import hu.akarnokd.reactive4javaflow.impl.FailingFusedSubscription;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CancellationException;

import static org.junit.Assert.*;

public class MulticastProcessorTest {

    @Test
    public void normal() {
        MulticastProcessor<Integer> mp = new MulticastProcessor<>(4);

        assertFalse(mp.hasSubscribers());
        assertFalse(mp.hasComplete());
        assertFalse(mp.hasThrowable());
        assertNull(mp.getThrowable());

        mp.start();

        mp.onNext(1);
        mp.onNext(2);
        assertTrue(mp.tryOnNext(3));
        assertTrue(mp.tryOnNext(4));
        assertFalse(mp.tryOnNext(5));
        mp.onComplete();

        TestConsumer<Integer> tc1 = mp.test(0);
        TestConsumer<Integer> tc2 = mp.test(0);

        assertTrue(mp.hasSubscribers());

        tc1.requestMore(1);

        tc1.assertEmpty();
        tc2.assertEmpty();

        tc2.requestMore(2);

        tc1.assertValues(1);
        tc2.assertValues(1);

        tc1.requestMore(3);
        tc2.requestMore(2);

        tc1.assertResult(1, 2, 3, 4);
        tc2.assertResult(1, 2, 3, 4);

        assertFalse(mp.hasSubscribers());
        assertTrue(mp.hasComplete());
        assertFalse(mp.hasThrowable());
        assertNull(mp.getThrowable());

        mp.test().assertResult();
    }

    @Test
    public void error() {
        MulticastProcessor<Integer> mp = new MulticastProcessor<>(4);

        assertFalse(mp.hasSubscribers());
        assertFalse(mp.hasComplete());
        assertFalse(mp.hasThrowable());
        assertNull(mp.getThrowable());

        mp.start();

        mp.onError(new IOException());

        assertFalse(mp.hasSubscribers());
        assertFalse(mp.hasComplete());
        assertTrue(mp.hasThrowable());
        assertTrue("" + mp.getThrowable(), mp.getThrowable() instanceof IOException);

        mp.test().assertFailure(IOException.class);
    }

    @Test
    public void syncFusedSource() {
        MulticastProcessor<Integer> mp = new MulticastProcessor<>(4);

        Folyam.range(1, 5).subscribe(mp);

        TestConsumer<Integer> tc1 = mp.test(0);
        TestConsumer<Integer> tc2 = mp.test(0);

        assertTrue(mp.hasSubscribers());

        tc1.requestMore(1);

        tc1.assertEmpty();
        tc2.assertEmpty();

        tc2.requestMore(2);

        tc1.assertValues(1);
        tc2.assertValues(1);

        tc1.requestMore(4);
        tc2.requestMore(3);

        tc1.assertResult(1, 2, 3, 4, 5);
        tc2.assertResult(1, 2, 3, 4, 5);

        assertFalse(mp.hasSubscribers());
        assertTrue(mp.hasComplete());
        assertFalse(mp.hasThrowable());
        assertNull(mp.getThrowable());

        mp.test().assertResult();
    }


    @Test
    public void asyncFusedSource() {
        MulticastProcessor<Integer> mp = new MulticastProcessor<>(4);

        SolocastProcessor<Integer> sp = new SolocastProcessor<>();

        Folyam.range(1, 5).subscribe(sp);

        sp.subscribe(mp);

        TestConsumer<Integer> tc1 = mp.test(0);
        TestConsumer<Integer> tc2 = mp.test(0);

        assertTrue(mp.hasSubscribers());

        tc1.requestMore(1);

        tc1.assertEmpty();
        tc2.assertEmpty();

        tc2.requestMore(2);

        tc1.assertValues(1);
        tc2.assertValues(1);

        tc1.requestMore(4);
        tc2.requestMore(3);

        tc1.assertResult(1, 2, 3, 4, 5);
        tc2.assertResult(1, 2, 3, 4, 5);

        assertFalse(mp.hasSubscribers());
        assertTrue(mp.hasComplete());
        assertFalse(mp.hasThrowable());
        assertNull(mp.getThrowable());

        mp.test().assertResult();
    }

    @Test
    public void cancelUnblocksTheOthers() {
        MulticastProcessor<Integer> mp = new MulticastProcessor<>(4);

        mp.start();

        mp.onNext(1);
        mp.onNext(2);
        mp.onNext(3);
        mp.onNext(4);

        TestConsumer<Integer> tc1 = mp.test(0);
        TestConsumer<Integer> tc2 = mp.test(0);
        TestConsumer<Integer> tc3 = mp.test(0);

        tc1.requestMore(1);
        tc3.requestMore(1);

        tc2.cancel();

        tc1.assertValues(1);
        tc2.assertEmpty();
        tc3.assertValues(1);
    }

    @Test
    public void syncFusedCrash() {
        MulticastProcessor<Integer> mp = new MulticastProcessor<>();

        new Folyam<Integer>() {

            @Override
            protected void subscribeActual(FolyamSubscriber<? super Integer> s) {
                s.onSubscribe(new FailingFusedSubscription(FusedSubscription.SYNC));
            }
        }
                .subscribe(mp);

        mp.test().assertFailure(IOException.class);

        assertFalse(mp.hasSubscribers());
        assertFalse(mp.hasComplete());
        assertTrue(mp.hasThrowable());
        assertTrue("" + mp.getThrowable(), mp.getThrowable() instanceof IOException);
    }


    @Test
    public void asyncFusedCrash() {
        MulticastProcessor<Integer> mp = new MulticastProcessor<>();

        new Folyam<Integer>() {

            @Override
            protected void subscribeActual(FolyamSubscriber<? super Integer> s) {
                s.onSubscribe(new FailingFusedSubscription(FusedSubscription.ASYNC));
            }
        }
                .subscribe(mp);

        mp.test().assertFailure(IOException.class);

        assertFalse(mp.hasSubscribers());
        assertFalse(mp.hasComplete());
        assertTrue(mp.hasThrowable());
        assertTrue("" + mp.getThrowable(), mp.getThrowable() instanceof IOException);
    }

    @Test
    public void cancelUpfront() {
        MulticastProcessor<Integer> mp = new MulticastProcessor<>(1);

        mp.start();

        mp.test(0, true, 0)
                .assertEmpty();
    }


    @Test
    public void overflow() {
        MulticastProcessor<Integer> mp = new MulticastProcessor<>(1);

        mp.start();

        mp.onNext(1);
        mp.onNext(2);

        assertFalse(mp.hasSubscribers());
        assertFalse(mp.hasComplete());
        assertTrue(mp.hasThrowable());
        assertTrue("" + mp.getThrowable(), mp.getThrowable() instanceof IllegalStateException);

        mp.test()
                .assertFailure(IllegalStateException.class, 1);
    }


    @Test(expected = IllegalStateException.class)
    public void tryOnNextFail() {
        MulticastProcessor<Integer> mp = new MulticastProcessor<>(1);

        new SolocastProcessor<Integer>().subscribe(mp);

        mp.tryOnNext(1);
    }

    @Test
    public void alreadyDone() {
        TestHelper.withErrorTracking(errors -> {
            MulticastProcessor<Integer> mp = new MulticastProcessor<>(1);
            mp.start();

            mp.onComplete();
            mp.onError(new IOException());
            mp.onNext(1);
            mp.tryOnNext(2);

            mp.test().assertResult();
        });
    }

    @Test
    public void close() {
        DirectProcessor<Integer> dp = new DirectProcessor<>();
        MulticastProcessor<Integer> mp = new MulticastProcessor<>(1);

        dp.subscribe(mp);

        assertTrue(dp.hasSubscribers());

        mp.close();

        assertFalse(dp.hasSubscribers());

        mp.test().assertFailure(CancellationException.class);
    }

    @Test
    public void beforeOnSubscribe() {
        MulticastProcessor<Integer> mp = new MulticastProcessor<>(1);

        TestConsumer<Integer> tc = mp.test(0);

        tc.requestMore(1);

        mp.start();

        mp.onNext(1);
        mp.onComplete();

        tc.assertResult(1);
    }

    @Test
    public void emptyFused() {
        MulticastProcessor<Integer> mp = new MulticastProcessor<>(1);

        TestConsumer<Integer> tc = mp.test();

        Folyam.<Integer>empty().subscribe(mp);

        tc.assertResult();
        mp.test().assertResult();
    }

    @Test
    public void justFused() {
        MulticastProcessor<Integer> mp = new MulticastProcessor<>(1);

        TestConsumer<Integer> tc = mp.test(2);

        Folyam.just(1).subscribe(mp);

        tc.assertResult(1);
        mp.test().assertResult();
    }
}