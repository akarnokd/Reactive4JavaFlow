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
import org.junit.*;

import java.io.IOException;
import java.util.concurrent.*;

import static org.junit.Assert.*;

public class ConnectableFolyamRefCountTest {

    @Test
    public void byCount() {
        final int[] subscriptions = { 0 };

        Folyam<Integer> source = Folyam.range(1, 5)
                .doOnSubscribe(s -> subscriptions[0]++)
                .publish()
                .<Integer>refCount(2);

        for (int i = 0; i < 3; i++) {
            TestConsumer<Integer> ts1 = source.test();

            ts1.assertEmpty();

            TestConsumer<Integer> ts2 = source.test();

            ts1.assertResult(1, 2, 3, 4, 5);
            ts2.assertResult(1, 2, 3, 4, 5);
        }

        assertEquals(3, subscriptions[0]);
    }

    @Test
    public void resubscribeBeforeTimeout() throws Exception {
        final int[] subscriptions = { 0 };

        DirectProcessor<Integer> pp = new DirectProcessor<>();

        Folyam<Integer> source = pp
                .doOnSubscribe(s -> subscriptions[0]++)
                .publish()
                .refCount(500, TimeUnit.MILLISECONDS, SchedulerServices.single());

        TestConsumer<Integer> ts1 = source.test(0);

        assertEquals(1, subscriptions[0]);

        ts1.cancel();

        Thread.sleep(100);

        ts1 = source.test(0);

        assertEquals(1, subscriptions[0]);

        Thread.sleep(500);

        assertEquals(1, subscriptions[0]);

        pp.onNext(1);
        pp.onNext(2);
        pp.onNext(3);
        pp.onNext(4);
        pp.onNext(5);
        pp.onComplete();

        ts1.requestMore(5)
                .assertResult(1, 2, 3, 4, 5);
    }

    @Test
    public void letitTimeout() throws Exception {
        final int[] subscriptions = { 0 };

        DirectProcessor<Integer> pp = new DirectProcessor<>();

        Folyam<Integer> source = pp
                .doOnSubscribe(s -> subscriptions[0]++)
                .publish()
                .refCount(1, 100, TimeUnit.MILLISECONDS, SchedulerServices.single());

        TestConsumer<Integer> ts1 = source.test(0);

        assertEquals(1, subscriptions[0]);

        ts1.cancel();

        assertTrue(pp.hasSubscribers());

        Thread.sleep(200);

        assertFalse(pp.hasSubscribers());
    }

    @Test
    public void error() {
        Folyam.<Integer>error(new IOException())
                .publish()
                .refCount(500, TimeUnit.MILLISECONDS, SchedulerServices.single())
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void comeAndGo() {
        DirectProcessor<Integer> pp = new DirectProcessor<>();

        Folyam<Integer> source = pp
                .publish()
                .refCount();

        TestConsumer<Integer> ts1 = source.test(0);

        assertTrue(pp.hasSubscribers());

        for (int i = 0; i < 3; i++) {
            TestConsumer<Integer> ts2 = source.test();
            ts1.cancel();
            ts1 = ts2;
        }

        ts1.cancel();

        assertFalse(pp.hasSubscribers());
    }

    @Test
    public void unsubscribeSubscribeRace() {
        for (int i = 0; i < 1000; i++) {

            final Folyam<Integer> source = Folyam.range(1, 5)
                    .replay()
                    .refCount(1)
                    ;

            final TestConsumer<Integer> ts1 = source.test(0);

            final TestConsumer<Integer> ts2 = new TestConsumer<Integer>(0);

            Runnable r1 = ts1::cancel;

            Runnable r2 = () -> source.subscribe(ts2);

            TestHelper.race(r1, r2);

            ts2.requestMore(6) // FIXME RxJava replay() doesn't issue onComplete without reqest
                    .withTag("Round: " + i)
                    .assertResult(1, 2, 3, 4, 5);
        }
    }
}
