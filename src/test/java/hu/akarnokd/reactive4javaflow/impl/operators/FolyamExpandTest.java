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
import hu.akarnokd.reactive4javaflow.functionals.CheckedFunction;
import hu.akarnokd.reactive4javaflow.processors.DirectProcessor;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class FolyamExpandTest {

    CheckedFunction<Integer, Flow.Publisher<Integer>> countDown = v -> v == 0 ? Folyam.empty() : Folyam.just(v - 1);

    @Test
    public void recursiveCountdownDepth() {
        Folyam.just(10)
                .expand(countDown, true)
                .test()
                .assertResult(10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0);
    }

    @Test
    public void recursiveCountdownDefault() {
        Folyam.just(10)
                .expand(countDown)
                .test()
                .assertResult(10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0);
    }

    @Test
    public void recursiveCountdownBreath() {
        Folyam.just(10)
                .expand(countDown, false)
                .test()
                .assertResult(10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0);
    }

    @Test
    public void error() {
        for (boolean strategy : new boolean[] { true, false }) {
            Folyam.<Integer>error(new IOException())
                    .expand(countDown, strategy)
                    .test()
                    .assertFailure(IOException.class);
        }
    }

    @Test
    public void empty() {
        for (boolean strategy : new boolean[] { true, false }) {
            Folyam.<Integer>empty()
                    .expand(countDown, strategy)
                    .test()
                    .withTag("" + strategy)
                    .assertResult();
        }
    }

    @Test
    public void recursiveCountdown() {
        for (boolean strategy : new boolean[] { true, false }) {
            for (int i = 0; i < 1000; i = (i < 100 ? i + 1 : i + 50)) {
                String tag = "i = " + i + ", strategy = " + strategy;

                TestConsumer<Integer> ts = Folyam.just(i)
                        .expand(countDown, strategy)
                        .test()
                        .withTag(tag)
                        .assertNoErrors()
                        .assertComplete()
                        .assertValueCount(i + 1);

                List<Integer> list = ts.values();
                for (int j = 0; j <= i; j++) {
                    assertEquals(tag + ", " + list, i - j, list.get(j).intValue());
                }
            }
        }
    }

    @Test
    public void recursiveCountdownTake() {
        for (boolean strategy : new boolean[] { true, false }) {
            Folyam.just(10)
                    .expand(countDown, strategy)
                    .take(5)
                    .test()
                    .withTag("" + strategy)
                    .assertResult(10, 9, 8, 7, 6);
        }
    }

    @Test
    public void recursiveCountdownBackpressure() {
        for (boolean strategy : new boolean[] { true, false }) {
            Folyam.just(10)
                    .expand(countDown, strategy)
                    .test(0L)
                    .withTag("" + strategy)
                    .requestMore(1)
                    .assertValues(10)
                    .requestMore(3)
                    .assertValues(10, 9, 8, 7)
                    .requestMore(4)
                    .assertValues(10, 9, 8, 7, 6, 5, 4, 3)
                    .requestMore(3)
                    .assertResult(10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0);
        }
    }

    @Test
    public void expanderThrows() {
        for (boolean strategy : new boolean[] { true, false }) {
            Folyam.just(10)
                    .expand(v -> {
                        throw new IOException();
                    }, strategy)
                    .test()
                    .withTag("" + strategy)
                    .assertFailure(IOException.class, 10);
        }
    }

    @Test
    public void expanderReturnsNull() {
        for (boolean strategy : new boolean[] { true, false }) {
            Folyam.just(10)
                    .expand(v -> null, strategy)
                    .test()
                    .withTag("" + strategy)
                    .assertFailure(NullPointerException.class, 10);
        }
    }

    static final class Node {
        final String name;
        final List<Node> children;

        Node(String name, Node... nodes) {
            this.name = name;
            this.children = new ArrayList<>();
            Collections.addAll(children, nodes);
        }

        @Override
        public String toString() {
            return name;
        }
    }

    Node createTest() {
        return new Node("root",
                new Node("1",
                        new Node("11")
                ),
                new Node("2",
                        new Node("21"),
                        new Node("22",
                                new Node("221")
                        )
                ),
                new Node("3",
                        new Node("31"),
                        new Node("32",
                                new Node("321")
                        ),
                        new Node("33",
                                new Node("331"),
                                new Node("332",
                                        new Node("3321")
                                )
                        )
                ),
                new Node("4",
                        new Node("41"),
                        new Node("42",
                                new Node("421")
                        ),
                        new Node("43",
                                new Node("431"),
                                new Node("432",
                                        new Node("4321")
                                )
                        ),
                        new Node("44",
                                new Node("441"),
                                new Node("442",
                                        new Node("4421")
                                ),
                                new Node("443",
                                        new Node("4431"),
                                        new Node("4432")
                                )
                        )
                )
        );
    }

    @Test(timeout = 5000)
    public void depthFirst() {
        Node root = createTest();

        Folyam.just(root)
                .<Node>expand(v -> Folyam.fromIterable(v.children), true)
                .map(v -> v.name)
                .test()
                .assertResult(
                        "root",
                        "1", "11",
                        "2", "21", "22", "221",
                        "3", "31", "32", "321", "33", "331", "332", "3321",
                        "4", "41", "42", "421", "43", "431", "432", "4321",
                        "44", "441", "442", "4421", "443", "4431", "4432"
                );
    }

    @Test(timeout = 5000)
    public void depthFirstAsync() {
        Node root = createTest();

        Folyam.just(root)
                .expand(v -> Folyam.fromIterable(v.children).subscribeOn(SchedulerServices.computation()), true)
                .map(v -> v.name)
                .test()
                .awaitDone(5, TimeUnit.SECONDS)
                .assertResult(
                        "root",
                        "1", "11",
                        "2", "21", "22", "221",
                        "3", "31", "32", "321", "33", "331", "332", "3321",
                        "4", "41", "42", "421", "43", "431", "432", "4321",
                        "44", "441", "442", "4421", "443", "4431", "4432"
                );
    }

    @Test(timeout = 5000)
    public void breathFirst() {
        Node root = createTest();

        Folyam.just(root)
                .expand(v -> Folyam.fromIterable(v.children), false)
                .map(v -> v.name)
                .test()
                .assertResult(
                        "root",
                        "1", "2", "3", "4",
                        "11", "21", "22", "31", "32", "33", "41", "42", "43", "44",
                        "221", "321", "331", "332", "421", "431", "432", "441", "442", "443",
                        "3321", "4321", "4421", "4431", "4432"
                );
    }

    @Test(timeout = 5000)
    public void breathFirstAsync() {
        Node root = createTest();

        Folyam.just(root)
                .expand(v -> Folyam.fromIterable(v.children).subscribeOn(SchedulerServices.computation()), false)
                .map(v -> v.name)
                .test()
                .awaitDone(5, TimeUnit.SECONDS)
                .assertResult(
                        "root",
                        "1", "2", "3", "4",
                        "11", "21", "22", "31", "32", "33", "41", "42", "43", "44",
                        "221", "321", "331", "332", "421", "431", "432", "441", "442", "443",
                        "3321", "4321", "4421", "4431", "4432"
                );
    }

    @Test
    public void depthFirstCancel() {

        final DirectProcessor<Integer> pp = new DirectProcessor<>();

        final TestConsumer<Integer> ts = new TestConsumer<>();

        FolyamSubscriber<Integer> s = new FolyamSubscriber<>() {

            Flow.Subscription upstream;

            @Override
            public void onSubscribe(Flow.Subscription s) {
                upstream = s;
                ts.onSubscribe(s);
            }

            @Override
            public void onNext(Integer t) {
                ts.onNext(t);
                upstream.cancel();
                upstream.request(1);
                onComplete();
            }

            @Override
            public void onError(Throwable t) {
                ts.onError(t);
            }

            @Override
            public void onComplete() {
                ts.onComplete();
            }
        };

        Folyam.just(1)
                .expand(v -> pp, true)
                .subscribe(s);

        assertFalse(pp.hasSubscribers());

        ts.assertResult(1);
    }

    @Test
    public void depthCancelRace() {
        for (int i = 0; i < 1000; i++) {
            final TestConsumer<Integer> ts = Folyam.just(0)
                    .expand(countDown, true)
                    .test(0);

            Runnable r1 = () -> ts.requestMore(1);
            Runnable r2 = ts::cancel;

            TestHelper.race(r1, r2);
        }
    }

    @Test
    public void depthEmitCancelRace() {
        for (int i = 0; i < 1000; i++) {

            final DirectProcessor<Integer> pp = new DirectProcessor<>();

            final TestConsumer<Integer> ts = Folyam.just(0)
                    .expand(v -> pp, true)
                    .test(1);

            Runnable r1 = () -> pp.onNext(1);
            Runnable r2 = ts::cancel;

            TestHelper.race(r1, r2);
        }
    }

    @Test
    public void depthCompleteCancelRace() {
        for (int i = 0; i < 1000; i++) {

            final DirectProcessor<Integer> pp = new DirectProcessor<>();

            final TestConsumer<Integer> ts = Folyam.just(0)
                    .expand(v -> pp, true)
                    .test(1);

            Runnable r1 = pp::onComplete;
            Runnable r2 = ts::cancel;

            TestHelper.race(r1, r2);
        }
    }

    @Test
    public void depthCancelRace2() throws Exception {
        for (int i = 0; i < 1000; i++) {

            final DirectProcessor<Integer> pp = new DirectProcessor<>();

            Folyam<Integer> source = Folyam.just(0)
                    .expand(v -> pp, true);

            final CountDownLatch cdl = new CountDownLatch(1);

            TestConsumer<Integer> ts = new TestConsumer<Integer>() {
                final AtomicInteger sync = new AtomicInteger(2);

                @Override
                public void onNext(Integer t) {
                    super.onNext(t);
                    SchedulerServices.single().schedule(() -> {
                        if (sync.decrementAndGet() != 0) {
                            while (sync.get() != 0) { }
                        }
                        cancel();
                        cdl.countDown();
                    });
                    if (sync.decrementAndGet() != 0) {
                        while (sync.get() != 0) { }
                    }
                }
            };

            source.subscribe(ts);

            assertTrue(cdl.await(5, TimeUnit.SECONDS));
        }
    }
}
