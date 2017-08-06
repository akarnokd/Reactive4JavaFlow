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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class FolyamCreateTest {

    static ExecutorService exec;

    @BeforeClass
    public static void beforeClass() {
        exec = Executors.newSingleThreadExecutor();
    }

    @AfterClass
    public static void afterClass() {
        exec.shutdown();
    }

    @Test
    public void standard() {
        for (BackpressureHandling h : BackpressureHandling.values()) {
            TestHelper.assertResult(Folyam.create(e -> {
                exec.submit(() -> {
                    long f = 0L;
                    for (int i = 1; i < 6; i++) {
                        if (e.isCancelled()) {
                            return;
                        }

                        while (e.requested() == f) {
                            if (e.isCancelled()) {
                                return;
                            }
                            try {
                                Thread.sleep(1);
                            } catch (InterruptedException e1) {
                                return;
                            }
                        }

                        e.onNext(i);

                        f++;
                    }
                    e.onComplete();
                });
            }, h), 1, 2, 3, 4, 5);
        }
    }

    @Test
    public void standardSerialized() {
        for (BackpressureHandling h : BackpressureHandling.values()) {
            TestHelper.assertResult(Folyam.create(x -> {
                exec.submit(() -> {
                    FolyamEmitter<Integer> e = x.serialized().serialized();
                    long f = 0L;
                    for (int i = 1; i < 6; i++) {
                        if (e.isCancelled()) {
                            return;
                        }

                        while (e.requested() == f) {
                            if (e.isCancelled()) {
                                return;
                            }
                            try {
                                Thread.sleep(1);
                            } catch (InterruptedException e1) {
                                return;
                            }
                        }

                        e.onNext(i);

                        f++;
                    }
                    e.onComplete();
                });
            }, h), 1, 2, 3, 4, 5);
        }
    }

    @Test
    public void handlerCrash() {
        Folyam.create(e -> { throw new IOException(); }, BackpressureHandling.MISSING)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void resourceManagement1() {
        for (BackpressureHandling h : BackpressureHandling.values()) {
            int[] counter = {0};
            Folyam.create(e -> {
                e.setResource(() -> counter[0]++);
                e.onNext(1);
                e.onComplete();
            }, h)
                    .test()
                    .assertResult(1);

            assertEquals(1, counter[0]);
        }
    }

    @Test
    public void resourceManagement2() {
        for (BackpressureHandling h : BackpressureHandling.values()) {
            int[] counter = {0};
            Folyam.create(e -> {
                e.setResource(() -> counter[0]++);
                if (h != BackpressureHandling.ERROR) {
                    e.onNext(1);
                }
                e.onComplete();
            }, h)
                    .test(1, true, 0)
                    .assertEmpty();

            assertEquals(1, counter[0]);
        }
    }

    @Test
    public void resourceManagement3() {
        for (BackpressureHandling h : BackpressureHandling.values()) {
            int[] counter = {0};
            Folyam.create(e -> {
                e.setResource(() -> counter[0]++);
                e.onNext(1);
                e.onError(new IOException());
            }, h)
                    .test()
                    .assertFailure(IOException.class, 1);

            assertEquals(1, counter[0]);
        }
    }

    @Test
    public void resourceManagement4() {
        for (BackpressureHandling h : BackpressureHandling.values()) {
            int[] counter = { 0 };
            TestConsumer<Integer> tc = Folyam.<Integer>create(e -> {
                e.setResource(() -> counter[0]++);
                e.setResource(() -> counter[0]++);
            }, h).test();

            tc.cancel();

            assertEquals(h.toString(), 2, counter[0]);
        }
    }

    @Test
    public void resourceManagement1Serialized() {
        for (BackpressureHandling h : BackpressureHandling.values()) {
            int[] counter = {0};
            Folyam.create(x -> {
                FolyamEmitter<Object> e = x.serialized();
                e.setResource(() -> counter[0]++);
                e.onNext(1);
                e.onComplete();
            }, h)
                    .test()
                    .assertResult(1);

            assertEquals(1, counter[0]);
        }
    }

    @Test
    public void overflowError() {
        Folyam.create(e -> {
            e.onNext(1);
            e.onNext(2);
        }, BackpressureHandling.ERROR)
                .test(1)
                .assertFailure(IllegalStateException.class, 1);
    }

    @Test
    public void nullItem() {
        for (BackpressureHandling h : BackpressureHandling.values()) {
            Folyam.create(e -> {
                e.onNext(null);
            }, h)
            .test()
                    .withTag(h.toString())
                    .assertFailureAndMessage(NullPointerException.class, "item == null");
        }
    }

    @Test
    public void nullItemSerialized() {
        for (BackpressureHandling h : BackpressureHandling.values()) {
            Folyam.create(e -> {
                e.serialized().onNext(null);
            }, h)
                    .test()
                    .withTag(h.toString())
                    .assertFailureAndMessage(NullPointerException.class, "item == null");
        }
    }

    @Test
    public void nullError() {
        for (BackpressureHandling h : BackpressureHandling.values()) {
            Folyam.create(e -> {
                e.onError(null);
            }, h)
                    .test()
                    .withTag(h.toString())
                    .assertFailureAndMessage(NullPointerException.class, "ex == null");
        }
    }

    @Test
    public void nullErrorSerialized() {
        for (BackpressureHandling h : BackpressureHandling.values()) {
            Folyam.create(e -> {
                e.serialized().onError(null);
            }, h)
                    .test()
                    .withTag(h.toString())
                    .assertFailureAndMessage(NullPointerException.class, "ex == null");
        }
    }


    @Test
    public void nullTryError() {
        for (BackpressureHandling h : BackpressureHandling.values()) {
            Folyam.create(e -> {
                e.tryOnError(null);
            }, h)
                    .test()
                    .withTag(h.toString())
                    .assertFailureAndMessage(NullPointerException.class, "ex == null");
        }
    }

    @Test
    public void nullTryErrorSerialized() {
        for (BackpressureHandling h : BackpressureHandling.values()) {
            Folyam.create(e -> {
                e.serialized().tryOnError(null);
            }, h)
                    .test()
                    .withTag(h.toString())
                    .assertFailureAndMessage(NullPointerException.class, "ex == null");
        }
    }


    @Test
    public void signalAfterComplete() {
        for (BackpressureHandling h : BackpressureHandling.values()) {
            Boolean[] b = { null };
            Folyam.create(e -> {
                e.onComplete();
                e.onNext(1);
                b[0] = e.tryOnError(new IOException());
                e.onComplete();
            }, h)
                    .test(1, false, 0)
                    .withTag(h.toString())
                    .assertResult();

            assertEquals(h.toString(), Boolean.FALSE, b[0]);
        }
    }

    @Test
    public void signalAfterCompleteSerialized() {
        for (BackpressureHandling h : BackpressureHandling.values()) {
            Boolean[] b = { null };
            Folyam.create(x -> {
                FolyamEmitter<Object> e = x.serialized();
                e.onComplete();
                e.onNext(1);
                b[0] = e.tryOnError(new IOException());
                e.onComplete();
            }, h)
                    .test(1, false, 0)
                    .withTag(h.toString())
                    .assertResult();

            assertEquals(h.toString(), Boolean.FALSE, b[0]);
        }
    }
    @Test
    public void throwingResourcesViaReplaceAndCancel() {
        for (BackpressureHandling h : BackpressureHandling.values()) {
            TestHelper.withErrorTracking(errors -> {
                TestConsumer<Object> tc = new TestConsumer<>();
                tc.withTag(h.toString());

                Folyam.create(e -> {
                    e.setResource(() -> { throw new IOException(h + "1"); });
                    e.setResource(() -> { throw new IOException(h + "2"); });
                    tc.cancel();
                    e.setResource(() -> { throw new IOException(h + "3"); });
                }, h)
                .subscribe(tc);

                tc.assertEmpty();

                TestHelper.assertError(errors, 0, IOException.class, h + "1");
                TestHelper.assertError(errors, 1, IOException.class, h + "2");
                TestHelper.assertError(errors, 2, IOException.class, h + "3");
            });

        }
    }

    @Test
    public void concurrentOnNext() {
        int n = 500;
        Object[] values = new Object[n];
        Arrays.fill(values, 1);
        for (int i = 0; i < 1000; i++) {

            TestConsumer<Object> tc = Folyam.create(x -> {
                FolyamEmitter<Object> e = x.serialized();

                AtomicInteger sync = new AtomicInteger(2);

                Future<?> f = exec.submit(() -> {
                   if (sync.decrementAndGet() != 0) {
                       while (sync.get() != 0) ;
                   }
                   for (int j = 0; j < n / 2; j++) {
                       e.onNext(1);
                   }
                });

                if (sync.decrementAndGet() != 0) {
                    while (sync.get() != 0) ;
                }
                for (int j = 0; j < n / 2; j++) {
                    e.onNext(1);
                }

                f.get();

                e.onComplete();
            }, BackpressureHandling.MISSING)
            .test();

            tc.awaitDone(5000, TimeUnit.MILLISECONDS)
            .assertResult(values);

        }
    }
}
