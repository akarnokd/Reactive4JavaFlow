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

package hu.akarnokd.reactive4javaflow.impl.schedulers;

import hu.akarnokd.reactive4javaflow.*;
import hu.akarnokd.reactive4javaflow.functionals.*;
import hu.akarnokd.reactive4javaflow.impl.BooleanSubscription;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class BlockingSchedulerServiceTest {

    TestConsumer<Integer> ts = new TestConsumer<>();

    @Test(timeout = 10000)
    public void workerUntimed() {
        TestHelper.withErrorTracking(errors -> {
            final BlockingSchedulerService scheduler = SchedulerServices.newBlocking();
            scheduler.execute(() -> {
                Folyam.range(1, 5)
                        .subscribeOn(scheduler)
                        .doFinally(scheduler::shutdown)
                        .subscribe(ts);

                ts.assertEmpty();
            });

            ts.assertResult(1, 2, 3, 4, 5);

            assertTrue(errors.toString(), errors.isEmpty());
        });
    }

    @Test(timeout = 10000)
    public void workerTimed() {
        TestHelper.withErrorTracking(errors -> {
            final BlockingSchedulerService scheduler = SchedulerServices.newBlocking();
            scheduler.execute(() -> {
                Folyam.range(1, 5)
                        .subscribeOn(scheduler)
                        .delay(100, TimeUnit.MILLISECONDS, scheduler)
                        .doFinally(scheduler::shutdown)
                        .subscribe(ts);

                ts.assertEmpty();
            });

            ts.assertResult(1, 2, 3, 4, 5);
            assertTrue(errors.toString(), errors.isEmpty());
        });
    }

    @Test(timeout = 10000)
    public void directCrash() {
        TestHelper.withErrorTracking(errors -> {
            final BlockingSchedulerService scheduler = SchedulerServices.newBlocking();
            scheduler.execute(() -> scheduler.schedule(() -> {
                scheduler.shutdown();
                throw new IllegalArgumentException();
            }));

            TestHelper.assertError(errors, 0, IllegalArgumentException.class);
        });
    }

    @Test(timeout = 10000)
    public void workerCrash() {
        TestHelper.withErrorTracking(errors -> {
            final BlockingSchedulerService scheduler = SchedulerServices.newBlocking();
            scheduler.execute(() -> {
                final SchedulerService.Worker worker = scheduler.worker();
                worker.schedule(() -> {
                    worker.close();
                    scheduler.shutdown();
                    throw new IllegalArgumentException();
                });
            });

            TestHelper.assertError(errors, 0, IllegalArgumentException.class);
        });
    }

    @Test(timeout = 10000)
    public void directUntimed() {
        TestHelper.withErrorTracking(errors -> {
            final BlockingSchedulerService scheduler = SchedulerServices.newBlocking();
            scheduler.execute(() -> {
                ts.onSubscribe(new BooleanSubscription());

                scheduler.schedule(() -> {
                    ts.onNext(1);
                    ts.onNext(2);
                    ts.onNext(3);
                    ts.onNext(4);
                    ts.onNext(5);
                    ts.onComplete();

                    scheduler.shutdown();
                });

                ts.assertEmpty();
            });

            ts.assertResult(1, 2, 3, 4, 5);
            for (Throwable t : errors) {
                t.printStackTrace();
            }
            assertTrue(errors.toString(), errors.isEmpty());
        });
    }

    @Test(timeout = 10000)
    public void directTimed() {
        TestHelper.withErrorTracking(errors -> {
            final BlockingSchedulerService scheduler = SchedulerServices.newBlocking();
            scheduler.execute(() -> {
                ts.onSubscribe(new BooleanSubscription());

                scheduler.schedule(() -> {
                    ts.onNext(1);
                    ts.onNext(2);
                    ts.onNext(3);
                    ts.onNext(4);
                    ts.onNext(5);
                    ts.onComplete();

                    scheduler.shutdown();
                }, 100, TimeUnit.MILLISECONDS);

                ts.assertEmpty();
            });

            ts.assertResult(1, 2, 3, 4, 5);
            assertTrue(errors.toString(), errors.isEmpty());
        });
    }

    @Test(timeout = 10000)
    public void cancelDirect() {
        TestHelper.withErrorTracking(errors -> {
            final BlockingSchedulerService scheduler = SchedulerServices.newBlocking();
            scheduler.execute(() -> {
                ts.onSubscribe(new BooleanSubscription());

                AutoDisposable d = scheduler.schedule(() -> {
                    ts.onNext(1);
                    ts.onNext(2);
                    ts.onNext(3);
                    ts.onNext(4);
                    ts.onNext(5);
                    ts.onComplete();
                }, 100, TimeUnit.MILLISECONDS);

                //assertFalse(d.isDisposed());
                d.close();
                //assertTrue(d.isDisposed());

                scheduler.schedule(scheduler::shutdown);

                ts.assertEmpty();
            });

            ts.assertEmpty();
            assertTrue(errors.toString(), errors.isEmpty());
        });
    }

    @Test(timeout = 10000)
    public void cancelDirectUntimed() {
        TestHelper.withErrorTracking(errors -> {
            final BlockingSchedulerService scheduler = SchedulerServices.newBlocking();
            scheduler.execute(() -> {
                ts.onSubscribe(new BooleanSubscription());

                AutoDisposable d = scheduler.schedule(() -> {
                    ts.onNext(1);
                    ts.onNext(2);
                    ts.onNext(3);
                    ts.onNext(4);
                    ts.onNext(5);
                    ts.onComplete();
                });

                //assertFalse(d.isDisposed());
                d.close();
                //assertTrue(d.isDisposed());

                scheduler.schedule(scheduler::shutdown);

                ts.assertEmpty();
            });

            ts.assertEmpty();
            assertTrue(errors.toString(), errors.isEmpty());
        });
    }

    @Test(timeout = 10000)
    public void cancelWorker() {
        TestHelper.withErrorTracking(errors -> {
            final BlockingSchedulerService scheduler = SchedulerServices.newBlocking();
            scheduler.execute(() -> {

                ts.onSubscribe(new BooleanSubscription());

                final SchedulerService.Worker w = scheduler.worker();

                AutoDisposable d = w.schedule(() -> {
                    ts.onNext(1);
                    ts.onNext(2);
                    ts.onNext(3);
                    ts.onNext(4);
                    ts.onNext(5);
                    ts.onComplete();
                }, 100, TimeUnit.MILLISECONDS);

                //assertFalse(d.isDisposed());
                d.close();
                //assertTrue(d.isDisposed());

                w.schedule(() -> {
                    w.close();
                    scheduler.shutdown();
                });

                ts.assertEmpty();
            });

            ts.assertEmpty();
            assertTrue(errors.toString(), errors.isEmpty());
        });
    }

    @Test(timeout = 10000)
    public void cancelWorkerUntimed() {
        TestHelper.withErrorTracking(errors -> {
            final BlockingSchedulerService scheduler = SchedulerServices.newBlocking();
            scheduler.execute(() -> {

                ts.onSubscribe(new BooleanSubscription());

                final SchedulerService.Worker w = scheduler.worker();

                AutoDisposable d = w.schedule(() -> {
                    ts.onNext(1);
                    ts.onNext(2);
                    ts.onNext(3);
                    ts.onNext(4);
                    ts.onNext(5);
                    ts.onComplete();
                });

                //assertFalse(d.isDisposed());
                d.close();
                //assertTrue(d.isDisposed());

                w.schedule(() -> {
                    w.close();
                    scheduler.shutdown();

                    //assertTrue(w.isDisposed());
                });

                ts.assertEmpty();
            });

            ts.assertEmpty();
            assertTrue(errors.toString(), errors.isEmpty());
        });
    }

    @Test(timeout = 10000)
    public void asyncShutdown() {
        TestHelper.withErrorTracking(errors -> {
            final BlockingSchedulerService scheduler = SchedulerServices.newBlocking();

            SchedulerServices.single().schedule(() -> {
                scheduler.schedule(() -> { });
                scheduler.shutdown();
                scheduler.shutdown();
                assertSame(SchedulerService.REJECTED, scheduler.schedule(() -> { }));
            }, 500, TimeUnit.MILLISECONDS);

            scheduler.start();

            assertTrue(errors.toString(), errors.isEmpty());
        });
    }

    @Test(timeout = 10000)
    public void asyncInterrupt() {
        TestHelper.withErrorTracking(errors -> {
            final BlockingSchedulerService scheduler = SchedulerServices.newBlocking();

            SchedulerServices.single().schedule(() -> {
                scheduler.shutdown.set(true);
                scheduler.thread.interrupt();
            }, 500, TimeUnit.MILLISECONDS);

            scheduler.start();

            assertTrue(errors.toString(), errors.isEmpty());
        });
    }

    @Test(timeout = 10000)
    public void asyncDispose() {
        TestHelper.withErrorTracking(errors -> {
            final BlockingSchedulerService scheduler = SchedulerServices.newBlocking();

            scheduler.execute(() -> {

                final AutoDisposable d = scheduler.schedule(() -> {
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException ex) {
                        // ignored
                        Thread.currentThread().interrupt();
                    }
                    scheduler.shutdown();
                });

                SchedulerServices.single().schedule(() -> {
                    d.close();
                    d.close();
                }, 500, TimeUnit.MILLISECONDS);
            });

            assertTrue(errors.toString(), errors.isEmpty());
        });
    }

    @Test(timeout = 10000)
    public void asyncFeedInto() {
        TestHelper.withErrorTracking(errors -> {
            final BlockingSchedulerService scheduler = SchedulerServices.newBlocking();

            final int n = 10000;

            final int[] counter = { 0 };

            scheduler.execute(() -> SchedulerServices.single().schedule(() -> {
                for (int i = 0; i < n; i++) {
                    scheduler.schedule(() -> counter[0]++);
                }
                scheduler.schedule(scheduler::shutdown);
            }));

            assertEquals(n, counter[0]);
            assertTrue(errors.toString(), errors.isEmpty());
        });
    }

    @Test(timeout = 10000)
    public void asyncFeedInto2() {
        TestHelper.withErrorTracking(errors -> {
            final BlockingSchedulerService scheduler = SchedulerServices.newBlocking();

            final int n = 1000;

            final int[] counter = { 0 };

            scheduler.execute(() -> {
                for (int i = 0; i < n; i++) {
                    scheduler.schedule(() -> counter[0]++, i, TimeUnit.MILLISECONDS);
                }
                scheduler.schedule(scheduler::shutdown, n + 10, TimeUnit.MILLISECONDS);
            });

            assertEquals(n, counter[0]);
            assertTrue(errors.toString(), errors.isEmpty());
        });
    }

    @Test(timeout = 10000)
    public void backtoSameThread() {
        TestHelper.withErrorTracking(errors -> {
            final BlockingSchedulerService scheduler = SchedulerServices.newBlocking();

            final Thread t0 = Thread.currentThread();
            final Thread[] t1 = { null };

            scheduler.execute(() -> Folyam.just(1)
                    .subscribeOn(SchedulerServices.newThread())
                    .observeOn(scheduler)
                    .doFinally(scheduler::shutdown)
                    .subscribe(v -> t1[0] = Thread.currentThread()));

            assertSame(t0, t1[0]);

            if (!errors.isEmpty()) {
                AssertionError ex = new AssertionError();
                errors.forEach(ex::addSuppressed);
                throw ex;
            }
        });
    }
}
