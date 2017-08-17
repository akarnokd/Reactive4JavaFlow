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
import hu.akarnokd.reactive4javaflow.functionals.AutoDisposable;
import hu.akarnokd.reactive4javaflow.fused.*;
import hu.akarnokd.reactive4javaflow.impl.*;
import hu.akarnokd.reactive4javaflow.impl.schedulers.TrampolineSchedulerService;

import java.lang.invoke.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public final class FolyamInterval extends Folyam<Long> {

    final long start;

    final long end;

    final long initialDelay;

    final long period;

    final TimeUnit unit;

    final SchedulerService executor;

    public FolyamInterval(long initialDelay, long period, TimeUnit unit, SchedulerService executor) {
        this(0, Long.MAX_VALUE, initialDelay, period, unit, executor);
    }

    public FolyamInterval(long start, long end, long initialDelay, long period, TimeUnit unit, SchedulerService executor) {
        this.start = start;
        this.end = end;
        this.initialDelay = initialDelay;
        this.period = period;
        this.unit = unit;
        this.executor = executor;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super Long> s) {
        AbstractIntervalSubscription parent;

        if (s instanceof ConditionalSubscriber) {
            parent = new IntervalConditionalSubscription((ConditionalSubscriber<? super Long>)s, start, end);
        } else {
            parent = new IntervalSubscription(s, start, end);
        }
        s.onSubscribe(parent);
        SchedulerService exec = executor;
        if (exec instanceof TrampolineSchedulerService) {
            SchedulerService.Worker w = exec.worker();
            parent.setTask(w);
            w.schedulePeriodically(parent, initialDelay, period, unit);
        } else {
            parent.setTask(exec.schedulePeriodically(parent, initialDelay, period, unit));
        }
    }

    static abstract class AbstractIntervalSubscription extends AtomicInteger implements FusedSubscription<Long>, Runnable {

        final long end;

        AutoDisposable task;
        static final VarHandle TASK = VH.find(MethodHandles.lookup(), AbstractIntervalSubscription.class, "task", AutoDisposable.class);

        long available;
        static final VarHandle AVAILABLE = VH.find(MethodHandles.lookup(), AbstractIntervalSubscription.class, "available", Long.TYPE);

        long requested;
        static final VarHandle REQUESTED = VH.find(MethodHandles.lookup(), AbstractIntervalSubscription.class, "requested", Long.TYPE);

        boolean cancelled;
        static final VarHandle CANCELLED = VH.find(MethodHandles.lookup(), AbstractIntervalSubscription.class, "cancelled", Boolean.TYPE);

        boolean fused;

        long consumed;

        AbstractIntervalSubscription(long start, long end) {
            this.available = start;
            this.consumed = start;
            this.end = end;
        }

        @Override
        public final int requestFusion(int mode) {
            if ((mode & ASYNC) != 0) {
                fused = true;
                return ASYNC;
            }
            return NONE;
        }

        @Override
        public final Long poll() throws Throwable {
            long c = consumed;
            long a = (long)AVAILABLE.getAcquire(this);
            long f = end;
            if (c != a && c != f) {
                consumed = c + 1;
                return c;
            }
            return null;
        }

        @Override
        public final boolean isEmpty() {
            long c = consumed;
            return c == end || c == (long)AVAILABLE.getAcquire(this);
        }

        @Override
        public final void clear() {
            consumed = end;
        }

        @Override
        public final void run() {
            long a = available;
            long f = end;
            if (a != f) {
                AVAILABLE.setRelease(this, ++a);
                drain();
                if (a == f) {
                    DisposableHelper.close(this, TASK);
                }
            }
        }

        @Override
        public final void request(long n) {
            SubscriptionHelper.addRequested(this, REQUESTED, n);
            drain();
        }

        final void drain() {
            if (getAndIncrement() != 0) {
                return;
            }

            if (fused) {
                drainFused();
            } else {
                drainNormal();
            }
        }

        abstract void drainFused();

        abstract void drainNormal();

        @Override
        public final void cancel() {
            CANCELLED.setRelease(this, true);
            DisposableHelper.close(this, TASK);
        }

        final void setTask(AutoDisposable d) {
            DisposableHelper.replace(this, TASK, d);
        }
    }

    static final class IntervalSubscription extends AbstractIntervalSubscription {

        final FolyamSubscriber<? super Long> actual;

        IntervalSubscription(FolyamSubscriber<? super Long> actual, long start, long end) {
            super(start, end);
            this.actual = actual;
        }

        @Override
        void drainFused() {
            int missed = 1;

            FolyamSubscriber<? super Long> a = actual;
            long f = end;

            for (;;) {

                if ((boolean)CANCELLED.getAcquire(this)) {
                    return;
                }

                long avail = (long)AVAILABLE.getAcquire(this);
                long c = consumed;

                if (avail != c) {
                    a.onNext(null);
                }

                if (avail == f) {
                    a.onComplete();
                    return;
                }

                missed = addAndGet(-missed);
                if (missed == 0) {
                    break;
                }
            }
        }

        @Override
        void drainNormal() {
            int missed = 1;

            FolyamSubscriber<? super Long> a = actual;
            long c = consumed;
            long f = end;

            for (;;) {

                long r = (long)REQUESTED.getAcquire(this);

                while (c != r) {
                    if ((boolean)CANCELLED.getAcquire(this)) {
                        return;
                    }

                    long avail = (long)AVAILABLE.getAcquire(this);
                    boolean d = avail == f;
                    boolean empty = avail == c;

                    if (d && empty) {
                        a.onComplete();
                        return;
                    }

                    if (empty) {
                        break;
                    }

                    a.onNext(c);

                    c++;
                }

                if (c == r) {
                    if ((boolean)CANCELLED.getAcquire(this)) {
                        return;
                    }

                    long avail = (long)AVAILABLE.getAcquire(this);
                    boolean d = avail == f;
                    boolean empty = avail == c;

                    if (d && empty) {
                        a.onComplete();
                        return;
                    }
                }

                consumed = c;
                missed = addAndGet(-missed);
                if (missed == 0) {
                    break;
                }
            }
        }
    }

    static final class IntervalConditionalSubscription extends AbstractIntervalSubscription {

        final ConditionalSubscriber<? super Long> actual;

        long emitted;

        IntervalConditionalSubscription(ConditionalSubscriber<? super Long> actual, long start, long end) {
            super(start, end);
            this.actual = actual;
        }

        @Override
        void drainFused() {
            int missed = 1;

            ConditionalSubscriber<? super Long> a = actual;
            long f = end;

            for (;;) {

                if ((boolean)CANCELLED.getAcquire(this)) {
                    return;
                }

                long avail = (long)AVAILABLE.getAcquire(this);
                long c = consumed;

                if (avail != c) {
                    a.tryOnNext(null);
                }

                if (avail == f) {
                    a.onComplete();
                    return;
                }

                missed = addAndGet(-missed);
                if (missed == 0) {
                    break;
                }
            }
        }

        @Override
        void drainNormal() {
            int missed = 1;

            ConditionalSubscriber<? super Long> a = actual;
            long c = consumed;
            long e = emitted;
            long f = end;

            for (;;) {

                long r = (long)REQUESTED.getAcquire(this);

                while (c != r) {
                    if ((boolean)CANCELLED.getAcquire(this)) {
                        return;
                    }

                    long avail = (long)AVAILABLE.getAcquire(this);
                    boolean d = avail == f;
                    boolean empty = avail == c;

                    if (d && empty) {
                        a.onComplete();
                        return;
                    }

                    if (empty) {
                        break;
                    }

                    a.onNext(c);

                    c++;
                }

                if (c == r) {
                    if ((boolean)CANCELLED.getAcquire(this)) {
                        return;
                    }

                    long avail = (long)AVAILABLE.getAcquire(this);
                    boolean d = avail == f;
                    boolean empty = avail == c;

                    if (d && empty) {
                        a.onComplete();
                        return;
                    }
                }

                emitted = e;
                consumed = c;
                missed = addAndGet(-missed);
                if (missed == 0) {
                    break;
                }
            }
        }
    }
}
