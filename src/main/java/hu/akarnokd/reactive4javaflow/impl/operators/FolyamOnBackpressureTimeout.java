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
import hu.akarnokd.reactive4javaflow.functionals.CheckedConsumer;
import hu.akarnokd.reactive4javaflow.impl.*;

import java.lang.invoke.*;
import java.util.ArrayDeque;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public final class FolyamOnBackpressureTimeout<T> extends Folyam<T> {

    final Folyam<T> source;

    final int maxSize;

    final long timeout;

    final TimeUnit unit;

    final SchedulerService scheduler;

    final CheckedConsumer<? super T> onEvict;

    public FolyamOnBackpressureTimeout(Folyam<T> source, int maxSize, long timeout, TimeUnit unit,
                                  SchedulerService scheduler, CheckedConsumer<? super T> onEvict) {
        this.source = source;
        this.maxSize = maxSize;
        this.timeout = timeout;
        this.unit = unit;
        this.scheduler = scheduler;
        this.onEvict = onEvict;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        source.subscribe(new OnBackpressureTimeoutSubscriber<>(s, maxSize, timeout, unit, scheduler.worker(), onEvict));
    }

    static final class OnBackpressureTimeoutSubscriber<T>
            extends AtomicInteger
            implements FolyamSubscriber<T>, Flow.Subscription, Runnable {

        private static final long serialVersionUID = 2264324530873250941L;

        final FolyamSubscriber<? super T> actual;

        final int maxSizeDouble;

        final long timeout;

        final TimeUnit unit;

        final SchedulerService.Worker worker;

        final CheckedConsumer<? super T> onEvict;

        Flow.Subscription s;

        long requested;
        static final VarHandle REQUESTED = VH.find(MethodHandles.lookup(), OnBackpressureTimeoutSubscriber.class, "requested", long.class);

        final ArrayDeque<Object> queue;

        volatile boolean done;
        Throwable error;

        volatile boolean cancelled;

        OnBackpressureTimeoutSubscriber(FolyamSubscriber<? super T> actual, int maxSize, long timeout, TimeUnit unit,
                                        SchedulerService.Worker worker, CheckedConsumer<? super T> onEvict) {
            this.actual = actual;
            this.maxSizeDouble = maxSize < Integer.MAX_VALUE / 2 ? maxSize << 1 : Integer.MAX_VALUE;
            this.timeout = timeout;
            this.unit = unit;
            this.worker = worker;
            this.onEvict = onEvict;
            this.queue = new ArrayDeque<>();
        }

        @Override
        public void request(long n) {
            SubscriptionHelper.addRequested(this, REQUESTED, n);
            drain();
        }

        @Override
        public void cancel() {
            cancelled = true;
            s.cancel();
            worker.close();

            if (getAndIncrement() == 0) {
                clearQueue();
            }
        }

        @SuppressWarnings("unchecked")
        void clearQueue() {
            for (;;) {
                T evicted;
                synchronized (this) {
                    if (queue.isEmpty()) {
                        break;
                    }

                    queue.poll();
                    evicted = (T)queue.poll();
                }

                evict(evicted);
            }
        }

        @Override
        public void onSubscribe(Flow.Subscription s) {
            this.s = s;

            actual.onSubscribe(this);

            s.request(Long.MAX_VALUE);
        }

        @SuppressWarnings("unchecked")
        @Override
        public void onNext(T t) {
            T evicted = null;
            synchronized (this) {
                if (queue.size() == maxSizeDouble) {
                    queue.poll();
                    evicted = (T)queue.poll();
                }
                queue.offer(worker.now(unit));
                queue.offer(t);
            }
            evict(evicted);
            worker.schedule(this, timeout, unit);
            drain();
        }

        @Override
        public void onError(Throwable t) {
            error = t;
            done = true;
            drain();
        }

        @Override
        public void onComplete() {
            done = true;
            drain();
        }

        @SuppressWarnings("unchecked")
        @Override
        public void run() {
            for (;;) {
                if (cancelled) {
                    break;
                }

                boolean d = done;
                boolean empty;
                T evicted = null;

                synchronized (this) {
                    Long ts = (Long)queue.peek();
                    empty = ts == null;
                    if (!empty) {
                        if (ts <= worker.now(unit) - timeout) {
                            queue.poll();
                            evicted = (T)queue.poll();
                        } else {
                            break;
                        }
                    }
                }

                evict(evicted);

                if (empty) {
                    if (d) {
                        drain();
                    }
                    break;
                }
            }
        }

        void evict(T evicted) {
            if (evicted != null) {
                try {
                    onEvict.accept(evicted);
                } catch (Throwable ex) {
                    FolyamPlugins.handleFatal(ex);
                    FolyamPlugins.onError(ex);
                }
            }
        }

        @SuppressWarnings("unchecked")
        void drain() {
            if (getAndIncrement() != 0) {
                return;
            }

            int missed = 1;

            for (;;) {
                long r = (long)REQUESTED.getAcquire(this);
                long e = 0;

                while (e != r) {
                    if (cancelled) {
                        clearQueue();
                        return;
                    }

                    boolean d = done;
                    T v;

                    synchronized (this) {
                        if (queue.poll() != null) {
                            v = (T)queue.poll();
                        } else {
                            v = null;
                        }
                    }

                    boolean empty = v == null;

                    if (d && empty) {
                        Throwable ex = error;
                        if (ex != null) {
                            actual.onError(ex);
                        } else {
                            actual.onComplete();
                        }

                        worker.close();
                        return;
                    }

                    if (empty) {
                        break;
                    }

                    actual.onNext(v);

                    e++;
                }

                if (e == r) {
                    if (cancelled) {
                        clearQueue();
                        return;
                    }

                    boolean d = done;
                    boolean empty;
                    synchronized (this) {
                        empty = queue.isEmpty();
                    }

                    if (d && empty) {
                        Throwable ex = error;
                        if (ex != null) {
                            actual.onError(ex);
                        } else {
                            actual.onComplete();
                        }

                        worker.close();
                        return;
                    }
                }

                missed = addAndGet(-missed);
                if (missed == 0) {
                    break;
                }
            }
        }
    }
}
