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
import hu.akarnokd.reactive4javaflow.fused.ConditionalSubscriber;
import hu.akarnokd.reactive4javaflow.impl.SubscriptionHelper;
import hu.akarnokd.reactive4javaflow.impl.schedulers.SchedulerMultiWorkerSupport;
import hu.akarnokd.reactive4javaflow.impl.util.SpscArrayQueue;

import java.util.concurrent.Flow;
import java.util.concurrent.atomic.*;

/**
 * Ensures each 'rail' from upstream runs on a Worker from a Scheduler.
 *
 * @param <T> the value type
 */
public final class ParallelRunOn<T> extends ParallelFolyam<T> {
    final ParallelFolyam<? extends T> source;

    final SchedulerService scheduler;

    final int prefetch;

    public ParallelRunOn(ParallelFolyam<? extends T> parent,
            SchedulerService scheduler, int prefetch) {
        this.source = parent;
        this.scheduler = scheduler;
        this.prefetch = prefetch;
    }

    @Override
    public void subscribeActual(FolyamSubscriber<? super T>[] subscribers) {

        int n = subscribers.length;

        @SuppressWarnings("unchecked")
        final FolyamSubscriber<T>[] parents = new FolyamSubscriber[n];

        if (scheduler instanceof SchedulerMultiWorkerSupport) {
            SchedulerMultiWorkerSupport multiworker = (SchedulerMultiWorkerSupport) scheduler;
            multiworker.createWorkers(n, new MultiWorkerCallback(subscribers, parents));
        } else {
            for (int i = 0; i < n; i++) {
                createSubscriber(i, subscribers, parents, scheduler.worker());
            }
        }
        source.subscribe(parents);
    }

    void createSubscriber(int i, FolyamSubscriber<? super T>[] subscribers,
                          FolyamSubscriber<T>[] parents, SchedulerService.Worker worker) {

        FolyamSubscriber<? super T> a = subscribers[i];

        SpscArrayQueue<T> q = new SpscArrayQueue<>(prefetch);

        if (a instanceof ConditionalSubscriber) {
            parents[i] = new RunOnConditionalSubscriber<>((ConditionalSubscriber<? super T>)a, prefetch, q, worker);
        } else {
            parents[i] = new RunOnSubscriber<>(a, prefetch, q, worker);
        }
    }

    final class MultiWorkerCallback implements SchedulerMultiWorkerSupport.WorkerCallback {

        final FolyamSubscriber<? super T>[] subscribers;

        final FolyamSubscriber<T>[] parents;

        MultiWorkerCallback(FolyamSubscriber<? super T>[] subscribers,
                            FolyamSubscriber<T>[] parents) {
            this.subscribers = subscribers;
            this.parents = parents;
        }

        @Override
        public void onWorker(int i, SchedulerService.Worker w) {
            createSubscriber(i, subscribers, parents, w);
        }
    }


    @Override
    public int parallelism() {
        return source.parallelism();
    }

    abstract static class BaseRunOnSubscriber<T> extends AtomicInteger
    implements FolyamSubscriber<T>, Flow.Subscription, Runnable {

        private static final long serialVersionUID = 9222303586456402150L;

        final int prefetch;

        final int limit;

        final SpscArrayQueue<T> queue;

        final SchedulerService.Worker worker;

        Flow.Subscription s;

        volatile boolean done;

        Throwable error;

        final AtomicLong requested = new AtomicLong();

        volatile boolean cancelled;

        int consumed;

        BaseRunOnSubscriber(int prefetch, SpscArrayQueue<T> queue, SchedulerService.Worker worker) {
            this.prefetch = prefetch;
            this.queue = queue;
            this.limit = prefetch - (prefetch >> 2);
            this.worker = worker;
        }

        @Override
        public final void onNext(T t) {
            if (done) {
                return;
            }
            if (!queue.offer(t)) {
                s.cancel();
                onError(new IllegalStateException("Queue is full?!"));
                return;
            }
            schedule();
        }

        @Override
        public final void onError(Throwable t) {
            if (done) {
                FolyamPlugins.onError(t);
                return;
            }
            error = t;
            done = true;
            schedule();
        }

        @Override
        public final void onComplete() {
            if (done) {
                return;
            }
            done = true;
            schedule();
        }

        @Override
        public final void request(long n) {
            SubscriptionHelper.addRequested(requested, n);
            schedule();
        }

        @Override
        public final void cancel() {
            if (!cancelled) {
                cancelled = true;
                s.cancel();
                worker.close();

                if (getAndIncrement() == 0) {
                    queue.clear();
                }
            }
        }

        final void schedule() {
            if (getAndIncrement() == 0) {
                worker.schedule(this);
            }
        }
    }

    static final class RunOnSubscriber<T> extends BaseRunOnSubscriber<T> {

        private static final long serialVersionUID = 1075119423897941642L;

        final FolyamSubscriber<? super T> actual;

        RunOnSubscriber(FolyamSubscriber<? super T> actual, int prefetch, SpscArrayQueue<T> queue, SchedulerService.Worker worker) {
            super(prefetch, queue, worker);
            this.actual = actual;
        }

        @Override
        public void onSubscribe(Flow.Subscription s) {
            this.s = s;

            actual.onSubscribe(this);

            s.request(prefetch);
        }

        @Override
        public void run() {
            int missed = 1;
            int c = consumed;
            SpscArrayQueue<T> q = queue;
            FolyamSubscriber<? super T> a = actual;
            int lim = limit;

            for (;;) {

                long r = requested.get();
                long e = 0L;

                while (e != r) {
                    if (cancelled) {
                        q.clear();
                        return;
                    }

                    boolean d = done;

                    if (d) {
                        Throwable ex = error;
                        if (ex != null) {
                            q.clear();

                            a.onError(ex);

                            worker.close();
                            return;
                        }
                    }

                    T v = q.poll();

                    boolean empty = v == null;

                    if (d && empty) {
                        a.onComplete();

                        worker.close();
                        return;
                    }

                    if (empty) {
                        break;
                    }

                    a.onNext(v);

                    e++;

                    int p = ++c;
                    if (p == lim) {
                        c = 0;
                        s.request(p);
                    }
                }

                if (e == r) {
                    if (cancelled) {
                        q.clear();
                        return;
                    }

                    if (done) {
                        Throwable ex = error;
                        if (ex != null) {
                            q.clear();

                            a.onError(ex);

                            worker.close();
                            return;
                        }
                        if (q.isEmpty()) {
                            a.onComplete();

                            worker.close();
                            return;
                        }
                    }
                }

                if (e != 0L && r != Long.MAX_VALUE) {
                    requested.addAndGet(-e);
                }

                int w = get();
                if (w == missed) {
                    consumed = c;
                    missed = addAndGet(-missed);
                    if (missed == 0) {
                        break;
                    }
                } else {
                    missed = w;
                }
            }
        }
    }

    static final class RunOnConditionalSubscriber<T> extends BaseRunOnSubscriber<T> {

        private static final long serialVersionUID = 1075119423897941642L;

        final ConditionalSubscriber<? super T> actual;

        RunOnConditionalSubscriber(ConditionalSubscriber<? super T> actual, int prefetch, SpscArrayQueue<T> queue, SchedulerService.Worker worker) {
            super(prefetch, queue, worker);
            this.actual = actual;
        }

        @Override
        public void onSubscribe(Flow.Subscription s) {
            this.s = s;

            actual.onSubscribe(this);

            s.request(prefetch);
        }

        @Override
        public void run() {
            int missed = 1;
            int c = consumed;
            SpscArrayQueue<T> q = queue;
            ConditionalSubscriber<? super T> a = actual;
            int lim = limit;

            for (;;) {

                long r = requested.get();
                long e = 0L;

                while (e != r) {
                    if (cancelled) {
                        q.clear();
                        return;
                    }

                    boolean d = done;

                    if (d) {
                        Throwable ex = error;
                        if (ex != null) {
                            q.clear();

                            a.onError(ex);

                            worker.close();
                            return;
                        }
                    }

                    T v = q.poll();

                    boolean empty = v == null;

                    if (d && empty) {
                        a.onComplete();

                        worker.close();
                        return;
                    }

                    if (empty) {
                        break;
                    }

                    if (a.tryOnNext(v)) {
                        e++;
                    }

                    int p = ++c;
                    if (p == lim) {
                        c = 0;
                        s.request(p);
                    }
                }

                if (e == r) {
                    if (cancelled) {
                        q.clear();
                        return;
                    }

                    if (done) {
                        Throwable ex = error;
                        if (ex != null) {
                            q.clear();

                            a.onError(ex);

                            worker.close();
                            return;
                        }
                        if (q.isEmpty()) {
                            a.onComplete();

                            worker.close();
                            return;
                        }
                    }
                }

                if (e != 0L && r != Long.MAX_VALUE) {
                    requested.addAndGet(-e);
                }

                int w = get();
                if (w == missed) {
                    consumed = c;
                    missed = addAndGet(-missed);
                    if (missed == 0) {
                        break;
                    }
                } else {
                    missed = w;
                }
            }
        }
    }
}
