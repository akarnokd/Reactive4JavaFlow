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
import hu.akarnokd.reactive4javaflow.impl.PlainQueue;
import hu.akarnokd.reactive4javaflow.impl.util.SpscLinkedArrayQueue;

import java.util.concurrent.*;

public final class FolyamSpanout<T> extends Folyam<T> {

    final Folyam<T> source;

    final long initialSpan;

    final long betweenSpan;

    final SchedulerService scheduler;

    final boolean delayError;

    final int bufferSize;

    public FolyamSpanout(Folyam<T> source,
                         long initialSpan, long betweenSpan, TimeUnit unit,
                         SchedulerService scheduler,
                         boolean delayError,
                         int bufferSize) {
        this.source = source;
        this.initialSpan = unit.toNanos(initialSpan);
        this.betweenSpan = unit.toNanos(betweenSpan);
        this.scheduler = scheduler;
        this.delayError = delayError;
        this.bufferSize = bufferSize;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        source.subscribe(new SpanoutSubscriber<T>(s, initialSpan, betweenSpan,
                scheduler.worker(), delayError, bufferSize));
    }

    static final class SpanoutSubscriber<T> implements FolyamSubscriber<T>, Flow.Subscription, Runnable {

        final FolyamSubscriber<? super T> actual;

        final long initialSpan;

        final long betweenSpan;

        final SchedulerService.Worker worker;

        final boolean delayError;

        final PlainQueue<T> queue;

        long lastEvent;

        Flow.Subscription s;

        volatile Object terminalEvent;

        SpanoutSubscriber(FolyamSubscriber<? super T> actual, long initialSpan, long betweenSpan,
                          SchedulerService.Worker worker, boolean delayError, int bufferSize) {
            this.actual = actual;
            this.initialSpan = initialSpan;
            this.betweenSpan = betweenSpan;
            this.worker = worker;
            this.delayError = delayError;
            this.lastEvent = -1L;
            this.queue = new SpscLinkedArrayQueue<T>(bufferSize);
        }

        @Override
        public void onSubscribe(Flow.Subscription s) {
            this.s = s;

            actual.onSubscribe(this);
        }

        @Override
        public void onNext(T t) {
            queue.offer(t);

            long now = worker.now(TimeUnit.NANOSECONDS);
            long last = lastEvent;
            long between = betweenSpan;

            if (last == -1L) {
                lastEvent = now + between + initialSpan;
                worker.schedule(this, initialSpan, TimeUnit.NANOSECONDS);
            } else {
                if (last < now) {
                    lastEvent = now + between;
                    worker.schedule(this);
                } else {
                    lastEvent = last + between;
                    long next = last - now;
                    worker.schedule(this, next, TimeUnit.NANOSECONDS);
                }
            }
        }

        @Override
        public void onError(Throwable t) {
            terminalEvent = t;
            if (delayError) {
                long now = worker.now(TimeUnit.NANOSECONDS);
                worker.schedule(this, lastEvent - now - betweenSpan, TimeUnit.NANOSECONDS);
            } else {
                worker.schedule(this);
            }
        }

        @Override
        public void onComplete() {
            terminalEvent = this;
            long now = worker.now(TimeUnit.NANOSECONDS);
            worker.schedule(this, lastEvent - now - betweenSpan, TimeUnit.NANOSECONDS);
        }

        @Override
        public void request(long n) {
            s.request(n);
        }

        @Override
        public void cancel() {
            s.cancel();
            worker.close();
        }

        @Override
        public void run() {
            Object o = terminalEvent;
            if (o != null && o != this && !delayError) {
                queue.clear();
                actual.onError((Throwable)o);
                worker.close();
                return;
            }

            T v = queue.poll();

            boolean empty = v == null;

            if (o != null && empty) {
                if (o == this) {
                    actual.onComplete();
                } else {
                    actual.onError((Throwable)o);
                }
                worker.close();
                return;
            }

            if (!empty) {
                actual.onNext(v);
            }
        }
    }
}
