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

import java.lang.invoke.*;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicLong;

public final class FolyamSubscribeOn<T> extends Folyam<T> {

    final Folyam<T> source;

    final SchedulerService executor;

    final boolean requestOn;

    public FolyamSubscribeOn(Folyam<T> source, SchedulerService executor, boolean requestOn) {
        this.source = source;
        this.executor = executor;
        this.requestOn = requestOn;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {

        SchedulerService.Worker w = executor.worker();

        AbstractSubscribeOn<T> parent;

        if (s instanceof ConditionalSubscriber) {
            parent = new SubscribeOnConditionalSubscriber<>((ConditionalSubscriber<? super T>)s, w, !requestOn, source);
        } else {
            parent = new SubscribeOnSubscriber<>(s, w, !requestOn, source);
        }

        s.onSubscribe(parent);

        w.schedule(parent);
    }

    static abstract class AbstractSubscribeOn<T> extends AtomicLong implements Flow.Subscription, Runnable {

        final SchedulerService.Worker worker;

        final boolean dontRequestOn;

        Thread thread;

        FolyamPublisher<T> source;

        Flow.Subscription upstream;
        static final VarHandle UPSTREAM;

        static {
            try {
                UPSTREAM = MethodHandles.lookup().findVarHandle(AbstractSubscribeOn.class, "upstream", Flow.Subscription.class);
            } catch (Throwable ex) {
                throw new InternalError(ex);
            }
        }

        AbstractSubscribeOn(SchedulerService.Worker worker, boolean dontRequestOn, FolyamPublisher<T> source) {
            this.worker = worker;
            this.dontRequestOn = dontRequestOn;
            this.source = source;
        }

        public final void onSubscribe(Flow.Subscription subscription) {
            if (SubscriptionHelper.replace(this, UPSTREAM, subscription)) {
                long n = getAndSet(0L);
                if (n != 0L) {
                    requestUpstream(n, subscription);
                }
            }
        }

        @Override
        public final void request(long n) {
            Flow.Subscription s = (Flow.Subscription)UPSTREAM.getAcquire(this);
            if (s != null) {
                requestUpstream(n, s);
            } else {
                SubscriptionHelper.addRequested(this, n);
                s = (Flow.Subscription)UPSTREAM.getAcquire(this);
                if (s != null) {
                    n = get();
                    if (n != 0L) {
                        requestUpstream(n, s);
                    }
                }
            }
        }

        @Override
        public final void cancel() {
            SubscriptionHelper.cancel(this, UPSTREAM);
            worker.close();
        }

        final void requestUpstream(long n, Flow.Subscription s) {
            if (dontRequestOn || thread == Thread.currentThread()) {
                s.request(n);
            } else {
                worker.schedule(new Request(s, n));
            }
        }
    }

    static final class SubscribeOnSubscriber<T> extends AbstractSubscribeOn<T> implements FolyamSubscriber<T> {

        final FolyamSubscriber<? super T> actual;

        SubscribeOnSubscriber(FolyamSubscriber<? super T> actual, SchedulerService.Worker worker, boolean dontRequestOn, FolyamPublisher<T> source) {
            super(worker, dontRequestOn, source);
            this.actual = actual;
        }

        @Override
        public void onNext(T item) {
            actual.onNext(item);
        }

        @Override
        public void onError(Throwable throwable) {
            actual.onError(throwable);
            worker.close();
            thread = null;
        }

        @Override
        public void onComplete() {
            actual.onComplete();
            worker.close();
            thread = null;
        }

        @Override
        public void run() {
            FolyamPublisher<T> src = source;
            source = null;
            thread = Thread.currentThread();
            src.subscribe(this);
        }
    }

    static final class SubscribeOnConditionalSubscriber<T> extends AbstractSubscribeOn<T> implements ConditionalSubscriber<T> {

        final ConditionalSubscriber<? super T> actual;

        SubscribeOnConditionalSubscriber(ConditionalSubscriber<? super T> actual, SchedulerService.Worker worker, boolean dontRequestOn, FolyamPublisher<T> source) {
            super(worker, dontRequestOn, source);
            this.actual = actual;
        }

        @Override
        public void onNext(T item) {
            actual.onNext(item);
        }

        @Override
        public boolean tryOnNext(T item) {
            return actual.tryOnNext(item);
        }

        @Override
        public void onError(Throwable throwable) {
            actual.onError(throwable);
            worker.close();
            thread = null;
        }

        @Override
        public void onComplete() {
            actual.onComplete();
            worker.close();
            thread = null;
        }

        @Override
        public void run() {
            FolyamPublisher<T> src = source;
            source = null;
            thread = Thread.currentThread();
            src.subscribe(this);
        }
    }

    static final class Request implements Runnable {
        final Flow.Subscription s;
        final long n;

        Request(Flow.Subscription s, long n) {
            this.s = s;
            this.n = n;
        }

        @Override
        public void run() {
            s.request(n);
        }
    }
}
