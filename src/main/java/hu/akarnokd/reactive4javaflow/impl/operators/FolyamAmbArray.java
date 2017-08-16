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
import hu.akarnokd.reactive4javaflow.impl.*;

import java.lang.invoke.*;
import java.util.Arrays;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicInteger;

public final class FolyamAmbArray<T> extends Folyam<T> {

    final Flow.Publisher<? extends T>[] sources;

    public FolyamAmbArray(Flow.Publisher<? extends T>[] sources) {
        this.sources = sources;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        Flow.Publisher<? extends T>[] srcs = sources;
        int n = srcs.length;
        if (n == 0) {
            EmptySubscription.complete(s);
            return;
        }
        if (n == 1) {
            Flow.Publisher<? extends T> fp = srcs[0];
            if (fp == null) {
                EmptySubscription.error(s, new NullPointerException("Flow.Publisher[0] == null"));
            } else {
                fp.subscribe(s);
            }
            return;
        }
        if (s instanceof ConditionalSubscriber) {
            AmbConditionalCoordinator<T> parent = new AmbConditionalCoordinator<>((ConditionalSubscriber<? super T>)s, n);
            s.onSubscribe(parent);
            parent.subscribe(srcs, n);
        } else {
            AmbCoordinator<T> parent = new AmbCoordinator<>(s, n);
            s.onSubscribe(parent);
            parent.subscribe(srcs, n);
        }
    }

    public static <T> Folyam<T> ambWith(Folyam<T> source, Flow.Publisher<? extends T> other) {
        if (source instanceof FolyamAmbArray) {
            FolyamAmbArray aa = (FolyamAmbArray) source;
            int n = aa.sources.length;
            Flow.Publisher<? extends T>[] newSrcs = Arrays.copyOf(aa.sources, n + 1);
            newSrcs[n] = other;
            return new FolyamAmbArray<>(newSrcs);
        }
        return new FolyamAmbArray<>(new Flow.Publisher[] { source, other });
    }

    static abstract class AbstractInnerSubscriber implements Flow.Subscription {

        final int index;

        Flow.Subscription upstream;
        static final VarHandle UPSTREAM;

        long requested;
        static final VarHandle REQUESTED;

        boolean won;

        static {
            try {
                UPSTREAM = MethodHandles.lookup().findVarHandle(AbstractInnerSubscriber.class, "upstream", Flow.Subscription.class);
                REQUESTED = MethodHandles.lookup().findVarHandle(AbstractInnerSubscriber.class, "requested", Long.TYPE);
            } catch (Throwable ex) {
                throw new InternalError(ex);
            }
        }

        protected AbstractInnerSubscriber(int index) {
            this.index = index;
        }

        public final void onSubscribe(Flow.Subscription subscription) {
            SubscriptionHelper.deferredReplace(this, UPSTREAM, REQUESTED, subscription);
        }

        @Override
        public final void request(long n) {
            SubscriptionHelper.deferredRequest(this, UPSTREAM, REQUESTED, n);
        }

        @Override
        public final void cancel() {
            SubscriptionHelper.cancel(this, UPSTREAM);
        }
    }

    static final class AmbCoordinator<T> extends AtomicInteger implements Flow.Subscription {

        final AmbInnerSubscriber<T>[] subscribers;

        public AmbCoordinator(FolyamSubscriber<? super T> actual, int n) {
            AmbInnerSubscriber<T>[] subs = new AmbInnerSubscriber[n];
            for (int i = 0; i < n; i++) {
                subs[i] = new AmbInnerSubscriber<>(actual, this, i);
            }
            subscribers = subs;
            setRelease(-1);
        }

        public void subscribe(Flow.Publisher<? extends T>[] sources, int n) {
            AmbInnerSubscriber<T>[] subs = this.subscribers;
            for (int i = 0; i < n; i++) {
                if (getAcquire() != -1) {
                    break;
                }
                Flow.Publisher<? extends T> p = sources[i];
                if (p == null) {
                    for (AmbInnerSubscriber<T> inner : subs) {
                        inner.cancel();
                    }
                    Throwable ex = new NullPointerException("Flow.Publisher[" + i + "] == null");
                    if (compareAndSet(-1, Integer.MAX_VALUE)) {
                        subs[i].actual.onError(ex);
                    } else {
                        FolyamPlugins.onError(ex);
                    }
                    return;
                }
                p.subscribe(subs[i]);
            }
        }

        @Override
        public void request(long n) {
            int idx = getAcquire();
            if (idx < 0) {
                for (AmbInnerSubscriber<T> inner : subscribers) {
                    inner.request(n);
                }
            } else if (idx != Integer.MAX_VALUE) {
                subscribers[idx].request(n);
            }
        }

        @Override
        public void cancel() {
            int idx = getAndSet(Integer.MAX_VALUE);
            if (idx == -1) {
                for (AmbInnerSubscriber<T> inner : subscribers) {
                    inner.cancel();
                }
            } else
            if (idx != Integer.MAX_VALUE) {
                subscribers[idx].cancel();
            }
        }

        boolean tryWin(int index) {
            if (getAcquire() == -1 && compareAndSet(-1, index)) {
                AmbInnerSubscriber<T>[] subs = this.subscribers;
                int n = subs.length;
                for (int i = 0; i < index; i++) {
                    subs[i].cancel();
                }
                for (int i = index + 1; i < n; i++) {
                    subs[i].cancel();
                }
                return true;
            }
            return false;
        }
        static final class AmbInnerSubscriber<T> extends AbstractInnerSubscriber implements FolyamSubscriber<T> {

            final FolyamSubscriber<? super T> actual;

            final AmbCoordinator<T> parent;

            AmbInnerSubscriber(FolyamSubscriber<? super T> actual, AmbCoordinator<T> parent, int index) {
                super(index);
                this.actual = actual;
                this.parent = parent;
            }

            @Override
            public void onNext(T item) {
                if (won) {
                    actual.onNext(item);
                } else {
                    if (parent.tryWin(index)) {
                        won = true;
                        actual.onNext(item);
                    } else {
                        upstream.cancel();
                    }
                }
            }

            @Override
            public void onError(Throwable throwable) {
                if (won) {
                    actual.onError(throwable);
                } else {
                    if (parent.tryWin(index)) {
                        actual.onError(throwable);
                    } else {
                        FolyamPlugins.onError(throwable);
                    }
                }
            }

            @Override
            public void onComplete() {
                if (won || parent.tryWin(index)) {
                    actual.onComplete();
                }
            }
        }
    }

    static final class AmbConditionalCoordinator<T> extends AtomicInteger implements Flow.Subscription {

        final AmbInnerConditionalSubscriber<T>[] subscribers;

        public AmbConditionalCoordinator(ConditionalSubscriber<? super T> actual, int n) {
            AmbInnerConditionalSubscriber<T>[] subs = new AmbInnerConditionalSubscriber[n];
            for (int i = 0; i < n; i++) {
                subs[i] = new AmbInnerConditionalSubscriber<>(actual, this, i);
            }
            subscribers = subs;
            setRelease(-1);
        }

        public void subscribe(Flow.Publisher<? extends T>[] sources, int n) {
            AmbInnerConditionalSubscriber<T>[] subs = this.subscribers;
            for (int i = 0; i < n; i++) {
                if (getAcquire() != -1) {
                    break;
                }
                Flow.Publisher<? extends T> p = sources[i];
                if (p == null) {
                    for (AmbInnerConditionalSubscriber<T> inner : subs) {
                        inner.cancel();
                    }
                    Throwable ex = new NullPointerException("Flow.Publisher[" + i + "] == null");
                    if (compareAndSet(-1, Integer.MAX_VALUE)) {
                        subs[i].actual.onError(ex);
                    } else {
                        FolyamPlugins.onError(ex);
                    }
                    return;
                }
                p.subscribe(subs[i]);
            }
        }

        @Override
        public void request(long n) {
            int idx = getAcquire();
            if (idx < 0) {
                for (AmbInnerConditionalSubscriber<T> inner : subscribers) {
                    inner.request(n);
                }
            } else if (idx != Integer.MAX_VALUE) {
                subscribers[idx].request(n);
            }
        }

        @Override
        public void cancel() {
            int idx = getAndSet(Integer.MAX_VALUE);
            if (idx == -1) {
                for (AmbInnerConditionalSubscriber<T> inner : subscribers) {
                    inner.cancel();
                }
            } else
            if (idx != Integer.MAX_VALUE) {
                subscribers[idx].cancel();
            }
        }

        boolean tryWin(int index) {
            if (getAcquire() == -1 && compareAndSet(-1, index)) {
                AmbInnerConditionalSubscriber<T>[] subs = this.subscribers;
                int n = subs.length;
                for (int i = 0; i < index; i++) {
                    subs[i].cancel();
                }
                for (int i = index + 1; i < n; i++) {
                    subs[i].cancel();
                }
                return true;
            }
            return false;
        }
        static final class AmbInnerConditionalSubscriber<T> extends AbstractInnerSubscriber implements ConditionalSubscriber<T> {

            final ConditionalSubscriber<? super T> actual;

            final AmbConditionalCoordinator<T> parent;

            AmbInnerConditionalSubscriber(ConditionalSubscriber<? super T> actual, AmbConditionalCoordinator<T> parent, int index) {
                super(index);
                this.actual = actual;
                this.parent = parent;
            }

            @Override
            public void onNext(T item) {
                if (won) {
                    actual.onNext(item);
                } else {
                    if (parent.tryWin(index)) {
                        won = true;
                        actual.onNext(item);
                    } else {
                        upstream.cancel();
                    }
                }
            }

            @Override
            public boolean tryOnNext(T item) {
                if (won) {
                    return actual.tryOnNext(item);
                }
                if (parent.tryWin(index)) {
                    won = true;
                    return actual.tryOnNext(item);
                }
                upstream.cancel();
                return false;
            }

            @Override
            public void onError(Throwable throwable) {
                if (won) {
                    actual.onError(throwable);
                } else {
                    if (parent.tryWin(index)) {
                        actual.onError(throwable);
                    } else {
                        FolyamPlugins.onError(throwable);
                    }
                }
            }

            @Override
            public void onComplete() {
                if (won || parent.tryWin(index)) {
                    actual.onComplete();
                }
            }
        }
    }

}
