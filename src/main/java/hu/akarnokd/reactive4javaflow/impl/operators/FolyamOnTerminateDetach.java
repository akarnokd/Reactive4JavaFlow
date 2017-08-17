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
import hu.akarnokd.reactive4javaflow.fused.*;
import hu.akarnokd.reactive4javaflow.impl.VH;

import java.lang.invoke.*;
import java.util.concurrent.Flow;

public final class FolyamOnTerminateDetach<T> extends Folyam<T> {

    final Folyam<T> source;

    public FolyamOnTerminateDetach(Folyam<T> source) {
        this.source = source;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        if (s instanceof ConditionalSubscriber) {
            source.subscribe(new OnTerminateDetachConditionalSubscriber<>((ConditionalSubscriber<? super T>)s));
        } else {
            source.subscribe(new OnTerminateDetachSubscriber<>(s));
        }
    }

    static abstract class AbstractOnTerminateDetach<T> implements FusedSubscription<T> {

        Flow.Subscription upstream;
        static final VarHandle UPSTREAM = VH.find(MethodHandles.lookup(), AbstractOnTerminateDetach.class, "upstream", Flow.Subscription.class);

        FusedSubscription<T> qs;
        static final VarHandle QS = VH.find(MethodHandles.lookup(), AbstractOnTerminateDetach.class, "qs", FusedSubscription.class);

        int sourceFused;

        @Override
        @SuppressWarnings("unchecked")
        public final int requestFusion(int mode) {
            FusedSubscription<T> fs = (FusedSubscription<T>)QS.getAcquire(this);
            if (fs != null) {
                int m = fs.requestFusion(mode);
                sourceFused = m;
                return m;
            }
            return NONE;
        }

        @Override
        @SuppressWarnings("unchecked")
        public final T poll() throws Throwable {
            FusedSubscription<T> fs = (FusedSubscription<T>)QS.getAcquire(this);
            if (fs != null) {
                T v = fs.poll();
                if (v == null && (sourceFused == SYNC || UPSTREAM.getAcquire(this) == null)) {
                    UPSTREAM.setRelease(this, null);
                    QS.setRelease(this, null);
                    cleanup();
                }
                return v;
            }
            return null;
        }

        @Override
        @SuppressWarnings("unchecked")
        public final boolean isEmpty() {
            FusedSubscription<T> fs = (FusedSubscription<T>)QS.getAcquire(this);
            return fs == null || fs.isEmpty();
        }

        @Override
        @SuppressWarnings("unchecked")
        public final void clear() {
            FusedSubscription<T> fs = (FusedSubscription<T>)QS.getAcquire(this);
            if (fs != null) {
                fs.clear();
            }
        }

        @Override
        public final void request(long n) {
            Flow.Subscription s = (Flow.Subscription)UPSTREAM.getAcquire(this);
            if (s != null) {
                s.request(n);
            }
        }

        @Override
        public final void cancel() {
            Flow.Subscription s = (Flow.Subscription)UPSTREAM.getAcquire(this);
            if (s != null) {
                cleanup();
                UPSTREAM.setRelease(this, null);
                QS.setRelease(this, null);
                s.cancel();
            }
        }

        @SuppressWarnings("unchecked")
        public final void onSubscribe(Flow.Subscription subscription) {
            UPSTREAM.setRelease(this, subscription);
            if (subscription instanceof FusedSubscription) {
                QS.setRelease(this, (FusedSubscription<T>)subscription);
            }
            onStart();
        }

        abstract void cleanup();

        abstract void onStart();
    }

    static final class OnTerminateDetachSubscriber<T> extends AbstractOnTerminateDetach<T> implements FolyamSubscriber<T> {

        FolyamSubscriber<? super T> actual;
        static final VarHandle ACTUAL = VH.find(MethodHandles.lookup(), OnTerminateDetachSubscriber.class, "actual", FolyamSubscriber.class);

        OnTerminateDetachSubscriber(FolyamSubscriber<? super T> actual) {
            ACTUAL.setRelease(this, actual);
        }

        @Override
        void onStart() {
            actual.onSubscribe(this);
        }

        @Override
        @SuppressWarnings("unchecked")
        public void onNext(T item) {
            FolyamSubscriber<T> a = (FolyamSubscriber<T>)ACTUAL.getAcquire(this);
            if (a != null) {
                a.onNext(item);
            }
        }

        @Override
        @SuppressWarnings("unchecked")
        public void onError(Throwable throwable) {
            FolyamSubscriber<T> a = (FolyamSubscriber<T>)ACTUAL.getAcquire(this);
            if (a != null) {
                ACTUAL.set(this, null);
                UPSTREAM.setRelease(this, null);
                if (sourceFused == NONE) {
                    QS.set(this, null);
                }
                a.onError(throwable);
            }
        }

        @Override
        @SuppressWarnings("unchecked")
        public void onComplete() {
            FolyamSubscriber<T> a = (FolyamSubscriber<T>)ACTUAL.getAcquire(this);
            if (a != null) {
                ACTUAL.set(this, null);
                UPSTREAM.setRelease(this, null);
                if (sourceFused == NONE) {
                    QS.set(this, null);
                }
                a.onComplete();
            }
        }

        @Override
        void cleanup() {
            ACTUAL.setRelease(this, null);
        }
    }

    static final class OnTerminateDetachConditionalSubscriber<T> extends AbstractOnTerminateDetach<T> implements ConditionalSubscriber<T> {

        ConditionalSubscriber<? super T> actual;
        static final VarHandle ACTUAL = VH.find(MethodHandles.lookup(), OnTerminateDetachConditionalSubscriber.class, "actual", ConditionalSubscriber.class);

        OnTerminateDetachConditionalSubscriber(ConditionalSubscriber<? super T> actual) {
            ACTUAL.setRelease(this, actual);
        }

        @Override
        void onStart() {
            actual.onSubscribe(this);
        }

        @Override
        @SuppressWarnings("unchecked")
        public void onNext(T item) {
            ConditionalSubscriber<T> a = (ConditionalSubscriber<T>)ACTUAL.getAcquire(this);
            if (a != null) {
                a.onNext(item);
            }
        }

        @Override
        @SuppressWarnings("unchecked")
        public boolean tryOnNext(T item) {
            ConditionalSubscriber<T> a = (ConditionalSubscriber<T>)ACTUAL.getAcquire(this);
            return a != null && a.tryOnNext(item);
        }

        @Override
        @SuppressWarnings("unchecked")
        public void onError(Throwable throwable) {
            ConditionalSubscriber<T> a = (ConditionalSubscriber<T>)ACTUAL.getAcquire(this);
            if (a != null) {
                ACTUAL.setRelease(this, null);
                UPSTREAM.setRelease(this, null);
                if (sourceFused == NONE) {
                    QS.set(this, null);
                }
                a.onError(throwable);
            }
        }

        @Override
        @SuppressWarnings("unchecked")
        public void onComplete() {
            ConditionalSubscriber<T> a = (ConditionalSubscriber<T>)ACTUAL.getAcquire(this);
            if (a != null) {
                ACTUAL.setRelease(this, null);
                UPSTREAM.setRelease(this, null);
                if (sourceFused == NONE) {
                    QS.set(this, null);
                }
                a.onComplete();
            }
        }

        @Override
        void cleanup() {
            ACTUAL.setRelease(this, null);
        }
    }
}
