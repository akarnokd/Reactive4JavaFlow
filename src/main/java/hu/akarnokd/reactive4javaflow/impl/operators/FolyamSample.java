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
import java.util.concurrent.Flow;

public final class FolyamSample<T> extends Folyam<T> {

    final Folyam<T> source;

    final Flow.Publisher<?> sampler;

    final boolean emitLast;

    public FolyamSample(Folyam<T> source, Flow.Publisher<?> sampler, boolean emitLast) {
        this.source = source;
        this.sampler = sampler;
        this.emitLast = emitLast;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        SampleSubscriber<T> parent = new SampleSubscriber<>(s, emitLast);
        s.onSubscribe(parent);
        sampler.subscribe(parent.sampler);
        source.subscribe(parent);
    }

    static final class SampleSubscriber<T> implements ConditionalSubscriber<T>, Flow.Subscription {

        final FolyamSubscriber<? super T> actual;

        final SamplerSubscriber sampler;

        final boolean emitLast;

        T value;
        static final VarHandle VALUE;

        Flow.Subscription upstream;
        static final VarHandle UPSTREAM;

        long requested;
        static final VarHandle REQUESTED;

        int wip;
        static final VarHandle WIP;

        Throwable error;
        static final VarHandle ERROR;

        boolean done;
        static final VarHandle DONE;

        volatile boolean cancelled;

        static {
            try {
                VALUE = MethodHandles.lookup().findVarHandle(SampleSubscriber.class, "value", Object.class);
                UPSTREAM = MethodHandles.lookup().findVarHandle(SampleSubscriber.class, "upstream", Flow.Subscription.class);
                REQUESTED = MethodHandles.lookup().findVarHandle(SampleSubscriber.class, "requested", long.class);
                WIP = MethodHandles.lookup().findVarHandle(SampleSubscriber.class, "wip", int.class);
                ERROR = MethodHandles.lookup().findVarHandle(SampleSubscriber.class, "error", Throwable.class);
                DONE = MethodHandles.lookup().findVarHandle(SampleSubscriber.class, "done", boolean.class);
            } catch (Throwable ex) {
                throw new InternalError(ex);
            }
        }

        SampleSubscriber(FolyamSubscriber<? super T> actual, boolean emitLast) {
            this.actual = actual;
            this.emitLast = emitLast;
            this.sampler = new SamplerSubscriber(this);
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            SubscriptionHelper.deferredReplace(this, UPSTREAM, REQUESTED, subscription);
        }

        @Override
        public void onNext(T item) {
            if (!tryOnNext(item)) {
                upstream.request(1);
            }
        }

        @Override
        public boolean tryOnNext(T item) {
            return VALUE.getAndSet(this, item) == null;
        }

        @Override
        public void onError(Throwable throwable) {
            sampler.cancel();
            if (ExceptionHelper.addThrowable(this, ERROR, throwable)) {
                DONE.setRelease(this, true);
                drain();
            } else {
                FolyamPlugins.onError(throwable);
            }
        }

        @Override
        public void onComplete() {
            sampler.cancel();
            DONE.setRelease(this, true);
            drain();
        }

        @Override
        public void request(long n) {
            SubscriptionHelper.deferredRequest(this, UPSTREAM, REQUESTED, n);
            sampler.request(n);
        }

        @Override
        public void cancel() {
            cancelled = true;
            SubscriptionHelper.cancel(this, UPSTREAM);
            sampler.cancel();
        }

        void samplerNext() {
            drain();
        }

        void samplerError(Throwable throwable) {
            SubscriptionHelper.cancel(this, UPSTREAM);
            if (ExceptionHelper.addThrowable(this, ERROR, throwable)) {
                DONE.setRelease(this, true);
                drain();
            } else {
                FolyamPlugins.onError(throwable);
            }
        }

        void samplerComplete() {
            SubscriptionHelper.cancel(this, UPSTREAM);
            DONE.setRelease(this, true);
            drain();
        }

        void drain() {
            if ((int)WIP.getAndAdd(this, 1) != 0) {
                return;
            }

            int missed = 1;

            while (missed != 0) {

                if (cancelled) {
                    VALUE.set(this, null);
                    return;
                }

                if (ERROR.getAcquire(this) != null) {
                    VALUE.set(this, null);
                    Throwable ex = ExceptionHelper.terminate(this, ERROR);
                    actual.onError(ex);
                    return;
                }

                boolean d = (boolean)DONE.getAcquire(this);
                T v = (T)VALUE.getAndSet(this, null);

                if (d) {
                    if (emitLast && v != null) {
                        actual.onNext(v);
                    }
                    actual.onComplete();
                    return;
                }

                if (v != null) {
                    actual.onNext(v);
                }

                missed = (int)WIP.getAndAdd(this, -missed) - missed;
            }
        }

        static final class SamplerSubscriber implements FolyamSubscriber<Object>, Flow.Subscription {

            final SampleSubscriber<?> parent;

            Flow.Subscription upstream;
            static final VarHandle UPSTREAM;

            long requested;
            static final VarHandle REQUESTED;

            static {
                try {
                    UPSTREAM = MethodHandles.lookup().findVarHandle(SamplerSubscriber.class, "upstream", Flow.Subscription.class);
                    REQUESTED = MethodHandles.lookup().findVarHandle(SamplerSubscriber.class, "requested", long.class);
                } catch (Throwable ex) {
                    throw new InternalError(ex);
                }
            }

            SamplerSubscriber(SampleSubscriber<?> parent) {
                this.parent = parent;
            }

            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                SubscriptionHelper.deferredReplace(this, UPSTREAM, REQUESTED, subscription);
            }

            @Override
            public void onNext(Object item) {
                parent.samplerNext();
            }

            @Override
            public void onError(Throwable throwable) {
                parent.samplerError(throwable);
            }

            @Override
            public void onComplete() {
                parent.samplerComplete();
            }

            @Override
            public void request(long n) {
                SubscriptionHelper.deferredRequest(this, UPSTREAM, REQUESTED, n);
            }

            @Override
            public void cancel() {
                SubscriptionHelper.cancel(this, UPSTREAM);
            }
        }
    }
}
