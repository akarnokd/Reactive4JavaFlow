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
        SampleSubscriber<T> parent = new SampleSubscriber<>(s);
        s.onSubscribe(parent);
        sampler.subscribe(parent.sampler);
        source.subscribe(parent);
    }

    static final class SampleSubscriber<T> implements ConditionalSubscriber<T>, Flow.Subscription {

        final FolyamSubscriber<? super T> actual;

        final SamplerSubscriber sampler;

        T value;
        static final VarHandle VALUE;

        Flow.Subscription upstream;
        static final VarHandle UPSTREAM;

        long requested;
        static final VarHandle REQUESTED;

        static {
            try {
                VALUE = MethodHandles.lookup().findVarHandle(SampleSubscriber.class, "value", Object.class);
                UPSTREAM = MethodHandles.lookup().findVarHandle(SampleSubscriber.class, "upstream", Flow.Subscription.class);
                REQUESTED = MethodHandles.lookup().findVarHandle(SampleSubscriber.class, "requested", long.class);
            } catch (Throwable ex) {
                throw new InternalError(ex);
            }
        }

        SampleSubscriber(FolyamSubscriber<? super T> actual) {
            this.actual = actual;
            this.sampler = new SamplerSubscriber(this);
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            SubscriptionHelper.replace(this, UPSTREAM, subscription);
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
            // TODO implement
        }

        @Override
        public void onComplete() {
            // TODO implement
        }

        @Override
        public void request(long n) {
            SubscriptionHelper.addRequested(this, REQUESTED, n);
            sampler.request(n);
        }

        @Override
        public void cancel() {
            SubscriptionHelper.cancel(this, UPSTREAM);
            sampler.cancel();
        }

        void samplerNext() {
            // TODO implement
        }

        void samplerError(Throwable ex) {
            // TODO implement
        }

        void samplerComplete() {
            // TODO implement
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
