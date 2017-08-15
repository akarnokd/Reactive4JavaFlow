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
import hu.akarnokd.reactive4javaflow.functionals.CheckedFunction;
import hu.akarnokd.reactive4javaflow.fused.*;
import hu.akarnokd.reactive4javaflow.impl.*;

import java.lang.invoke.*;
import java.util.Objects;
import java.util.concurrent.Flow;

public final class EsetlegFlatMapPublisher<T, R> extends Folyam<R> {

    final Esetleg<T> source;

    final CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper;

    public EsetlegFlatMapPublisher(Esetleg<T> source, CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper) {
        this.source = source;
        this.mapper = mapper;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super R> s) {
        if (!tryScalarXMap(source, s, mapper)) {
            source.subscribe(new FlatMapPublisherSubscriber<>(s, mapper));
        }
    }

    public static <T, R> boolean tryScalarXMap(FolyamPublisher<T> source, FolyamSubscriber<? super R> s, CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper) {
        if (source instanceof FusedDynamicSource) {
            FusedDynamicSource<T> f = (FusedDynamicSource<T>) source;
            Flow.Publisher<? extends R> e = null;

            try {
                T v = f.value();
                if (v != null) {
                    e = Objects.requireNonNull(mapper.apply(v), "The mapper returned a null Flow.Publisher");
                }
            } catch (Throwable ex) {
                FolyamPlugins.handleFatal(ex);
                EmptySubscription.error(s, ex);
                return true;
            }

            if (e == null) {
                EmptySubscription.complete(s);
                return true;
            }

            if (e instanceof FusedDynamicSource) {
                FusedDynamicSource<R> g = (FusedDynamicSource<R>) e;
                R w;
                try {
                    w = g.value();
                } catch (Throwable ex) {
                    FolyamPlugins.handleFatal(ex);
                    EmptySubscription.error(s, ex);
                    return true;
                }

                if (w == null) {
                    EmptySubscription.complete(s);
                } else {
                    s.onSubscribe(new FolyamJust.JustSubscription<>(s, w));
                }
            } else {
                e.subscribe(s);
            }
            return true;
        }
        return false;
    }

    static final class FlatMapPublisherSubscriber<T, R> implements FolyamSubscriber<T>, Flow.Subscription {

        final FolyamSubscriber<? super R> actual;

        final CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper;

        Flow.Subscription upstream;
        static final VarHandle UPSTREAM;

        Flow.Subscription innerUpstream;
        static final VarHandle INNER_UPSTREAM;

        long requested;
        static final VarHandle REQUESTED;

        boolean done;

        static {
            try {
                UPSTREAM = MethodHandles.lookup().findVarHandle(FlatMapPublisherSubscriber.class, "upstream", Flow.Subscription.class);
                INNER_UPSTREAM = MethodHandles.lookup().findVarHandle(FlatMapPublisherSubscriber.class, "innerUpstream", Flow.Subscription.class);
                REQUESTED = MethodHandles.lookup().findVarHandle(FlatMapPublisherSubscriber.class, "requested", long.class);
            } catch (Throwable ex) {
                throw new InternalError(ex);
            }
        }

        FlatMapPublisherSubscriber(FolyamSubscriber<? super R> actual, CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper) {
            this.actual = actual;
            this.mapper = mapper;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            upstream = subscription;
            actual.onSubscribe(this);
            subscription.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(T item) {
            done = true;
            Flow.Publisher<? extends R> p;

            try {
                p = Objects.requireNonNull(mapper.apply(item), "The mapper returned a null Flow.Publisher");
            } catch (Throwable ex) {
                FolyamPlugins.handleFatal(ex);
                actual.onError(ex);
                return;
            }

            if (p instanceof FusedDynamicSource) {
                FusedDynamicSource<R> f = (FusedDynamicSource<R>) p;
                R w;

                try {
                    w = f.value();
                } catch (Throwable ex) {
                    FolyamPlugins.handleFatal(ex);
                    actual.onError(ex);
                    return;
                }

                if (w == null) {
                    actual.onComplete();
                } else {
                    innerOnSubscribe(new FolyamJust.JustSubscription<>(actual, w));
                }
            } else {
                if (actual instanceof ConditionalSubscriber) {
                    p.subscribe(new FlatMapPublisherInnerConditionalSubscriber((ConditionalSubscriber<? super R>)actual));
                } else {
                    p.subscribe(new FlatMapPublisherInnerSubscriber(actual));
                }
            }
        }

        @Override
        public void onError(Throwable throwable) {
            actual.onError(throwable);
        }

        @Override
        public void onComplete() {
            if (!done) {
                done = true;
                actual.onComplete();
            }
        }

        @Override
        public void request(long n) {
            SubscriptionHelper.deferredRequest(this, INNER_UPSTREAM, REQUESTED, n);
        }

        @Override
        public void cancel() {
            upstream.cancel();
            SubscriptionHelper.cancel(this, INNER_UPSTREAM);
        }

        void innerOnSubscribe(Flow.Subscription s) {
            SubscriptionHelper.deferredReplace(this, INNER_UPSTREAM, REQUESTED, s);
        }

        final class FlatMapPublisherInnerSubscriber implements FolyamSubscriber<R> {

            final FolyamSubscriber<? super R> actual;

            FlatMapPublisherInnerSubscriber(FolyamSubscriber<? super R> actual) {
                this.actual = actual;
            }

            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                innerOnSubscribe(subscription);
            }

            @Override
            public void onNext(R item) {
                actual.onNext(item);
            }

            @Override
            public void onError(Throwable throwable) {
                actual.onError(throwable);
            }

            @Override
            public void onComplete() {
                actual.onComplete();
            }
        }

        final class FlatMapPublisherInnerConditionalSubscriber implements ConditionalSubscriber<R> {

            final ConditionalSubscriber<? super R> actual;

            FlatMapPublisherInnerConditionalSubscriber(ConditionalSubscriber<? super R> actual) {
                this.actual = actual;
            }

            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                innerOnSubscribe(subscription);
            }

            @Override
            public void onNext(R item) {
                actual.onNext(item);
            }

            @Override
            public boolean tryOnNext(R item) {
                return actual.tryOnNext(item);
            }

            @Override
            public void onError(Throwable throwable) {
                actual.onError(throwable);
            }

            @Override
            public void onComplete() {
                actual.onComplete();
            }
        }
    }
}
