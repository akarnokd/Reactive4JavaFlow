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
import java.util.*;
import java.util.concurrent.Flow;
import java.util.stream.Stream;

public final class EsetlegFlatMapStream<T, R> extends Folyam<R> {

    final Esetleg<T> source;

    final CheckedFunction<? super T, ? extends Stream<? extends R>> mapper;

    public EsetlegFlatMapStream(Esetleg<T> source, CheckedFunction<? super T, ? extends Stream<? extends R>> mapper) {
        this.source = source;
        this.mapper = mapper;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super R> s) {
        if (!tryScalarXMap(source, s, mapper)) {
            source.subscribe(new FlatMapIterableSubscriber<>(s, mapper));
        }
    }

    public static <T, R> boolean tryScalarXMap(FolyamPublisher<T> source, FolyamSubscriber<? super R> s, CheckedFunction<? super T, ? extends Stream<? extends R>> mapper) {
        if (source instanceof FusedDynamicSource) {
            FusedDynamicSource<T> f = (FusedDynamicSource<T>) source;
            Stream<? extends R> e = null;
            Iterator<? extends R> it = null;
            boolean hasValue = false;

            try {
                T v = f.value();
                if (v != null) {
                    e = Objects.requireNonNull(mapper.apply(v), "The mapper returned a null Stream");
                    it = e.iterator();
                    hasValue = it.hasNext();
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

            if (hasValue) {
                if (s instanceof ConditionalSubscriber) {
                    s.onSubscribe(new FolyamStream.StreamConditionalSubscription<>((ConditionalSubscriber<? super R>)s, it, e));
                } else {
                    s.onSubscribe(new FolyamStream.StreamSubscription<>(s, it, e));
                }
            } else {
                EmptySubscription.complete(s);
            }

            return true;
        }
        return false;
    }

    static final class FlatMapIterableSubscriber<T, R> implements FolyamSubscriber<T>, Flow.Subscription {

        final FolyamSubscriber<? super R> actual;

        final CheckedFunction<? super T, ? extends Stream<? extends R>> mapper;

        Flow.Subscription upstream;
        static final VarHandle UPSTREAM = VH.find(MethodHandles.lookup(), FlatMapIterableSubscriber.class, "upstream", Flow.Subscription.class);

        Flow.Subscription innerUpstream;
        static final VarHandle INNER_UPSTREAM = VH.find(MethodHandles.lookup(), FlatMapIterableSubscriber.class, "innerUpstream", Flow.Subscription.class);

        long requested;
        static final VarHandle REQUESTED = VH.find(MethodHandles.lookup(), FlatMapIterableSubscriber.class, "requested", long.class);

        boolean done;

        FlatMapIterableSubscriber(FolyamSubscriber<? super R> actual, CheckedFunction<? super T, ? extends Stream<? extends R>> mapper) {
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
            Stream<? extends R> s;
            Iterator<? extends R> p;
            boolean hasValue;
            try {
                s = Objects.requireNonNull(mapper.apply(item), "The mapper returned a null Flow.Publisher");
                p = s.iterator();
                hasValue = p.hasNext();
            } catch (Throwable ex) {
                FolyamPlugins.handleFatal(ex);
                actual.onError(ex);
                return;
            }
            if (hasValue) {
                if (actual instanceof ConditionalSubscriber) {
                    innerOnSubscribe(new FolyamStream.StreamConditionalSubscription<>((ConditionalSubscriber<? super R>)actual, p, s));
                } else {
                    innerOnSubscribe(new FolyamStream.StreamSubscription<>(actual, p, s));
                }
            } else {
                actual.onComplete();
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

    }
}
