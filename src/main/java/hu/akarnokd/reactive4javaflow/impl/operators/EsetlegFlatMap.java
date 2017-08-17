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
import hu.akarnokd.reactive4javaflow.fused.FusedDynamicSource;
import hu.akarnokd.reactive4javaflow.impl.*;

import java.lang.invoke.*;
import java.util.Objects;
import java.util.concurrent.Flow;

public final class EsetlegFlatMap<T, R> extends Esetleg<R> {

    final Esetleg<T> source;

    final CheckedFunction<? super T, ? extends Esetleg<? extends R>> mapper;

    public EsetlegFlatMap(Esetleg<T> source, CheckedFunction<? super T, ? extends Esetleg<? extends R>> mapper) {
        this.source = source;
        this.mapper = mapper;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super R> s) {
        if (!tryScalarXMap(source, s, mapper)) {
            source.subscribe(new FlatMapSubscriber<>(s, mapper));
        }
    }

    public static <T, R> boolean tryScalarXMap(FolyamPublisher<T> source, FolyamSubscriber<? super R> s, CheckedFunction<? super T, ? extends FolyamPublisher<? extends R>> mapper) {
        if (source instanceof FusedDynamicSource) {
            FusedDynamicSource<T> f = (FusedDynamicSource<T>) source;
            FolyamPublisher<? extends R> e = null;

            try {
                T v = f.value();
                if (v != null) {
                    e = Objects.requireNonNull(mapper.apply(v), "The mapper returned a null FolyamPublisher");
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

    static final class FlatMapSubscriber<T, R> extends DeferredScalarSubscription<R> implements FolyamSubscriber<T> {

        final CheckedFunction<? super T, ? extends Esetleg<? extends R>> mapper;

        Flow.Subscription upstream;
        static final VarHandle UPSTREAM = VH.find(MethodHandles.lookup(), FlatMapSubscriber.class, "upstream", Flow.Subscription.class);

        boolean done;

        public FlatMapSubscriber(FolyamSubscriber<? super R> actual, CheckedFunction<? super T, ? extends Esetleg<? extends R>> mapper) {
            super(actual);
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
                error(ex);
                return;
            }
            if (p instanceof FusedDynamicSource) {
                FusedDynamicSource<R> f = (FusedDynamicSource<R>) p;
                R w;
                try {
                    w = f.value();
                } catch (Throwable ex) {
                    error(ex);
                    return;
                }
                if (w == null) {
                    complete();
                } else {
                    complete(w);
                }
            } else {
                p.subscribe(new FlatMapInnerSubscriber());
            }
        }

        @Override
        public void onError(Throwable throwable) {
            error(throwable);
        }

        @Override
        public void onComplete() {
            if (!done) {
                done = true;
                complete();
            }
        }

        @Override
        public void cancel() {
            super.cancel();
            SubscriptionHelper.cancel(this, UPSTREAM);
        }

        void innerOnSubscribe(Flow.Subscription s) {
            if (SubscriptionHelper.replace(this, UPSTREAM, s)) {
                s.request(Long.MAX_VALUE);
            }
        }

        final class FlatMapInnerSubscriber implements FolyamSubscriber<R> {

            boolean done;

            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                innerOnSubscribe(subscription);
            }

            @Override
            public void onNext(R item) {
                done = true;
                complete(item);
            }

            @Override
            public void onError(Throwable throwable) {
                error(throwable);
            }

            @Override
            public void onComplete() {
                if (!done) {
                    done = true;
                    complete();
                }
            }
        }
    }
}
