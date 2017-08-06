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
import hu.akarnokd.reactive4javaflow.fused.ConditionalSubscriber;

import java.util.Objects;
import java.util.concurrent.Flow;

/**
 * Maps each 'rail' of the source ParallelFolyam with a mapper function.
 *
 * @param <T> the input value type
 * @param <R> the output value type
 */
public final class ParallelMap<T, R> extends ParallelFolyam<R> {

    final ParallelFolyam<T> source;

    final CheckedFunction<? super T, ? extends R> mapper;

    public ParallelMap(ParallelFolyam<T> source, CheckedFunction<? super T, ? extends R> mapper) {
        this.source = source;
        this.mapper = mapper;
    }

    @Override
    public void subscribeActual(FolyamSubscriber<? super R>[] subscribers) {

        int n = subscribers.length;
        @SuppressWarnings("unchecked")
        FolyamSubscriber<? super T>[] parents = new FolyamSubscriber[n];

        for (int i = 0; i < n; i++) {
            FolyamSubscriber<? super R> a = subscribers[i];
            if (a instanceof ConditionalSubscriber) {
                parents[i] = new ParallelMapConditionalSubscriber<T, R>((ConditionalSubscriber<? super R>)a, mapper);
            } else {
                parents[i] = new ParallelMapSubscriber<T, R>(a, mapper);
            }
        }

        source.subscribe(parents);
    }

    @Override
    public int parallelism() {
        return source.parallelism();
    }

    static final class ParallelMapSubscriber<T, R> implements FolyamSubscriber<T>, Flow.Subscription {

        final Flow.Subscriber<? super R> actual;

        final CheckedFunction<? super T, ? extends R> mapper;

        Flow.Subscription s;

        boolean done;

        ParallelMapSubscriber(FolyamSubscriber<? super R> actual, CheckedFunction<? super T, ? extends R> mapper) {
            this.actual = actual;
            this.mapper = mapper;
        }

        @Override
        public void request(long n) {
            s.request(n);
        }

        @Override
        public void cancel() {
            s.cancel();
        }

        @Override
        public void onSubscribe(Flow.Subscription s) {
            this.s = s;

            actual.onSubscribe(this);
        }

        @Override
        public void onNext(T t) {
            if (done) {
                return;
            }
            R v;

            try {
                v = Objects.requireNonNull(mapper.apply(t), "The mapper returned a null value");
            } catch (Throwable ex) {
                FolyamPlugins.handleFatal(ex);
                cancel();
                onError(ex);
                return;
            }

            actual.onNext(v);
        }

        @Override
        public void onError(Throwable t) {
            if (done) {
                FolyamPlugins.onError(t);
                return;
            }
            done = true;
            actual.onError(t);
        }

        @Override
        public void onComplete() {
            if (done) {
                return;
            }
            done = true;
            actual.onComplete();
        }

    }
    static final class ParallelMapConditionalSubscriber<T, R> implements ConditionalSubscriber<T>, Flow.Subscription {

        final ConditionalSubscriber<? super R> actual;

        final CheckedFunction<? super T, ? extends R> mapper;

        Flow.Subscription s;

        boolean done;

        ParallelMapConditionalSubscriber(ConditionalSubscriber<? super R> actual, CheckedFunction<? super T, ? extends R> mapper) {
            this.actual = actual;
            this.mapper = mapper;
        }

        @Override
        public void request(long n) {
            s.request(n);
        }

        @Override
        public void cancel() {
            s.cancel();
        }

        @Override
        public void onSubscribe(Flow.Subscription s) {
            this.s = s;

            actual.onSubscribe(this);
        }

        @Override
        public void onNext(T t) {
            if (done) {
                return;
            }
            R v;

            try {
                v = Objects.requireNonNull(mapper.apply(t), "The mapper returned a null value");
            } catch (Throwable ex) {
                FolyamPlugins.handleFatal(ex);
                cancel();
                onError(ex);
                return;
            }

            actual.onNext(v);
        }

        @Override
        public boolean tryOnNext(T t) {
            if (done) {
                return false;
            }
            R v;

            try {
                v = Objects.requireNonNull(mapper.apply(t), "The mapper returned a null value");
            } catch (Throwable ex) {
                FolyamPlugins.handleFatal(ex);
                cancel();
                onError(ex);
                return false;
            }

            return actual.tryOnNext(v);
        }

        @Override
        public void onError(Throwable t) {
            if (done) {
                FolyamPlugins.onError(t);
                return;
            }
            done = true;
            actual.onError(t);
        }

        @Override
        public void onComplete() {
            if (done) {
                return;
            }
            done = true;
            actual.onComplete();
        }

    }
}
