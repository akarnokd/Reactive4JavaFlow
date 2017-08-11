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
import hu.akarnokd.reactive4javaflow.errors.CompositeThrowable;
import hu.akarnokd.reactive4javaflow.functionals.*;
import hu.akarnokd.reactive4javaflow.fused.ConditionalSubscriber;

import java.util.Objects;
import java.util.concurrent.Flow;

/**
 * Calls a Consumer for each upstream value passing by
 * and handles any failure with a handler function.
 *
 * @param <T> the input value type
 * @since 2.0.8 - experimental
 */
public final class ParallelDoOnNextTry<T> extends ParallelFolyam<T> {

    final ParallelFolyam<T> source;

    final CheckedConsumer<? super T> onNext;

    final CheckedBiFunction<? super Long, ? super Throwable, ParallelFailureHandling> errorHandler;

    public ParallelDoOnNextTry(ParallelFolyam<T> source, CheckedConsumer<? super T> onNext,
            CheckedBiFunction<? super Long, ? super Throwable, ParallelFailureHandling> errorHandler) {
        this.source = source;
        this.onNext = onNext;
        this.errorHandler = errorHandler;
    }

    @Override
    public void subscribeActual(FolyamSubscriber<? super T>[] subscribers) {
        int n = subscribers.length;
        @SuppressWarnings("unchecked")
        FolyamSubscriber<? super T>[] parents = new FolyamSubscriber[n];

        for (int i = 0; i < n; i++) {
            FolyamSubscriber<? super T> a = subscribers[i];
            if (a instanceof ConditionalSubscriber) {
                parents[i] = new ParallelDoOnNextConditionalSubscriber<>((ConditionalSubscriber<? super T>) a, onNext, errorHandler);
            } else {
                parents[i] = new ParallelDoOnNextSubscriber<>(a, onNext, errorHandler);
            }
        }

        source.subscribe(parents);
    }

    @Override
    public int parallelism() {
        return source.parallelism();
    }

    static final class ParallelDoOnNextSubscriber<T> implements ConditionalSubscriber<T>, Flow.Subscription {

        final Flow.Subscriber<? super T> actual;

        final CheckedConsumer<? super T> onNext;

        final CheckedBiFunction<? super Long, ? super Throwable, ParallelFailureHandling> errorHandler;

        Flow.Subscription upstream;

        boolean done;

        ParallelDoOnNextSubscriber(FolyamSubscriber<? super T> actual, CheckedConsumer<? super T> onNext,
                CheckedBiFunction<? super Long, ? super Throwable, ParallelFailureHandling> errorHandler) {
            this.actual = actual;
            this.onNext = onNext;
            this.errorHandler = errorHandler;
        }

        @Override
        public void request(long n) {
            upstream.request(n);
        }

        @Override
        public void cancel() {
            upstream.cancel();
        }

        @Override
        public void onSubscribe(Flow.Subscription s) {
            this.upstream = s;

            actual.onSubscribe(this);
        }

        @Override
        public void onNext(T t) {
            if (!tryOnNext(t) && !done) {
                upstream.request(1);
            }
        }

        @Override
        public boolean tryOnNext(T t) {
            if (done) {
                return false;
            }
            long retries = 0;

            for (;;) {
                try {
                    onNext.accept(t);
                } catch (Throwable ex) {
                    FolyamPlugins.handleFatal(ex);

                    ParallelFailureHandling h;

                    try {
                        h = Objects.requireNonNull(errorHandler.apply(++retries, ex), "The errorHandler returned a null item");
                    } catch (Throwable exc) {
                        FolyamPlugins.handleFatal(exc);
                        cancel();
                        onError(new CompositeThrowable(ex, exc));
                        return false;
                    }

                    switch (h) {
                    case RETRY:
                        continue;
                    case SKIP:
                        return false;
                    case STOP:
                        cancel();
                        onComplete();
                        return false;
                    default:
                        cancel();
                        onError(ex);
                        return false;
                    }
                }

                actual.onNext(t);
                return true;
            }
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
    static final class ParallelDoOnNextConditionalSubscriber<T> implements ConditionalSubscriber<T>, Flow.Subscription {

        final ConditionalSubscriber<? super T> actual;

        final CheckedConsumer<? super T> onNext;

        final CheckedBiFunction<? super Long, ? super Throwable, ParallelFailureHandling> errorHandler;

        Flow.Subscription upstream;

        boolean done;

        ParallelDoOnNextConditionalSubscriber(ConditionalSubscriber<? super T> actual,
                CheckedConsumer<? super T> onNext,
                CheckedBiFunction<? super Long, ? super Throwable, ParallelFailureHandling> errorHandler) {
            this.actual = actual;
            this.onNext = onNext;
            this.errorHandler = errorHandler;
        }

        @Override
        public void request(long n) {
            upstream.request(n);
        }

        @Override
        public void cancel() {
            upstream.cancel();
        }

        @Override
        public void onSubscribe(Flow.Subscription s) {
            this.upstream = s;

            actual.onSubscribe(this);
        }

        @Override
        public void onNext(T t) {
            if (!tryOnNext(t) && !done) {
                upstream.request(1);
            }
        }

        @Override
        public boolean tryOnNext(T t) {
            if (done) {
                return false;
            }
            long retries = 0;

            for (;;) {
                try {
                    onNext.accept(t);
                } catch (Throwable ex) {
                    FolyamPlugins.handleFatal(ex);

                    ParallelFailureHandling h;

                    try {
                        h = Objects.requireNonNull(errorHandler.apply(++retries, ex), "The errorHandler returned a null item");
                    } catch (Throwable exc) {
                        FolyamPlugins.handleFatal(exc);
                        cancel();
                        onError(new CompositeThrowable(ex, exc));
                        return false;
                    }

                    switch (h) {
                    case RETRY:
                        continue;
                    case SKIP:
                        return false;
                    case STOP:
                        cancel();
                        onComplete();
                        return false;
                    default:
                        cancel();
                        onError(ex);
                        return false;
                    }
                }

                return actual.tryOnNext(t);
            }
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
