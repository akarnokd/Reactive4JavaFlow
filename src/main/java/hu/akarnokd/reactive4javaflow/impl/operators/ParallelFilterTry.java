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
 * Filters each 'rail' of the source ParallelFolyam with a predicate function.
 *
 * @param <T> the input value type
 */
public final class ParallelFilterTry<T> extends ParallelFolyam<T> {

    final ParallelFolyam<T> source;

    final CheckedPredicate<? super T> predicate;

    final CheckedBiFunction<? super Long, ? super Throwable, ParallelFailureHandling> errorHandler;

    public ParallelFilterTry(ParallelFolyam<T> source, CheckedPredicate<? super T> predicate,
                             CheckedBiFunction<? super Long, ? super Throwable, ParallelFailureHandling> errorHandler) {
        this.source = source;
        this.predicate = predicate;
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
                parents[i] = new ParallelFilterConditionalSubscriber<>((ConditionalSubscriber<? super T>) a, predicate, errorHandler);
            } else {
                parents[i] = new ParallelFilterSubscriber<>(a, predicate, errorHandler);
            }
        }

        source.subscribe(parents);
    }

    @Override
    public int parallelism() {
        return source.parallelism();
    }

    abstract static class BaseFilterSubscriber<T> implements ConditionalSubscriber<T>, Flow.Subscription {
        final CheckedPredicate<? super T> predicate;

        final CheckedBiFunction<? super Long, ? super Throwable, ParallelFailureHandling> errorHandler;

        Flow.Subscription s;

        boolean done;

        BaseFilterSubscriber(CheckedPredicate<? super T> predicate, CheckedBiFunction<? super Long, ? super Throwable, ParallelFailureHandling> errorHandler) {
            this.predicate = predicate;
            this.errorHandler = errorHandler;
        }

        @Override
        public final void request(long n) {
            s.request(n);
        }

        @Override
        public final void cancel() {
            s.cancel();
        }

        @Override
        public final void onNext(T t) {
            if (!tryOnNext(t) && !done) {
                s.request(1);
            }
        }
    }

    static final class ParallelFilterSubscriber<T> extends BaseFilterSubscriber<T> {

        final FolyamSubscriber<? super T> actual;

        ParallelFilterSubscriber(FolyamSubscriber<? super T> actual, CheckedPredicate<? super T> predicate, CheckedBiFunction<? super Long, ? super Throwable, ParallelFailureHandling> errorHandler) {
            super(predicate, errorHandler);
            this.actual = actual;
        }

        @Override
        public void onSubscribe(Flow.Subscription s) {
            this.s = s;

            actual.onSubscribe(this);
        }

        @Override
        public boolean tryOnNext(T t) {
            if (!done) {
                long retries = 0L;

                for (;;) {
                    boolean b;

                    try {
                        b = predicate.test(t);
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

                    if (b) {
                        actual.onNext(t);
                        return true;
                    }
                    return false;
                }
            }
            return false;
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
            if (!done) {
                done = true;
                actual.onComplete();
            }
        }
    }

    static final class ParallelFilterConditionalSubscriber<T> extends BaseFilterSubscriber<T> {

        final ConditionalSubscriber<? super T> actual;

        ParallelFilterConditionalSubscriber(ConditionalSubscriber<? super T> actual,
                CheckedPredicate<? super T> predicate,
                CheckedBiFunction<? super Long, ? super Throwable, ParallelFailureHandling> errorHandler) {
            super(predicate, errorHandler);
            this.actual = actual;
        }

        @Override
        public void onSubscribe(Flow.Subscription s) {
            this.s = s;

            actual.onSubscribe(this);
        }

        @Override
        public boolean tryOnNext(T t) {
            if (!done) {
                long retries = 0L;

                for (;;) {
                    boolean b;

                    try {
                        b = predicate.test(t);
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

                    return b && actual.tryOnNext(t);
                }
            }
            return false;
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
            if (!done) {
                done = true;
                actual.onComplete();
            }
        }
    }}
