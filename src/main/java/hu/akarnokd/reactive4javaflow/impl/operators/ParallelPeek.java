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
import hu.akarnokd.reactive4javaflow.impl.EmptySubscription;

import java.util.Objects;
import java.util.concurrent.Flow;

/**
 * Execute a Consumer in each 'rail' for the current element passing through.
 *
 * @param <T> the value type
 */
public final class ParallelPeek<T> extends ParallelFolyam<T> {

    final ParallelFolyam<T> source;

    final CheckedConsumer<? super T> onNext;
    final CheckedConsumer<? super T> onAfterNext;
    final CheckedConsumer<? super Throwable> onError;
    final CheckedRunnable onComplete;
    final CheckedConsumer<? super Flow.Subscription> onSubscribe;
    final CheckedConsumer<? super Long> onRequest;
    final CheckedRunnable onCancel;

    public ParallelPeek(ParallelFolyam<T> source,
                        CheckedConsumer<? super T> onNext,
                        CheckedConsumer<? super T> onAfterNext,
                        CheckedConsumer<? super Throwable> onError,
                        CheckedRunnable onComplete,
                        CheckedConsumer<? super Flow.Subscription> onSubscribe,
                        CheckedConsumer<? super Long> onRequest,
                        CheckedRunnable onCancel
    ) {
        this.source = source;

        this.onNext = Objects.requireNonNull(onNext, "onNext is null");
        this.onAfterNext = Objects.requireNonNull(onAfterNext, "onAfterNext is null");
        this.onError = Objects.requireNonNull(onError, "onError is null");
        this.onComplete = Objects.requireNonNull(onComplete, "onComplete is null");
        this.onSubscribe = Objects.requireNonNull(onSubscribe, "onSubscribe is null");
        this.onRequest = Objects.requireNonNull(onRequest, "onRequest is null");
        this.onCancel = Objects.requireNonNull(onCancel, "onCancel is null");
    }

    @Override
    public void subscribeActual(FolyamSubscriber<? super T>[] subscribers) {

        int n = subscribers.length;
        @SuppressWarnings("unchecked")
        FolyamSubscriber<? super T>[] parents = new FolyamSubscriber[n];

        for (int i = 0; i < n; i++) {
            parents[i] = new ParallelPeekSubscriber<T>(subscribers[i], this);
        }

        source.subscribe(parents);
    }

    @Override
    public int parallelism() {
        return source.parallelism();
    }

    static final class ParallelPeekSubscriber<T> implements FolyamSubscriber<T>, Flow.Subscription {

        final FolyamSubscriber<? super T> actual;

        final ParallelPeek<T> parent;

        Flow.Subscription s;

        boolean done;

        ParallelPeekSubscriber(FolyamSubscriber<? super T> actual, ParallelPeek<T> parent) {
            this.actual = actual;
            this.parent = parent;
        }

        @Override
        public void request(long n) {
            try {
                parent.onRequest.accept(n);
            } catch (Throwable ex) {
                FolyamPlugins.handleFatal(ex);
                FolyamPlugins.onError(ex);
            }
            s.request(n);
        }

        @Override
        public void cancel() {
            try {
                parent.onCancel.run();
            } catch (Throwable ex) {
                FolyamPlugins.handleFatal(ex);
                FolyamPlugins.onError(ex);
            }
            s.cancel();
        }

        @Override
        public void onSubscribe(Flow.Subscription s) {
            this.s = s;

            try {
                parent.onSubscribe.accept(s);
            } catch (Throwable ex) {
                FolyamPlugins.handleFatal(ex);
                s.cancel();
                actual.onSubscribe(EmptySubscription.INSTANCE);
                onError(ex);
                return;
            }

            actual.onSubscribe(this);
        }

        @Override
        public void onNext(T t) {
            if (!done) {
                try {
                    parent.onNext.accept(t);
                } catch (Throwable ex) {
                    FolyamPlugins.handleFatal(ex);
                    onError(ex);
                    return;
                }

                actual.onNext(t);

                try {
                    parent.onAfterNext.accept(t);
                } catch (Throwable ex) {
                    FolyamPlugins.handleFatal(ex);
                    onError(ex);
                }
            }
        }

        @Override
        public void onError(Throwable t) {
            if (done) {
                FolyamPlugins.onError(t);
                return;
            }
            done = true;

            try {
                parent.onError.accept(t);
            } catch (Throwable ex) {
                FolyamPlugins.handleFatal(ex);
                t = new CompositeThrowable(t, ex);
            }
            actual.onError(t);
        }

        @Override
        public void onComplete() {
            if (!done) {
                done = true;
                try {
                    parent.onComplete.run();
                } catch (Throwable ex) {
                    FolyamPlugins.handleFatal(ex);
                    actual.onError(ex);
                    return;
                }
                actual.onComplete();
            }
        }
    }
}
