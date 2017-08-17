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
import hu.akarnokd.reactive4javaflow.functionals.AutoDisposable;
import hu.akarnokd.reactive4javaflow.impl.SubscriptionHelper;

import java.util.concurrent.Flow;
import java.util.concurrent.atomic.*;

public final class ParallelIgnoreElements<T> extends Esetleg<T> {

    final ParallelFolyam<T> source;

    public ParallelIgnoreElements(ParallelFolyam<T> source) {
        this.source = source;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        IgnoreCoordinator<T> parent = new IgnoreCoordinator<>(s, source.parallelism());
        s.onSubscribe(parent);
        source.subscribe(parent.subscribers);
    }

    static final class IgnoreCoordinator<T> extends AtomicInteger implements Flow.Subscription {

        final FolyamSubscriber<? super T> actual;

        final Throwable[] errors;

        final IgnoreSubscriber[] subscribers;

        boolean hasError;

        IgnoreCoordinator(FolyamSubscriber<? super T> actual, int n) {
            this.actual = actual;
            errors = new Throwable[n];
            subscribers = new IgnoreSubscriber[n];
            for (int i = 0; i < n; i++) {
                subscribers[i] = new IgnoreSubscriber(i, this);
            }
            setRelease(n);
        }

        @Override
        public void request(long n) {
            // this operator never produces any items
        }

        @Override
        public void cancel() {
            for (IgnoreSubscriber s : subscribers) {
                s.close();
            }
        }

        void innerError(int index, Throwable throwable) {
            errors[index] = throwable;
            hasError = true;
            innerComplete();
        }

        void innerComplete() {
            if (decrementAndGet() == 0) {
                if (hasError) {
                    Throwable ex = null;
                    for (Throwable t : errors) {
                        if (t != null) {
                            if (ex instanceof CompositeThrowable) {
                                ((CompositeThrowable) ex).addSuppressed(t);
                            } else if (ex != null && ex != t) {
                                ex = new CompositeThrowable(ex, t);
                            } else {
                                ex = t;
                            }
                        }
                    }
                    actual.onError(ex);
                } else {
                    actual.onComplete();
                }
            }
        }

        static final class IgnoreSubscriber extends AtomicReference<Flow.Subscription> implements FolyamSubscriber<Object>, AutoDisposable {

            final int index;

            final IgnoreCoordinator<?> parent;

            IgnoreSubscriber(int index, IgnoreCoordinator<?> parent) {
                this.index = index;
                this.parent = parent;
            }

            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                if (SubscriptionHelper.replace(this, subscription)) {
                    subscription.request(Long.MAX_VALUE);
                }
            }

            @Override
            public void onNext(Object item) {
                // deliberately ignored
            }

            @Override
            public void onError(Throwable throwable) {
                parent.innerError(index, throwable);
            }

            @Override
            public void onComplete() {
                parent.innerComplete();
            }

            @Override
            public void close() {
                SubscriptionHelper.cancel(this);
            }
        }
    }
}
