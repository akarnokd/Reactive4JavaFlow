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
import hu.akarnokd.reactive4javaflow.impl.*;

import java.lang.invoke.*;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicReference;

public final class ParallelSumFloat<T> extends Esetleg<Float> {

    final ParallelFolyam<T> source;

    final CheckedFunction<? super T, ? extends Number> valueSelector;

    public ParallelSumFloat(ParallelFolyam<T> source, CheckedFunction<? super T, ? extends Number> valueSelector) {
        this.source = source;
        this.valueSelector = valueSelector;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super Float> s) {
        SumFloatCoordinator<T> parent = new SumFloatCoordinator<>(s, source.parallelism(), valueSelector);
        s.onSubscribe(parent);
        source.subscribe(parent.subscribers);
    }

    static final class SumFloatCoordinator<T> extends DeferredScalarSubscription<Float> implements Flow.Subscription {

        boolean hasValue;
        final float[] partials;

        final SumFloatInnerSubscriber<T>[] subscribers;

        boolean hasError;
        final Throwable[] errors;

        int n;
        static final VarHandle N = VH.find(MethodHandles.lookup(), SumFloatCoordinator.class, "n", int.class);

        SumFloatCoordinator(FolyamSubscriber<? super Float> actual, int n, CheckedFunction<? super T, ? extends Number> valueSelector) {
            super(actual);
            partials = new float[n];
            errors = new Throwable[n];
            subscribers = new SumFloatInnerSubscriber[n];
            for (int i = 0; i < n; i++) {
                subscribers[i] = new SumFloatInnerSubscriber<>(i, this, valueSelector);
            }
            N.setRelease(this, n);
        }

        @Override
        public void cancel() {
            super.cancel();
            for (SumFloatInnerSubscriber s : subscribers) {
                s.close();
            }
        }

        void innerValue(int index, float sum, boolean hasValue) {
            partials[index] = sum;
            if (hasValue) {
                this.hasValue = true;
            }
            done();
        }

        void innerError(int index, Throwable throwable) {
            errors[index] = throwable;
            hasError = true;
            done();
        }

        void done() {
            if ((int)N.getAndAdd(this, -1) - 1 == 0) {
                if (hasError) {
                    Throwable ex = null;
                    for (Throwable t : errors) {
                        if (t != null) {
                            if (ex instanceof CompositeThrowable) {
                                ex.addSuppressed(t);
                            } else if (ex != null && ex != t) {
                                ex = new CompositeThrowable(ex, t);
                            } else {
                                ex = t;
                            }
                        }
                    }
                    actual.onError(ex);
                } else {
                    if (hasValue) {
                        float s = 0;
                        for (float i : partials) {
                            s += i;
                        }
                        complete(s);
                    } else {
                        complete();
                    }
                }
            }

        }

        static final class SumFloatInnerSubscriber<T> extends AtomicReference<Flow.Subscription> implements FolyamSubscriber<T>, AutoDisposable {

            final int index;

            final SumFloatCoordinator<T> parent;

            final CheckedFunction<? super T, ? extends Number> valueSelector;

            float sum;
            boolean hasValue;

            SumFloatInnerSubscriber(int index, SumFloatCoordinator<T> parent, CheckedFunction<? super T, ? extends Number> valueSelector) {
                this.index = index;
                this.parent = parent;
                this.valueSelector = valueSelector;
            }

            @Override
            public void close() {
                SubscriptionHelper.cancel(this);
            }

            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                if (SubscriptionHelper.replace(this, subscription)) {
                    subscription.request(Long.MAX_VALUE);
                }
            }

            @Override
            public void onNext(T item) {
                if (!hasValue) {
                    hasValue = true;
                }
                try {
                    sum += valueSelector.apply(item).floatValue();
                } catch (Throwable ex) {
                    getPlain().cancel();
                    onError(ex);
                }
            }

            @Override
            public void onError(Throwable throwable) {
                if (getPlain() != SubscriptionHelper.CANCELLED) {
                    setPlain(SubscriptionHelper.CANCELLED);
                    parent.innerError(index, throwable);
                } else {
                    FolyamPlugins.onError(throwable);
                }
            }

            @Override
            public void onComplete() {
                if (getPlain() != SubscriptionHelper.CANCELLED) {
                    setPlain(SubscriptionHelper.CANCELLED);
                    parent.innerValue(index, sum, hasValue);
                }
            }
        }
    }
}
