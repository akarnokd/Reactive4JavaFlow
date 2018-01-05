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
import hu.akarnokd.reactive4javaflow.functionals.CheckedPredicate;
import hu.akarnokd.reactive4javaflow.fused.ConditionalSubscriber;
import hu.akarnokd.reactive4javaflow.impl.*;

import java.lang.invoke.*;
import java.util.concurrent.Flow;

public final class FolyamRetry<T> extends Folyam<T> {

    final Folyam<T> source;

    final long times;

    final CheckedPredicate<? super Throwable> condition;

    public FolyamRetry(Folyam<T> source, long times, CheckedPredicate<? super Throwable> condition) {
        this.source = source;
        this.times = times;
        this.condition = condition;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        AbstractRetrySubscriber parent;
        if (s instanceof ConditionalSubscriber) {
            parent = new RetryConditionalSubscriber<>((ConditionalSubscriber<? super T>)s, times, condition, source);
        } else {
            parent = new RetrySubscriber<>(s, times, condition, source);
        }

        s.onSubscribe(parent);
        parent.subscribeNext();
    }

    static abstract class AbstractRetrySubscriber<T> extends SubscriptionArbiter implements FolyamSubscriber<T> {

        final CheckedPredicate<? super Throwable> condition;

        final FolyamPublisher<T> source;

        long times;

        int wip;
        static final VarHandle WIP = VH.find(MethodHandles.lookup(), AbstractRetrySubscriber.class, "wip", Integer.TYPE);

        long produced;

        AbstractRetrySubscriber(long times, CheckedPredicate<? super Throwable> condition, FolyamPublisher<T> source) {
            this.times = times;
            this.condition = condition;
            this.source = source;
        }

        @Override
        public final void onSubscribe(Flow.Subscription subscription) {
            arbiterReplace(subscription);
        }

        @Override
        public final void onError(Throwable throwable) {
            long t = times;
            if (t != Long.MAX_VALUE) {
                if (--t <= 0) {
                    error(throwable);
                    return;
                }
                times = t;
            }
            boolean b;
            try {
                b = condition.test(throwable);
            } catch (Throwable ex) {
                error(new CompositeThrowable(throwable, ex));
                return;
            }
            if (!b) {
                error(throwable);
                return;
            }
            subscribeNext();
        }

        abstract void error(Throwable ex);

        void subscribeNext() {
            if ((int)WIP.getAndAdd(this, 1) == 0) {
                do {
                    if (arbiterIsCancelled()) {
                        return;
                    }
                    long p = produced;
                    if (p != 0L) {
                        produced = 0L;
                        arbiterProduced(p);
                    }
                    source.subscribe(this);
                } while ((int)WIP.getAndAdd(this, -1) - 1 != 0);
            }
        }
    }

    static final class RetrySubscriber<T> extends AbstractRetrySubscriber<T> {

        final FolyamSubscriber<? super T> actual;

        RetrySubscriber(FolyamSubscriber<? super T> actual, long times, CheckedPredicate<? super Throwable> condition, FolyamPublisher<T> source) {
            super(times, condition, source);
            this.actual = actual;
        }

        @Override
        public void onNext(T item) {
            produced++;
            actual.onNext(item);
        }

        @Override
        void error(Throwable ex) {
            actual.onError(ex);
        }

        @Override
        public void onComplete() {
            actual.onComplete();
        }
    }

    static final class RetryConditionalSubscriber<T> extends AbstractRetrySubscriber<T> implements ConditionalSubscriber<T> {

        final ConditionalSubscriber<? super T> actual;

        RetryConditionalSubscriber(ConditionalSubscriber<? super T> actual, long times, CheckedPredicate<? super Throwable> condition, FolyamPublisher<T> source) {
            super(times, condition, source);
            this.actual = actual;
        }

        @Override
        public void onNext(T item) {
            produced++;
            actual.onNext(item);
        }

        @Override
        public boolean tryOnNext(T item) {
            if (actual.tryOnNext(item)) {
                produced++;
                return true;
            }
            return false;
        }

        @Override
        void error(Throwable ex) {
            actual.onError(ex);
        }

        @Override
        public void onComplete() {
            actual.onComplete();
        }

    }
}
