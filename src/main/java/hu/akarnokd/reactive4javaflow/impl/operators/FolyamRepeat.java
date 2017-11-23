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
import hu.akarnokd.reactive4javaflow.functionals.CheckedBooleanSupplier;
import hu.akarnokd.reactive4javaflow.fused.ConditionalSubscriber;
import hu.akarnokd.reactive4javaflow.impl.*;

import java.lang.invoke.*;
import java.util.concurrent.Flow;

public final class FolyamRepeat<T> extends Folyam<T> {

    final FolyamPublisher<T> source;

    final long times;

    final CheckedBooleanSupplier condition;

    public FolyamRepeat(FolyamPublisher<T> source, long times, CheckedBooleanSupplier condition) {
        this.source = source;
        this.times = times;
        this.condition = condition;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        AbstractRepeatSubscriber parent;
        if (s instanceof ConditionalSubscriber) {
            parent = new RepeatConditionalSubscriber<>((ConditionalSubscriber<? super T>)s, times, condition, source);
        } else {
            parent = new RepeatSubscriber<>(s, times, condition, source);
        }

        s.onSubscribe(parent);
        parent.subscribeNext();
    }

    static abstract class AbstractRepeatSubscriber<T> extends SubscriptionArbiter implements FolyamSubscriber<T> {

        final CheckedBooleanSupplier condition;

        final FolyamPublisher<T> source;

        long times;

        int wip;
        static final VarHandle WIP = VH.find(MethodHandles.lookup(), AbstractRepeatSubscriber.class, "wip", Integer.TYPE);

        long produced;

        AbstractRepeatSubscriber(long times, CheckedBooleanSupplier condition, FolyamPublisher<T> source) {
            this.times = times;
            this.condition = condition;
            this.source = source;
        }

        @Override
        public final void onSubscribe(Flow.Subscription subscription) {
            arbiterReplace(subscription);
        }

        @Override
        public final void onComplete() {
            long t = times;
            if (t != Long.MAX_VALUE) {
                if (--t <= 0) {
                    complete();
                    return;
                }
                times = t;
            }
            boolean b;
            try {
                b = condition.getAsBoolean();
            } catch (Throwable ex) {
                onError(ex);
                return;
            }
            if (!b) {
                complete();
                return;
            }
            subscribeNext();
        }

        abstract void complete();

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

    static final class RepeatSubscriber<T> extends AbstractRepeatSubscriber<T> {

        final FolyamSubscriber<? super T> actual;

        RepeatSubscriber(FolyamSubscriber<? super T> actual, long times, CheckedBooleanSupplier condition, FolyamPublisher<T> source) {
            super(times, condition, source);
            this.actual = actual;
        }

        @Override
        void complete() {
            actual.onComplete();
        }

        @Override
        public void onNext(T item) {
            produced++;
            actual.onNext(item);
        }

        @Override
        public void onError(Throwable throwable) {
            actual.onError(throwable);
        }
    }

    static final class RepeatConditionalSubscriber<T> extends AbstractRepeatSubscriber<T> implements ConditionalSubscriber<T> {

        final ConditionalSubscriber<? super T> actual;

        RepeatConditionalSubscriber(ConditionalSubscriber<? super T> actual, long times, CheckedBooleanSupplier condition, FolyamPublisher<T> source) {
            super(times, condition, source);
            this.actual = actual;
        }

        @Override
        void complete() {
            actual.onComplete();
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
        public void onError(Throwable throwable) {
            actual.onError(throwable);
        }
    }
}
