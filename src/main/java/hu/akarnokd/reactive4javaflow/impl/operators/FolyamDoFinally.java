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
import hu.akarnokd.reactive4javaflow.functionals.CheckedRunnable;
import hu.akarnokd.reactive4javaflow.fused.*;

import java.lang.invoke.*;
import java.util.concurrent.Flow;

public final class FolyamDoFinally<T> extends Folyam<T> {

    final Folyam<T> source;

    final CheckedRunnable onFinally;

    final SchedulerService executor;

    public FolyamDoFinally(Folyam<T> source, CheckedRunnable onFinally, SchedulerService executor) {
        this.source = source;
        this.onFinally = onFinally;
        this.executor = executor;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        if (s instanceof ConditionalSubscriber) {
            source.subscribe(new DoFinallyConditionalSubscriber<>((ConditionalSubscriber<? super T>)s, onFinally, executor));
        } else {
            source.subscribe(new DoFinallySubscriber<>(s, onFinally, executor));
        }
    }

    static abstract class AbstractDoFinallySubscriber<T> implements FusedSubscription<T>, Runnable {

        final CheckedRunnable onFinally;

        final SchedulerService executor;

        Flow.Subscription upstream;

        FusedSubscription<T> qs;

        boolean once;
        static final VarHandle ONCE;

        int fusionMode;

        static {
            try {
                ONCE = MethodHandles.lookup().findVarHandle(AbstractDoFinallySubscriber.class, "once", Boolean.TYPE);
            } catch (Throwable ex) {
                throw new InternalError(ex);
            }
        }

        AbstractDoFinallySubscriber(CheckedRunnable onFinally, SchedulerService executor) {
            this.onFinally = onFinally;
            this.executor = executor;
        }

        public final void onSubscribe(Flow.Subscription subscription) {
            this.upstream = subscription;
            if (subscription instanceof FusedSubscription) {
                this.qs = (FusedSubscription<T>)subscription;
            }
            onStart();
        }

        abstract void onStart();

        @Override
        public final T poll() throws Throwable {
            T v = qs.poll();
            if (v == null && fusionMode == SYNC) {
                finish();
            }
            return v;
        }

        @Override
        public final boolean isEmpty() {
            return qs.isEmpty();
        }

        @Override
        public final void clear() {
            qs.clear();
        }

        @Override
        public final int requestFusion(int mode) {
            FusedSubscription<T> fs = qs;
            if (fs != null && (mode & BOUNDARY) == 0) {
                int m = fs.requestFusion(mode);
                fusionMode = m;
                return m;
            }
            return NONE;
        }

        @Override
        public final void request(long n) {
            upstream.request(n);
        }

        @Override
        public final void cancel() {
            upstream.cancel();
            finish();
        }

        final void finish() {
            if (ONCE.compareAndSet(this, false, true)) {
                executor.schedule(this);
            }
        }

        @Override
        public void run() {
            try {
                onFinally.run();
            } catch (Throwable ex) {
                FolyamPlugins.onError(ex);
            }
        }
    }

    static final class DoFinallySubscriber<T> extends AbstractDoFinallySubscriber<T> implements FolyamSubscriber<T> {

        final FolyamSubscriber<? super T> actual;

        DoFinallySubscriber(FolyamSubscriber<? super T> actual, CheckedRunnable onFinally, SchedulerService executor) {
            super(onFinally, executor);
            this.actual = actual;
        }

        @Override
        void onStart() {
            actual.onSubscribe(this);
        }

        @Override
        public void onNext(T item) {
            actual.onNext(item);
        }

        @Override
        public void onError(Throwable throwable) {
            actual.onError(throwable);
            finish();
        }

        @Override
        public void onComplete() {
            actual.onComplete();
            finish();
        }
    }

    static final class DoFinallyConditionalSubscriber<T> extends AbstractDoFinallySubscriber<T> implements ConditionalSubscriber<T> {

        final ConditionalSubscriber<? super T> actual;

        DoFinallyConditionalSubscriber(ConditionalSubscriber<? super T> actual, CheckedRunnable onFinally, SchedulerService executor) {
            super(onFinally, executor);
            this.actual = actual;
        }

        @Override
        void onStart() {
            actual.onSubscribe(this);
        }

        @Override
        public void onNext(T item) {
            actual.onNext(item);
        }

        @Override
        public boolean tryOnNext(T item) {
            return actual.tryOnNext(item);
        }

        @Override
        public void onError(Throwable throwable) {
            actual.onError(throwable);
            finish();
        }

        @Override
        public void onComplete() {
            actual.onComplete();
            finish();
        }
    }
}
