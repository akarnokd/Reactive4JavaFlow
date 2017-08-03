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
import hu.akarnokd.reactive4javaflow.fused.*;
import hu.akarnokd.reactive4javaflow.impl.EmptySubscription;

import java.lang.invoke.*;
import java.util.Objects;
import java.util.concurrent.*;

public final class FolyamUsing<T, R> extends Folyam<T> {

    final Callable<R> resourceSupplier;

    final CheckedFunction<? super R, ? extends Flow.Publisher<? extends T>> flowSupplier;

    final CheckedConsumer<? super R> resourceCleanup;

    final boolean eager;

    public FolyamUsing(Callable<R> resourceSupplier, CheckedFunction<? super R, ? extends Flow.Publisher<? extends T>> flowSupplier, CheckedConsumer<? super R> resourceCleanup, boolean eager) {
        this.resourceSupplier = resourceSupplier;
        this.flowSupplier = flowSupplier;
        this.resourceCleanup = resourceCleanup;
        this.eager = eager;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        R res;

        try {
            res = resourceSupplier.call();
        } catch (Throwable ex) {
            EmptySubscription.error(s, ex);
            return;
        }

        Flow.Publisher<? extends T> p;

        try {
            p = Objects.requireNonNull(flowSupplier.apply(res), "The flowSupplier returned a null Flow.Publisher");
        } catch (Throwable ex) {
            if (eager) {
                try {
                    resourceCleanup.accept(res);
                } catch (Throwable exc) {
                    ex = new CompositeThrowable(ex, exc);
                }
                EmptySubscription.error(s, ex);
            } else {
                EmptySubscription.error(s, ex);
                try {
                    resourceCleanup.accept(res);
                } catch (Throwable exc) {
                    FolyamPlugins.onError(exc);
                }
            }
            return;
        }

        if (s instanceof ConditionalSubscriber) {
            p.subscribe(new UsingConditionalSubscriber<>((ConditionalSubscriber<? super T>)s, res, resourceCleanup, eager));
        } else {
            p.subscribe(new UsingSubscriber<>(s, res, resourceCleanup, eager));
        }
    }

    static abstract class AbstractUsingSubscriber<T, R> implements FusedSubscription<T> {

        final R res;

        final CheckedConsumer<? super R> resourceCleanup;

        final boolean eager;

        Flow.Subscription upstream;

        FusedSubscription<T> qs;

        int fusionMode;

        boolean once;
        static final VarHandle ONCE;

        static {
            try {
                ONCE = MethodHandles.lookup().findVarHandle(AbstractUsingSubscriber.class, "once", Boolean.TYPE);
            } catch (Throwable ex) {
                throw new InternalError(ex);
            }
        }

        protected AbstractUsingSubscriber(R res, CheckedConsumer<? super R> resourceCleanup, boolean eager) {
            this.res = res;
            this.resourceCleanup = resourceCleanup;
            this.eager = eager;
        }
        @Override
        public final int requestFusion(int mode) {
            FusedSubscription<T> fs = qs;
            if (fs != null) {
                int m = fs.requestFusion(mode);
                fusionMode = m;
                return m;
            }
            return NONE;
        }

        @Override
        public final T poll() throws Throwable {
            return qs.poll();
        }

        @Override
        public final boolean isEmpty() {
            return qs.isEmpty();
        }

        @Override
        public final void clear() {
            qs.clear();
        }

        @SuppressWarnings("unchecked")
        public final void onSubscribe(Flow.Subscription subscription) {
            upstream = subscription;
            if (subscription instanceof FusedSubscription) {
                this.qs = (FusedSubscription<T>)subscription;
            }
            onSubscribe();
        }
        @Override
        public final void request(long n) {
            upstream.request(n);
        }

        @Override
        public final void cancel() {
            if (eager) {
                cleanup();
            }
            upstream.cancel();
            if (!eager) {
                cleanup();
            }
        }

        abstract void onSubscribe();

        final void cleanup() {
            if (ONCE.compareAndSet(this, false, true)) {
                try {
                    resourceCleanup.accept(res);
                } catch (Throwable ex) {
                    FolyamPlugins.onError(ex);
                }
            }
        }
    }

    static final class UsingSubscriber<T, R> extends AbstractUsingSubscriber<T, R> implements FolyamSubscriber<T> {

        final FolyamSubscriber<? super T> actual;

        UsingSubscriber(FolyamSubscriber<? super T> actual, R res, CheckedConsumer<? super R> resourceCleanup, boolean eager) {
            super(res, resourceCleanup, eager);
            this.actual = actual;
        }

        @Override
        void onSubscribe() {
            actual.onSubscribe(this);
        }

        @Override
        public void onNext(T item) {
            actual.onNext(item);
        }

        @Override
        public void onError(Throwable throwable) {
            if (eager) {
                if (ONCE.compareAndSet(this, false, true)) {
                    try {
                        resourceCleanup.accept(res);
                    } catch (Throwable ex) {
                        throwable = new CompositeThrowable(throwable, ex);
                    }
                }
            }
            actual.onError(throwable);
            if (!eager) {
                cleanup();
            }
        }

        @Override
        public void onComplete() {
            if (eager) {
                if (ONCE.compareAndSet(this, false, true)) {
                    try {
                        resourceCleanup.accept(res);
                    } catch (Throwable ex) {
                        actual.onError(ex);
                        return;
                    }
                }
            }
            actual.onComplete();
            if (!eager) {
                cleanup();
            }
        }
    }


    static final class UsingConditionalSubscriber<T, R> extends AbstractUsingSubscriber<T, R> implements ConditionalSubscriber<T> {

        final ConditionalSubscriber<? super T> actual;

        UsingConditionalSubscriber(ConditionalSubscriber<? super T> actual, R res, CheckedConsumer<? super R> resourceCleanup, boolean eager) {
            super(res, resourceCleanup, eager);
            this.actual = actual;
        }

        @Override
        void onSubscribe() {
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
            if (eager) {
                if (ONCE.compareAndSet(this, false, true)) {
                    try {
                        resourceCleanup.accept(res);
                    } catch (Throwable ex) {
                        throwable = new CompositeThrowable(throwable, ex);
                    }
                }
            }
            actual.onError(throwable);
            if (!eager) {
                cleanup();
            }
        }

        @Override
        public void onComplete() {
            if (eager) {
                if (ONCE.compareAndSet(this, false, true)) {
                    try {
                        resourceCleanup.accept(res);
                    } catch (Throwable ex) {
                        actual.onError(ex);
                        return;
                    }
                }
            }
            actual.onComplete();
            if (!eager) {
                cleanup();
            }
        }
    }
}
