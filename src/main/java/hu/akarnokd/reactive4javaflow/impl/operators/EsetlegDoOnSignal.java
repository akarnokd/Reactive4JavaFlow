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

import java.util.concurrent.Flow;

public final class EsetlegDoOnSignal<T> extends Esetleg<T> {

    final Esetleg<T> source;

    final CheckedConsumer<? super Flow.Subscription> onSubscribe;

    final CheckedConsumer<? super T> onNext;

    final CheckedConsumer<? super T> onAfterNext;

    final CheckedConsumer<? super Throwable> onError;

    final CheckedRunnable onComplete;

    final CheckedConsumer<? super Long> onRequest;

    final CheckedRunnable onCancel;

    public EsetlegDoOnSignal(Esetleg<T> source, CheckedConsumer<? super Flow.Subscription> onSubscribe, CheckedConsumer<? super T> onNext, CheckedConsumer<? super T> onAfterNext, CheckedConsumer<? super Throwable> onError, CheckedRunnable onComplete, CheckedConsumer<? super Long> onRequest, CheckedRunnable onCancel) {
        this.source = source;
        this.onSubscribe = onSubscribe;
        this.onNext = onNext;
        this.onAfterNext = onAfterNext;
        this.onError = onError;
        this.onComplete = onComplete;
        this.onRequest = onRequest;
        this.onCancel = onCancel;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        if (s instanceof ConditionalSubscriber) {
            source.subscribe(new DoOnSignalConditionalSubscriber<>((ConditionalSubscriber<? super T>)s, onSubscribe, onNext, onAfterNext, onError, onComplete, onRequest, onCancel));
        } else {
            source.subscribe(new DoOnSignalSubscriber<>(s, onSubscribe, onNext, onAfterNext, onError, onComplete, onRequest, onCancel));
        }
    }

    public static <T> Esetleg<T> withOnSubscribe(Esetleg<T> source, CheckedConsumer<? super Flow.Subscription> onSubscribe) {
        if (source instanceof EsetlegDoOnSignal) {
            EsetlegDoOnSignal<T> s = (EsetlegDoOnSignal<T>) source;
            if (s.onSubscribe == null) {
                return new EsetlegDoOnSignal<>(s.source,
                        onSubscribe,
                        s.onNext,
                        s.onAfterNext,
                        s.onError,
                        s.onComplete,
                        s.onRequest,
                        s.onCancel);
            }
            return new EsetlegDoOnSignal<>(s.source,
                    v -> { s.onSubscribe.accept(v); onSubscribe.accept(v); },
                    s.onNext,
                    s.onAfterNext,
                    s.onError,
                    s.onComplete,
                    s.onRequest,
                    s.onCancel);
        }
        return new EsetlegDoOnSignal<>(source,
                onSubscribe,
                null,
                null,
                null,
                null,
                null,
                null);
    }

    public static <T> Esetleg<T> withOnNext(Esetleg<T> source, CheckedConsumer<? super T> onNext) {
        if (source instanceof EsetlegDoOnSignal) {
            EsetlegDoOnSignal<T> s = (EsetlegDoOnSignal<T>) source;
            if (s.onNext == null) {
                return new EsetlegDoOnSignal<>(s.source,
                        s.onSubscribe,
                        onNext,
                        s.onAfterNext,
                        s.onError,
                        s.onComplete,
                        s.onRequest,
                        s.onCancel);
            }
            return new EsetlegDoOnSignal<>(s.source,
                    s.onSubscribe,
                    v -> { s.onNext.accept(v); onNext.accept(v); },
                    s.onAfterNext,
                    s.onError,
                    s.onComplete,
                    s.onRequest,
                    s.onCancel);
        }
        return new EsetlegDoOnSignal<>(source,
                null,
                onNext,
                null,
                null,
                null,
                null,
                null);
    }

    public static <T> Esetleg<T> withOnAfterNext(Esetleg<T> source, CheckedConsumer<? super T> onAfterNext) {
        if (source instanceof EsetlegDoOnSignal) {
            EsetlegDoOnSignal<T> s = (EsetlegDoOnSignal<T>) source;
            if (s.onAfterNext == null) {
                return new EsetlegDoOnSignal<>(s.source,
                        s.onSubscribe,
                        s.onNext,
                        onAfterNext,
                        s.onError,
                        s.onComplete,
                        s.onRequest,
                        s.onCancel);
            }
            return new EsetlegDoOnSignal<>(s.source,
                    s.onSubscribe,
                    s.onNext,
                    v -> { s.onAfterNext.accept(v); onAfterNext.accept(v); },
                    s.onError,
                    s.onComplete,
                    s.onRequest,
                    s.onCancel);
        }
        return new EsetlegDoOnSignal<>(source,
                null,
                null,
                onAfterNext,
                null,
                null,
                null,
                null);
    }

    public static <T> Esetleg<T> withOnError(Esetleg<T> source, CheckedConsumer<? super Throwable> onError) {
        if (source instanceof EsetlegDoOnSignal) {
            EsetlegDoOnSignal<T> s = (EsetlegDoOnSignal<T>) source;
            if (s.onError == null) {
                return new EsetlegDoOnSignal<>(s.source,
                        s.onSubscribe,
                        s.onNext,
                        s.onAfterNext,
                        onError,
                        s.onComplete,
                        s.onRequest,
                        s.onCancel);
            }
            return new EsetlegDoOnSignal<>(s.source,
                    s.onSubscribe,
                    s.onNext,
                    s.onAfterNext,
                    v -> { s.onError.accept(v); onError.accept(v); },
                    s.onComplete,
                    s.onRequest,
                    s.onCancel);
        }
        return new EsetlegDoOnSignal<>(source,
                null,
                null,
                null,
                onError,
                null,
                null,
                null);
    }

    public static <T> Esetleg<T> withOnComplete(Esetleg<T> source, CheckedRunnable onComplete) {
        if (source instanceof EsetlegDoOnSignal) {
            EsetlegDoOnSignal<T> s = (EsetlegDoOnSignal<T>) source;
            if (s.onComplete == null) {
                return new EsetlegDoOnSignal<>(s.source,
                        s.onSubscribe,
                        s.onNext,
                        s.onAfterNext,
                        s.onError,
                        onComplete,
                        s.onRequest,
                        s.onCancel);
            }
            return new EsetlegDoOnSignal<>(s.source,
                    s.onSubscribe,
                    s.onNext,
                    s.onAfterNext,
                    s.onError,
                    () -> { s.onComplete.run(); onComplete.run(); },
                    s.onRequest,
                    s.onCancel);
        }
        return new EsetlegDoOnSignal<>(source,
                null,
                null,
                null,
                null,
                onComplete,
                null,
                null);
    }


    public static <T> Esetleg<T> withOnRequest(Esetleg<T> source, CheckedConsumer<? super Long> onRequest) {
        if (source instanceof EsetlegDoOnSignal) {
            EsetlegDoOnSignal<T> s = (EsetlegDoOnSignal<T>) source;
            if (s.onRequest == null) {
                return new EsetlegDoOnSignal<>(s.source,
                        s.onSubscribe,
                        s.onNext,
                        s.onAfterNext,
                        s.onError,
                        s.onComplete,
                        onRequest,
                        s.onCancel);
            }
            return new EsetlegDoOnSignal<>(s.source,
                    s.onSubscribe,
                    s.onNext,
                    s.onAfterNext,
                    s.onError,
                    s.onComplete,
                    v -> { s.onRequest.accept(v); onRequest.accept(v); },
                    s.onCancel);
        }
        return new EsetlegDoOnSignal<>(source,
                null,
                null,
                null,
                null,
                null,
                onRequest,
                null);
    }

    public static <T> Esetleg<T> withOnCancel(Esetleg<T> source, CheckedRunnable onCancel) {
        if (source instanceof EsetlegDoOnSignal) {
            EsetlegDoOnSignal<T> s = (EsetlegDoOnSignal<T>) source;
            if (s.onCancel == null) {
                return new EsetlegDoOnSignal<>(s.source,
                        s.onSubscribe,
                        s.onNext,
                        s.onAfterNext,
                        s.onError,
                        s.onComplete,
                        s.onRequest,
                        onCancel);
            }
            return new EsetlegDoOnSignal<>(s.source,
                    s.onSubscribe,
                    s.onNext,
                    s.onAfterNext,
                    s.onError,
                    s.onComplete,
                    s.onRequest,
                    () -> { s.onCancel.run(); onCancel.run(); });
        }
        return new EsetlegDoOnSignal<>(source,
                null,
                null,
                null,
                null,
                null,
                null,
                onCancel);
    }

    static abstract class AbstractDoOnSignal<T> implements FusedSubscription<T> {

        final CheckedConsumer<? super Flow.Subscription> onSubscribe;

        final CheckedConsumer<? super T> onNext;

        final CheckedConsumer<? super T> onAfterNext;

        final CheckedConsumer<? super Throwable> onError;

        final CheckedRunnable onComplete;

        final CheckedConsumer<? super Long> onRequest;

        final CheckedRunnable onCancel;

        Flow.Subscription upstream;

        FusedSubscription<T> qs;

        int sourceFused;

        boolean done;

        protected AbstractDoOnSignal(CheckedConsumer<? super Flow.Subscription> onSubscribe,
                                     CheckedConsumer<? super T> onNext,
                                     CheckedConsumer<? super T> onAfterNext,
                                     CheckedConsumer<? super Throwable> onError,
                                     CheckedRunnable onComplete,
                                     CheckedConsumer<? super Long> onRequest,
                                     CheckedRunnable onCancel) {
            this.onSubscribe = onSubscribe != null ? onSubscribe : e -> { };
            this.onNext = onNext != null ? onNext : e -> { };
            this.onAfterNext = onAfterNext != null ? onAfterNext : e -> { };
            this.onError = onError != null ? onError : e -> { };
            this.onComplete = onComplete != null ? onComplete : () -> { };
            this.onRequest = onRequest != null ? onRequest : e -> { };
            this.onCancel = onCancel != null ? onCancel : () -> { };
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
        public final int requestFusion(int mode) {
            FusedSubscription<T> fs = qs;
            if (fs != null && (mode & BOUNDARY) == 0) {
                int m = fs.requestFusion(mode);
                sourceFused = m;
                return m;
            }
            return NONE;
        }

        @Override
        public final T poll() throws Throwable {
            T v;

            try {
                v = qs.poll();
            } catch (Throwable ex) {
                try {
                    onError.accept(ex);
                } catch (Throwable exc) {
                    ex = new CompositeThrowable(ex, exc);
                }
                throw ex;
            }

            if (v == null) {
                if (sourceFused == SYNC) {
                    onComplete.run();
                }
            } else {
                onNext.accept(v);
                onAfterNext.accept(v);
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
        public final void cancel() {
            try {
                onCancel.run();
            } catch (Throwable ex) {
                FolyamPlugins.onError(ex);
            }
            upstream.cancel();
        }

        @Override
        public final void request(long n) {
            try {
                onRequest.accept(n);
            } catch (Throwable ex) {
                FolyamPlugins.onError(ex);
            }
            upstream.request(n);
        }
    }

    static final class DoOnSignalSubscriber<T> extends AbstractDoOnSignal<T> implements FolyamSubscriber<T> {

        final FolyamSubscriber<? super T> actual;

        protected DoOnSignalSubscriber(FolyamSubscriber<? super T> actual,
                                       CheckedConsumer<? super Flow.Subscription> onSubscribe,
                                       CheckedConsumer<? super T> onNext,
                                       CheckedConsumer<? super T> onAfterNext,
                                       CheckedConsumer<? super Throwable> onError,
                                       CheckedRunnable onComplete,
                                       CheckedConsumer<? super Long> onRequest,
                                       CheckedRunnable onCancel) {
            super(onSubscribe, onNext, onAfterNext, onError, onComplete, onRequest, onCancel);
            this.actual = actual;
        }

        @Override
        void onStart() {
            try {
                onSubscribe.accept(upstream);
            } catch (Throwable ex) {
                upstream.cancel();
                actual.onSubscribe(EmptySubscription.INSTANCE);
                onError(ex);
                return;
            }
            actual.onSubscribe(this);
        }

        @Override
        public void onNext(T item) {
            if (done) {
                return;
            }
            if (item == null) {
                actual.onNext(null);
            } else {
                try {
                    onNext.accept(item);
                } catch (Throwable ex) {
                    cancel();
                    onError(ex);
                    return;
                }

                actual.onNext(item);

                try {
                    onAfterNext.accept(item);
                } catch (Throwable ex) {
                    cancel();
                    onError(ex);
                }
            }
        }

        @Override
        public void onError(Throwable throwable) {
            if (done) {
                FolyamPlugins.onError(throwable);
                return;
            }
            done = true;
            try {
                onError.accept(throwable);
            } catch (Throwable ex) {
                throwable = new CompositeThrowable(throwable, ex);
            }

            actual.onError(throwable);
        }

        @Override
        public void onComplete() {
            if (done) {
                return;
            }
            done = true;
            try {
                onComplete.run();
            } catch (Throwable ex) {
                actual.onError(ex);
                return;
            }

            actual.onComplete();
        }
    }


    static final class DoOnSignalConditionalSubscriber<T> extends AbstractDoOnSignal<T> implements ConditionalSubscriber<T> {

        final ConditionalSubscriber<? super T> actual;

        protected DoOnSignalConditionalSubscriber(ConditionalSubscriber<? super T> actual,
                                       CheckedConsumer<? super Flow.Subscription> onSubscribe,
                                       CheckedConsumer<? super T> onNext,
                                       CheckedConsumer<? super T> onAfterNext,
                                       CheckedConsumer<? super Throwable> onError,
                                       CheckedRunnable onComplete,
                                       CheckedConsumer<? super Long> onRequest,
                                       CheckedRunnable onCancel) {
            super(onSubscribe, onNext, onAfterNext, onError, onComplete, onRequest, onCancel);
            this.actual = actual;
        }

        @Override
        void onStart() {
            try {
                onSubscribe.accept(upstream);
            } catch (Throwable ex) {
                upstream.cancel();
                actual.onSubscribe(EmptySubscription.INSTANCE);
                onError(ex);
                return;
            }
            actual.onSubscribe(this);
        }


        @Override
        public void onNext(T item) {
            if (done) {
                return;
            }
            if (item == null) {
                actual.onNext(null);
            } else {
                try {
                    onNext.accept(item);
                } catch (Throwable ex) {
                    cancel();
                    onError(ex);
                    return;
                }

                actual.onNext(item);

                try {
                    onAfterNext.accept(item);
                } catch (Throwable ex) {
                    cancel();
                    onError(ex);
                }
            }
        }

        @Override
        public boolean tryOnNext(T item) {
            if (done) {
                return false;
            }
            if (item == null) {
                return actual.tryOnNext(null);
            }
            try {
                onNext.accept(item);
            } catch (Throwable ex) {
                cancel();
                onError(ex);
                return false;
            }

            boolean b = actual.tryOnNext(item);

            try {
                onAfterNext.accept(item);
            } catch (Throwable ex) {
                cancel();
                onError(ex);
            }
            return b;
        }

        @Override
        public void onError(Throwable throwable) {
            if (done) {
                FolyamPlugins.onError(throwable);
                return;
            }
            done = true;
            try {
                onError.accept(throwable);
            } catch (Throwable ex) {
                throwable = new CompositeThrowable(throwable, ex);
            }

            actual.onError(throwable);
        }

        @Override
        public void onComplete() {
            if (done) {
                return;
            }
            done = true;
            try {
                onComplete.run();
            } catch (Throwable ex) {
                actual.onError(ex);
                return;
            }

            actual.onComplete();
        }
    }
}
