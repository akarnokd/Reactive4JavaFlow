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
import hu.akarnokd.reactive4javaflow.fused.FusedSubscription;
import hu.akarnokd.reactive4javaflow.impl.*;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

public final class FolyamGenerate<T, S> extends Folyam<T> {

    final Callable<S> stateSupplier;

    final CheckedBiFunction<S, Emitter<T>, S> generator;

    final CheckedConsumer<? super S> stateCleanup;

    public FolyamGenerate(Callable<S> stateSupplier, CheckedBiFunction<S, Emitter<T>, S> generator, CheckedConsumer<? super S> stateCleanup) {
        this.stateSupplier = stateSupplier;
        this.generator = generator;
        this.stateCleanup = stateCleanup;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        S state;
        try {
            state = stateSupplier.call();
        } catch (Throwable ex) {
            EmptySubscription.error(s, ex);
            return;
        }
        s.onSubscribe(new GenerateSubscription<>(s, state, generator, stateCleanup));
    }

    static final class GenerateSubscription<T, S> extends AtomicLong implements FusedSubscription<T>, Emitter<T> {

        final FolyamSubscriber<? super T> actual;

        final CheckedBiFunction<S, Emitter<T>, S> generator;

        final CheckedConsumer<? super S> stateCleanup;

        S state;

        T value;
        boolean done;
        Throwable error;

        volatile boolean cancelled;

        GenerateSubscription(FolyamSubscriber<? super T> actual, S initialState, CheckedBiFunction<S, Emitter<T>, S> generator, CheckedConsumer<? super S> stateCleanup) {
            this.actual = actual;
            this.state = initialState;
            this.generator = generator;
            this.stateCleanup = stateCleanup;
        }

        @Override
        public void onNext(T item) {
            if (item == null) {
                onError(new NullPointerException("item == null"));
            } else
            if (!done) {
                T v = value;
                if (v == null) {
                    value = item;
                } else {
                    onError(new IllegalStateException("onNext already called in this generator round"));
                }
            }
        }

        @Override
        public void onError(Throwable ex) {
            if (ex == null) {
                ex = new NullPointerException("ex == null");
            }
            if (!done) {
                done = true;
                Throwable err = error;
                if (err == null) {
                    error = ex;
                    return;
                }
            }
            FolyamPlugins.onError(ex);
        }

        @Override
        public void onComplete() {
            done = true;
        }

        @Override
        public int requestFusion(int mode) {
            if ((mode & BOUNDARY) == 0) {
                return mode & SYNC;
            }
            return NONE;
        }

        @Override
        public T poll() throws Throwable {
            Throwable err = error;
            if (err != null) {
                error = null;
                clearState(state);
                throw err;
            }
            if (!done) {
                state = generator.apply(state, this);
                T v = value;
                if (v != null) {
                    value = null;
                    return v;
                }
                err = error;
                if (err != null) {
                    error = null;
                    clearState(state);
                    throw err;
                }
                clearState(state);
                if (!done) {
                    done = true;
                    throw new IllegalStateException("No onXXX method called in this generator round");
                }
            }
            return null;
        }

        @Override
        public boolean isEmpty() {
            return value == null && error == null && done;
        }

        @Override
        public void clear() {
            clearState(state);
            done = true;
        }

        void clearState(S st) {
            value = null;
            error = null;
            state = null;
            try {
                stateCleanup.accept(st);
            } catch (Throwable ex) {
                FolyamPlugins.onError(ex);
            }
        }

        @Override
        public void request(long n) {
            if (SubscriptionHelper.addRequested(this, n) == 0) {
                drain(n);
            }
        }

        @Override
        public void cancel() {
            cancelled = true;
            if (SubscriptionHelper.addRequested(this, 1) == 0) {
                clearState(state);
            }
        }

        void drain(long r) {
            FolyamSubscriber<? super T> a = actual;
            CheckedBiFunction<S, Emitter<T>, S> gen = generator;
            S state = this.state;
            long e = 0L;
            for (;;) {

                while (e != r) {
                    if (cancelled) {
                        clearState(state);
                        return;
                    }
                    try {
                        state = gen.apply(state, this);
                    } catch (Throwable ex) {
                        Throwable err = error;
                        if (err == null) {
                            a.onError(ex);
                        } else {
                            a.onError(new CompositeThrowable(err, ex));
                        }
                        clearState(state);
                        return;
                    }
                    T v = value;
                    if (v != null) {
                        value = null;

                        a.onNext(v);
                        e++;
                    }
                    if (done) {
                        Throwable err = error;
                        if (err != null) {
                            error = null;
                            a.onError(err);
                        } else {
                            a.onComplete();
                        }
                        clearState(state);
                        return;
                    }
                    if (v == null) {
                        a.onError(new IllegalStateException("No onXXX method called in this generator round"));
                        clearState(state);
                        return;
                    }

                }

                r = getAcquire();
                if (e == r) {
                    this.state = state;
                    r = addAndGet(-e);
                    if (r == 0L) {
                        break;
                    }
                    e = 0L;
                }
            }
        }
    }
}
