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
import hu.akarnokd.reactive4javaflow.fused.ConditionalSubscriber;
import hu.akarnokd.reactive4javaflow.impl.*;

import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.VarHandle;
import java.util.*;
import java.util.concurrent.Flow;

import static java.lang.invoke.MethodHandles.lookup;

public final class FolyamSwitchIfEmptyMany<T> extends Folyam<T> {

    final Folyam<T> source;

    final Iterable<? extends Flow.Publisher<? extends T>> others;

    public FolyamSwitchIfEmptyMany(Folyam<T> source, Iterable<? extends Flow.Publisher<? extends T>> others) {
        this.source = source;
        this.others = others;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        if (s instanceof ConditionalSubscriber) {
            SwitchIfEmptyManyConditionalSubscriber<T> parent = new SwitchIfEmptyManyConditionalSubscriber<>((ConditionalSubscriber<? super T>)s, others);
            s.onSubscribe(parent);
            parent.subscribeNext(source);
        } else {
            SwitchIfEmptyManySubscriber<T> parent = new SwitchIfEmptyManySubscriber<>(s, others);
            s.onSubscribe(parent);
            parent.subscribeNext(source);
        }
    }

    static final class SwitchIfEmptyManySubscriber<T> extends SubscriptionArbiter implements FolyamSubscriber<T> {

        final FolyamSubscriber<? super T> actual;

        final Iterable<? extends Flow.Publisher<? extends T>> others;

        Iterator<? extends Flow.Publisher<? extends T>> it;

        boolean hasValue;

        int wip;
        static final VarHandle WIP;

        static {
            Lookup lk = lookup();
            try {
                WIP = lk.findVarHandle(SwitchIfEmptyManySubscriber.class, "wip", int.class);
            } catch (Throwable ex) {
                throw new InternalError(ex);
            }
        }

        SwitchIfEmptyManySubscriber(FolyamSubscriber<? super T> actual, Iterable<? extends Flow.Publisher<? extends T>> others) {
            this.actual = actual;
            this.others = others;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            arbiterReplace(subscription);
        }

        @Override
        public void onNext(T item) {
            if (!hasValue) {
                hasValue = true;
            }
            actual.onNext(item);
        }

        @Override
        public void onError(Throwable throwable) {
            actual.onError(throwable);
        }

        @Override
        public void onComplete() {
            if (hasValue) {
                actual.onComplete();
            } else {
                subscribeNext(null);
            }
        }

        void subscribeNext(FolyamPublisher<T> source) {
            if ((int)WIP.getAndAdd(this, 1) == 0) {
                do {
                    if (source != null) {
                        source.subscribe(this);
                        source = null;
                    } else {
                        Iterator<? extends Flow.Publisher<? extends T>> it = this.it;
                        if (it == null) {
                            try {
                                it = Objects.requireNonNull(this.others.iterator(), "The iterable returned a null iterator");
                                this.it = it;
                            } catch (Throwable ex) {
                                actual.onError(ex);
                                return;
                            }
                        }

                        Flow.Publisher<? extends T> p;

                        try {
                            if (!it.hasNext()) {
                                p = null;
                            } else {
                                p = Objects.requireNonNull(it.next(), "The iterator returned a null Flow.Publisher");
                            }
                        } catch (Throwable ex) {
                            this.it = null;
                            actual.onError(ex);
                            return;
                        }

                        if (p == null) {
                            actual.onComplete();
                        } else {
                            p.subscribe(this);
                        }
                    }
                } while ((int)WIP.getAndAdd(this, -1) - 1 != 0);
            }
        }
    }

    static final class SwitchIfEmptyManyConditionalSubscriber<T> extends SubscriptionArbiter implements ConditionalSubscriber<T> {

        final ConditionalSubscriber<? super T> actual;

        final Iterable<? extends Flow.Publisher<? extends T>> others;

        Iterator<? extends Flow.Publisher<? extends T>> it;

        boolean hasValue;

        int wip;
        static final VarHandle WIP;

        static {
            Lookup lk = lookup();
            try {
                WIP = lk.findVarHandle(SwitchIfEmptyManyConditionalSubscriber.class, "wip", int.class);
            } catch (Throwable ex) {
                throw new InternalError(ex);
            }
        }

        SwitchIfEmptyManyConditionalSubscriber(ConditionalSubscriber<? super T> actual, Iterable<? extends Flow.Publisher<? extends T>> others) {
            this.actual = actual;
            this.others = others;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            arbiterReplace(subscription);
        }

        @Override
        public void onNext(T item) {
            if (!hasValue) {
                hasValue = true;
            }
            actual.onNext(item);
        }

        @Override
        public boolean tryOnNext(T item) {
            if (!hasValue) {
                hasValue = true;
            }
            return actual.tryOnNext(item);
        }

        @Override
        public void onError(Throwable throwable) {
            actual.onError(throwable);
        }

        @Override
        public void onComplete() {
            if (hasValue) {
                actual.onComplete();
            } else {
                subscribeNext(null);
            }
        }

        void subscribeNext(FolyamPublisher<T> source) {
            if ((int)WIP.getAndAdd(this, 1) == 0) {
                do {
                    if (source != null) {
                        source.subscribe(this);
                        source = null;
                    } else {
                        Iterator<? extends Flow.Publisher<? extends T>> it = this.it;
                        if (it == null) {
                            try {
                                it = Objects.requireNonNull(this.others.iterator(), "The iterable returned a null iterator");
                                this.it = it;
                            } catch (Throwable ex) {
                                actual.onError(ex);
                                return;
                            }
                        }

                        Flow.Publisher<? extends T> p;

                        try {
                            if (!it.hasNext()) {
                                p = null;
                            } else {
                                p = Objects.requireNonNull(it.next(), "The iterator returned a null Flow.Publisher");
                            }
                        } catch (Throwable ex) {
                            this.it = null;
                            actual.onError(ex);
                            return;
                        }

                        if (p == null) {
                            actual.onComplete();
                        } else {
                            p.subscribe(this);
                        }
                    }
                } while ((int)WIP.getAndAdd(this, -1) - 1 != 0);
            }
        }
    }
}
