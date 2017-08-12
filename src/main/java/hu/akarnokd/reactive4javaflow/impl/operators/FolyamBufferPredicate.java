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
import hu.akarnokd.reactive4javaflow.functionals.CheckedPredicate;
import hu.akarnokd.reactive4javaflow.fused.ConditionalSubscriber;
import hu.akarnokd.reactive4javaflow.impl.EmptySubscription;

import java.util.*;
import java.util.concurrent.*;

public final class FolyamBufferPredicate<T, C extends Collection<? super T>> extends Folyam<C> {

    public enum BufferPredicateMode {
        /** The item triggering the new buffer will be part of the new buffer. */
        BEFORE,
        /** The item triggering the new buffer will be part of the old buffer. */
        AFTER,
        /** The item won't be part of any buffers. */
        SPLIT
    }

    final Folyam<T> source;

    final CheckedPredicate<? super T> predicate;

    final BufferPredicateMode mode;

    final Callable<C> bufferSupplier;

    public FolyamBufferPredicate(Folyam<T> source, CheckedPredicate<? super T> predicate, BufferPredicateMode mode,
                            Callable<C> bufferSupplier) {
        this.source = source;
        this.predicate = predicate;
        this.mode = mode;
        this.bufferSupplier = bufferSupplier;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super C> s) {
        C buffer;

        try {
            buffer = Objects.requireNonNull(bufferSupplier.call(), "The bufferSupplier returned a null buffer");
        } catch (Throwable ex) {
            FolyamPlugins.handleFatal(ex);
            EmptySubscription.error(s, ex);
            return;
        }

        source.subscribe(new BufferPredicateSubscriber<T, C>(s, buffer, predicate, mode, bufferSupplier));
    }

    static final class BufferPredicateSubscriber<T, C extends Collection<? super T>>
            implements ConditionalSubscriber<T>, Flow.Subscription {

        final FolyamSubscriber<? super C> actual;

        final CheckedPredicate<? super T> predicate;

        final BufferPredicateMode mode;

        final Callable<C> bufferSupplier;

        C buffer;

        Flow.Subscription upstream;

        int count;

        BufferPredicateSubscriber(FolyamSubscriber<? super C> actual,
                                  C buffer,
                                  CheckedPredicate<? super T> predicate, BufferPredicateMode mode,
                                  Callable<C> bufferSupplier) {
            this.actual = actual;
            this.predicate = predicate;
            this.mode = mode;
            this.buffer = buffer;
            this.bufferSupplier = bufferSupplier;
        }

        @Override
        public void onSubscribe(Flow.Subscription s) {
            this.upstream = s;

            actual.onSubscribe(this);
        }

        @Override
        public void onNext(T t) {
            if (!tryOnNext(t) && buffer != null) {
                upstream.request(1);
            }
        }

        @Override
        public boolean tryOnNext(T t) {
            C buf = buffer;
            if (buf != null) {
                boolean b;

                try {
                    b = predicate.test(t);
                } catch (Throwable ex) {
                    FolyamPlugins.handleFatal(ex);
                    upstream.cancel();
                    buffer = null;
                    actual.onError(ex);
                    return true;
                }

                switch (mode) {
                    case AFTER: {
                        buf.add(t);
                        if (b) {
                            actual.onNext(buf);

                            try {
                                buffer = Objects.requireNonNull(bufferSupplier.call(), "The bufferSupplier returned a null collection");
                            } catch (Throwable ex) {
                                FolyamPlugins.handleFatal(ex);
                                upstream.cancel();
                                onError(ex);
                                return true;
                            }

                            count = 0;
                        } else {
                            count++;
                            return false;
                        }
                        break;
                    }
                    case BEFORE: {
                        if (b) {
                            buf.add(t);
                            count++;
                            return false;
                        } else {
                            actual.onNext(buf);
                            try {
                                buf = Objects.requireNonNull(bufferSupplier.call(), "The bufferSupplier returned a null collection");
                            } catch (Throwable ex) {
                                FolyamPlugins.handleFatal(ex);
                                upstream.cancel();
                                onError(ex);
                                return true;
                            }

                            buf.add(t);
                            buffer = buf;
                            count = 1;
                        }
                        break;
                    }
                    default:
                        if (b) {
                            actual.onNext(buf);

                            try {
                                buffer = Objects.requireNonNull(bufferSupplier.call(), "The bufferSupplier returned a null collection");
                            } catch (Throwable ex) {
                                FolyamPlugins.handleFatal(ex);
                                upstream.cancel();
                                onError(ex);
                                return true;
                            }

                            count = 0;
                        } else {
                            buf.add(t);
                            count++;
                            return false;
                        }
                }
            }
            return true;
        }

        @Override
        public void onError(Throwable t) {
            if (buffer != null) {
                buffer = null;
                actual.onError(t);
            } else {
                FolyamPlugins.onError(t);
            }
        }

        @Override
        public void onComplete() {
            C b = buffer;
            if (b != null) {
                buffer = null;
                if (count != 0) {
                    actual.onNext(b);
                }
                actual.onComplete();
            }
        }

        @Override
        public void request(long n) {
            upstream.request(n);
        }

        @Override
        public void cancel() {
            upstream.cancel();
        }
    }
}
