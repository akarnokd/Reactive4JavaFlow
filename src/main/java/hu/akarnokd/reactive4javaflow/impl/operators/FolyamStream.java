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
import hu.akarnokd.reactive4javaflow.fused.*;
import hu.akarnokd.reactive4javaflow.impl.*;

import java.lang.invoke.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

public final class FolyamStream<T> extends Folyam<T> {

    final Stream<? extends T> source;

    public FolyamStream(Stream<? extends T> source) {
        this.source = source;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        Iterator<? extends T> it;
        boolean has;

        try {
            it = source.iterator();
            has = it.hasNext();
        } catch (Throwable ex) {
            EmptySubscription.error(s, ex);
            source.close();
            return;
        }

        if (!has) {
            EmptySubscription.complete(s);
            source.close();
            return;
        }

        if (s instanceof ConditionalSubscriber) {
            s.onSubscribe(new StreamConditionalSubscription<>((ConditionalSubscriber<? super T>)s, it, source));
        } else {
            s.onSubscribe(new StreamSubscription<>(s, it, source));
        }
    }

    static abstract class AbstractStreamSubscription<T> extends AtomicLong implements FusedSubscription<T> {

        Iterator<? extends T> iterator;

        Stream<? extends T> resource;
        static final VarHandle RESOURCE;

        boolean checkNext;

        volatile boolean cancelled;

        static {
            try {
                RESOURCE = MethodHandles.lookup().findVarHandle(AbstractStreamSubscription.class, "resource", Stream.class);
            } catch (Throwable ex) {
                throw new InternalError(ex);
            }
        }

        protected AbstractStreamSubscription(Iterator<? extends T> iterator, Stream<? extends T> resource) {
            this.iterator = iterator;
            this.resource = resource;
        }

        void closeResource() {
            Stream<?> a = (Stream<?>)RESOURCE.getAcquire(this);
            if (a != null) {
                a = (Stream<?>)RESOURCE.getAndSetAcquire(this, null);
                if (a != null) {
                    a.close();
                }
            }
        }

        @Override
        public final void cancel() {
            cancelled = true;
            closeResource();
        }

        @Override
        public final void request(long n) {
            if (SubscriptionHelper.addRequested(this, n) == 0L) {
                if (n == Long.MAX_VALUE) {
                    fastPath();
                } else {
                    slowPath(n);
                }
            }
        }

        @Override
        public final int requestFusion(int mode) {
            return mode & SYNC;
        }

        @Override
        public final T poll() throws Throwable {
            Iterator<? extends T> it = iterator;
            if (it != null) {
                if (checkNext) {
                    if (it.hasNext()) {
                        return Objects.requireNonNull(it.next(), "The iterator returned a null value");
                    }
                    iterator = null;
                    closeResource();
                } else {
                    checkNext = true;
                    return Objects.requireNonNull(it.next(), "The iterator returned a null value");
                }
            }
            return null;
        }

        @Override
        public final boolean isEmpty() {
            return iterator == null;
        }

        @Override
        public final void clear() {
            iterator = null;
        }

        abstract void fastPath();

        abstract void slowPath(long n);
    }

    static final class StreamSubscription<T> extends AbstractStreamSubscription<T> {

        final FolyamSubscriber<? super T> actual;

        StreamSubscription(FolyamSubscriber<? super T> actual, Iterator<? extends T> iterator, Stream<? extends T> resource) {
            super(iterator, resource);
            this.actual = actual;
        }

        @Override
        void fastPath() {
            FolyamSubscriber<? super T> a = actual;
            Iterator<? extends T> it = iterator;
            for (;;) {
                if (cancelled) {
                    return;
                }

                T v;

                try {
                    v = Objects.requireNonNull(it.next(), "The iterator returned a null value");
                } catch (Throwable ex) {
                    FolyamPlugins.handleFatal(ex);
                    a.onError(ex);
                    closeResource();
                    return;
                }

                if (cancelled) {
                    return;
                }

                a.onNext(v);

                if (cancelled) {
                    return;
                }

                boolean b;

                try {
                    b = it.hasNext();
                } catch (Throwable ex) {
                    FolyamPlugins.handleFatal(ex);
                    a.onError(ex);
                    closeResource();
                    return;
                }

                if (!b) {
                    if (!cancelled) {
                        a.onComplete();
                        closeResource();
                    }
                    return;
                }
            }
        }

        @Override
        void slowPath(long n) {
            FolyamSubscriber<? super T> a = actual;
            Iterator<? extends T> it = iterator;

            long e = 0L;

            for (;;) {

                while (e != n) {
                    if (cancelled) {
                        return;
                    }

                    T v;

                    try {
                        v = Objects.requireNonNull(it.next(), "The iterator returned a null value");
                    } catch (Throwable ex) {
                        FolyamPlugins.handleFatal(ex);
                        a.onError(ex);
                        closeResource();
                        return;
                    }

                    if (cancelled) {
                        return;
                    }

                    a.onNext(v);

                    if (cancelled) {
                        return;
                    }

                    boolean b;

                    try {
                        b = it.hasNext();
                    } catch (Throwable ex) {
                        FolyamPlugins.handleFatal(ex);
                        a.onError(ex);
                        closeResource();
                        return;
                    }

                    if (!b) {
                        if (!cancelled) {
                            a.onComplete();
                            closeResource();
                        }
                        return;
                    }

                    e++;
                }

                n = getAcquire();
                if (n == e) {
                    n = addAndGet(-e);
                    if (n == 0L) {
                        break;
                    }
                    e = 0L;
                }
            }
        }
    }

    static final class StreamConditionalSubscription<T> extends AbstractStreamSubscription<T> {

        final ConditionalSubscriber<? super T> actual;

        StreamConditionalSubscription(ConditionalSubscriber<? super T> actual, Iterator<? extends T> iterator, Stream<? extends T> resource) {
            super(iterator, resource);
            this.actual = actual;
        }

        @Override
        void fastPath() {
            ConditionalSubscriber<? super T> a = actual;
            Iterator<? extends T> it = iterator;
            for (;;) {
                if (cancelled) {
                    return;
                }

                T v;

                try {
                    v = Objects.requireNonNull(it.next(), "The iterator returned a null value");
                } catch (Throwable ex) {
                    FolyamPlugins.handleFatal(ex);
                    a.onError(ex);
                    closeResource();
                    return;
                }

                if (cancelled) {
                    return;
                }

                a.tryOnNext(v);

                if (cancelled) {
                    return;
                }

                boolean b;

                try {
                    b = it.hasNext();
                } catch (Throwable ex) {
                    FolyamPlugins.handleFatal(ex);
                    a.onError(ex);
                    closeResource();
                    return;
                }

                if (!b) {
                    if (!cancelled) {
                        a.onComplete();
                        closeResource();
                    }
                    return;
                }
            }
        }

        @Override
        void slowPath(long n) {
            ConditionalSubscriber<? super T> a = actual;
            Iterator<? extends T> it = iterator;

            long e = 0L;

            for (;;) {

                while (e != n) {
                    if (cancelled) {
                        return;
                    }

                    T v;

                    try {
                        v = Objects.requireNonNull(it.next(), "The iterator returned a null value");
                    } catch (Throwable ex) {
                        FolyamPlugins.handleFatal(ex);
                        a.onError(ex);
                        closeResource();
                        return;
                    }

                    if (cancelled) {
                        return;
                    }

                    if (a.tryOnNext(v)) {
                        e++;
                    }

                    if (cancelled) {
                        return;
                    }

                    boolean b;

                    try {
                        b = it.hasNext();
                    } catch (Throwable ex) {
                        FolyamPlugins.handleFatal(ex);
                        a.onError(ex);
                        closeResource();
                        return;
                    }

                    if (!b) {
                        if (!cancelled) {
                            a.onComplete();
                            closeResource();
                        }
                        return;
                    }
                }

                n = getAcquire();
                if (n == e) {
                    n = addAndGet(-e);
                    if (n == 0L) {
                        break;
                    }
                    e = 0L;
                }
            }
        }
    }
}
