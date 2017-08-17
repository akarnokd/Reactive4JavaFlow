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
import hu.akarnokd.reactive4javaflow.functionals.CheckedConsumer;
import hu.akarnokd.reactive4javaflow.impl.*;

import java.lang.invoke.*;

public final class EsetlegCreate<T> extends Esetleg<T> {

    final CheckedConsumer<? super FolyamEmitter<T>> onSubscribe;

    public EsetlegCreate(CheckedConsumer<? super FolyamEmitter<T>> onSubscribe) {
        this.onSubscribe = onSubscribe;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        CreateEmitter<T> emitter = new CreateEmitter<>(s);
        s.onSubscribe(emitter);
        try {
            onSubscribe.accept(emitter);
        } catch (Throwable ex) {
            emitter.onError(ex);
        }
    }

    static final class CreateEmitter<T> extends DeferredScalarSubscription<T> implements FolyamEmitter<T> {

        boolean once;
        static final VarHandle ONCE = VH.find(MethodHandles.lookup(), CreateEmitter.class, "once", boolean.class);

        AutoCloseable resource;
        static final VarHandle RESOURCE = VH.find(MethodHandles.lookup(), CreateEmitter.class, "resource", AutoCloseable.class);

        static final AutoCloseable CLOSED = () -> { };

        CreateEmitter(FolyamSubscriber<? super T> actual) {
            super(actual);
        }

        @Override
        public void setResource(AutoCloseable resource) {
            for (;;) {
                AutoCloseable a = (AutoCloseable)RESOURCE.getAcquire(this);
                if (a == CLOSED) {
                    closeSilently(resource);
                    break;
                }
                if (RESOURCE.compareAndSet(this, a, resource)) {
                    closeSilently(a);
                    break;
                }
            }
        }

        @Override
        public long requested() {
            return getAcquire() == HAS_REQUEST_NO_VALUE ? 1L : 0L;
        }

        @Override
        public FolyamEmitter<T> serialized() {
            return this;
        }

        void closeSilently(AutoCloseable a) {
            if (a != null) {
                try {
                    a.close();
                } catch (Throwable ex) {
                    FolyamPlugins.onError(ex);
                }
            }
        }

        @Override
        public void onNext(T item) {
            if (item == null) {
                onError(new NullPointerException("item == null"));
                return;
            }
            if (ONCE.compareAndSet(this, false, true)) {
                AutoCloseable a = (AutoCloseable)RESOURCE.getAndSet(this, CLOSED);
                if (a != CLOSED) {
                    complete(item);
                    closeSilently(a);
                }
            }
        }

        @Override
        public boolean tryOnError(Throwable throwable) {
            if (throwable == null) {
                throwable = new NullPointerException("throwable == null");
            }
            if (ONCE.compareAndSet(this, false, true)) {
                AutoCloseable a = (AutoCloseable)RESOURCE.getAndSet(this, CLOSED);
                if (a != CLOSED) {
                    error(throwable);
                    closeSilently(a);
                    return true;
                }
            }
            return false;
        }

        @Override
        public void onError(Throwable throwable) {
            if (!tryOnError(throwable)) {
                FolyamPlugins.onError(throwable);
            }
        }

        @Override
        public void onComplete() {
            if (!(boolean)ONCE.getAcquire(this) && ONCE.compareAndSet(this, false, true)) {
                AutoCloseable a = (AutoCloseable)RESOURCE.getAndSet(this, CLOSED);
                if (a != CLOSED) {
                    complete();
                    closeSilently(a);
                }
            }
        }

        @Override
        public void cancel() {
            if (ONCE.compareAndSet(this, false, true)) {
                super.cancel();
                AutoCloseable a = (AutoCloseable) RESOURCE.getAcquire(this);
                if (a != CLOSED) {
                    a = (AutoCloseable) RESOURCE.getAndSet(this, CLOSED);
                    if (a != CLOSED) {
                        closeSilently(a);
                    }
                }
            }
        }

        @Override
        public boolean isCancelled() {
            return (boolean)ONCE.getAcquire(this);
        }
    }
}
