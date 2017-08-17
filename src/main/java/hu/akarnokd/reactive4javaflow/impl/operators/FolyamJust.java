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

import java.util.concurrent.atomic.AtomicInteger;

public final class FolyamJust<T> extends Folyam<T> implements FusedStaticSource<T> {

    final T value;

    public FolyamJust(T value) {
        this.value = value;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        s.onSubscribe(new JustSubscription<>(s, value));
    }

    @Override
    public T value() {
        return value;
    }

    static final class JustSubscription<T> extends AtomicInteger implements FusedSubscription<T> {

        final FolyamSubscriber<? super T> actual;

        final T value;

        JustSubscription(FolyamSubscriber<? super T> actual, T value) {
            this.actual = actual;
            this.value = value;
        }

        @Override
        public int requestFusion(int mode) {
            return mode & SYNC;
        }

        @Override
        public T poll() throws Throwable {
            if (getAcquire() == 0) {
                setRelease(2);
                return value;
            }
            return null;
        }

        @Override
        public boolean isEmpty() {
            return getAcquire() != 0;
        }

        @Override
        public void clear() {
            setRelease(2);
        }

        @Override
        public void request(long n) {
            if (compareAndSet(0, 1)) {
                actual.onNext(value);
                if (get() != 2) {
                    setRelease(2);
                    actual.onComplete();
                }
            }
        }

        @Override
        public void cancel() {
            setRelease(2);
        }
    }
}
