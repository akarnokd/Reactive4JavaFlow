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

package hu.akarnokd.reactive4javaflow.impl.util;

import hu.akarnokd.reactive4javaflow.impl.PlainQueue;

import java.util.concurrent.atomic.AtomicReference;

public final class SpscOneQueue<T> extends AtomicReference<T> implements PlainQueue<T> {
    @Override
    public boolean offer(T item) {
        if (getAcquire() == null) {
            setRelease(item);
            return true;
        }
        return false;
    }

    @Override
    public T poll() {
        T v = getAcquire();
        if (v != null) {
            setRelease(null);
        }
        return v;
    }

    @Override
    public boolean isEmpty() {
        return getAcquire() == null;
    }

    @Override
    public void clear() {
        setRelease(null);
    }
}
