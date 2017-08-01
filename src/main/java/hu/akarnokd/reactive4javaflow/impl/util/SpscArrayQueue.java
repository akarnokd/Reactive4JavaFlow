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
import hu.akarnokd.reactive4javaflow.impl.QueueHelper;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Objects;

public final class SpscArrayQueue<T> implements PlainQueue<T> {

    final T[] array;
    static final VarHandle ARRAY;

    final int mask;

    long producerIndex;
    static final VarHandle PRODUCER_INDEX;

    long consumerIndex;
    static final VarHandle CONSUMER_INDEX;

    static {
        try {
            PRODUCER_INDEX = MethodHandles.lookup().findVarHandle(SpscArrayQueue.class, "producerIndex", Long.TYPE);
            CONSUMER_INDEX = MethodHandles.lookup().findVarHandle(SpscArrayQueue.class, "consumerIndex", Long.TYPE);
        } catch (Throwable ex) {
            throw new InternalError(ex);
        }
        ARRAY = MethodHandles.arrayElementVarHandle(Object[].class);
    }

    public SpscArrayQueue(int capacity) {
        int c = QueueHelper.pow2(capacity);
        this.array = (T[])new Object[c];
        this.mask = c - 1;
    }

    @Override
    public boolean offer(T item) {
        Objects.requireNonNull(item, "item == null");
        T[] a = array;
        int m = mask;
        long pi = producerIndex;
        int offset = (int)pi & m;

        if (ARRAY.getAcquire(a, offset) == null) {
            ARRAY.setRelease(a, offset, item);
            PRODUCER_INDEX.setRelease(this, pi + 1);
            return true;
        }

        return false;
    }

    @Override
    public T poll() {
        T[] a = array;
        int m = mask;
        long ci = consumerIndex;
        int offset = (int)ci & m;
        T v = (T)ARRAY.getAcquire(a, offset);
        if (v != null) {
            ARRAY.setRelease(a, offset, null);
            CONSUMER_INDEX.setRelease(this, ci + 1);
        }

        return v;
    }

    @Override
    public boolean isEmpty() {
        return (long)PRODUCER_INDEX.getAcquire(this) == (long)CONSUMER_INDEX.getAcquire(this);
    }

    @Override
    public void clear() {
        T[] a = array;
        int m = mask;
        long ci = consumerIndex;
        int offset = (int)ci & m;

        if (ARRAY.getAcquire(a, offset) != null) {
            for (;;) {
                ARRAY.setRelease(a, offset, null);
                offset = (offset + 1) & m;
                ci++;
                if (ARRAY.getAcquire(a, offset) == null) {
                    break;
                }
            }
            CONSUMER_INDEX.setRelease(this, ci);
        }

    }
}
