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
import java.util.function.BiConsumer;

public final class SpscLinkedArrayQueue<T> implements PlainQueue<T> {

    final int mask;

    static final VarHandle ARRAY;

    Object[] producerArray;
    long producerIndex;
    static final VarHandle PRODUCER_INDEX;

    Object[] consumerArray;
    long consumerIndex;
    static final VarHandle CONSUMER_INDEX;

    static final Object NEXT = new Object();

    static {
        try {
            ARRAY = MethodHandles.arrayElementVarHandle(Object[].class);
            PRODUCER_INDEX = MethodHandles.lookup().findVarHandle(SpscLinkedArrayQueue.class, "producerIndex", Long.TYPE);
            CONSUMER_INDEX = MethodHandles.lookup().findVarHandle(SpscLinkedArrayQueue.class, "consumerIndex", Long.TYPE);
        } catch (Throwable ex) {
            throw new InternalError(ex);
        }
    }

    public SpscLinkedArrayQueue(int capacity) {
        int c = QueueHelper.pow2(Math.max(4, capacity));
        producerArray = consumerArray = new Object[c + 1];
        this.mask = c - 1;
    }

    @Override
    public boolean offer(T item) {
        Objects.requireNonNull(item, "item == null");
        Object[] a = producerArray;
        int m = mask;
        long pi = producerIndex;

        int offset = (int)pi & m;
        int offset1 = (int)(pi + 1) & m;

        if (ARRAY.getAcquire(a, offset1) != null) {
            Object[] b = new Object[m + 2];
            b[offset] = item;
            a[m + 1] = b;
            producerArray = b;
            ARRAY.setRelease(a, offset, NEXT);
        } else {
            ARRAY.setRelease(a, offset, item);
        }
        PRODUCER_INDEX.setRelease(this, pi + 1);

        return true;
    }

    @Override
    public T poll() {
        Object[] a = consumerArray;
        int m = mask;
        long ci = consumerIndex;
        int offset = (int)ci & m;

        Object v = ARRAY.getAcquire(a, offset);
        if (v == null) {
            return null;
        }

        if (v == NEXT) {
            Object[] b = (Object[])a[m + 2];
            v = b[offset];
            a[m + 1] = null;
            consumerArray = b;
            ARRAY.setRelease(b, offset, null);
        }
        CONSUMER_INDEX.setRelease(this, ci + 1);

        return (T)v;
    }

    @Override
    public boolean isEmpty() {
        return (long)PRODUCER_INDEX.getAcquire(this) == (long)CONSUMER_INDEX.getAcquire(this);
    }

    @Override
    public void clear() {
        QueueHelper.clear(this);
    }

    public void offer(T item1, T item2) {
        Objects.requireNonNull(item1, "item1 == null");
        Objects.requireNonNull(item2, "item2 == null");

        Object[] a = producerArray;
        int m = mask;
        long pi = producerIndex;

        int offset = (int)pi & m;
        int offset2 = (int)(pi + 2) & m;

        if (ARRAY.getAcquire(a, offset2) != null) {
            Object[] b = new Object[m + 2];
            b[offset] = item1;
            b[offset + 1] = item2;
            a[m + 1] = b;
            ARRAY.setRelease(a, offset, NEXT);
        } else {
            ARRAY.set(a, offset + 1, item2);
            ARRAY.setRelease(a, offset, item1);
        }
        PRODUCER_INDEX.setRelease(this, pi + 2);
    }

    public boolean poll(BiConsumer<T, T> consumer) {
        Object[] a = consumerArray;
        int m = mask;
        long ci = consumerIndex;
        int offset = (int)ci & m;

        Object v = ARRAY.getAcquire(a, offset);
        if (v == null) {
            return false;
        }
        Object w;
        if (v == NEXT) {
            Object[] b = (Object[])a[m + 2];
            a[m + 1] = null;
            consumerArray = b;
            v = b[offset];
            w = b[offset + 1];
            b[offset + 1] = null;
            ARRAY.setRelease(b, offset, null);
        } else {
            w = a[offset + 1];
            a[offset + 1] = null;
            ARRAY.setRelease(a, offset, null);
        }
        CONSUMER_INDEX.setRelease(this, ci + 2);
        consumer.accept((T)v, (T)w);
        return true;
    }
}
