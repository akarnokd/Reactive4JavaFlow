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

public final class MpscLinkedArrayQueue<T> implements PlainQueue<T> {

    final int capacity;

    static final VarHandle ARRAY;

    Island producerArray;
    static final VarHandle PRODUCER_ARRAY;
    static final VarHandle PRODUCER_INDEX;

    Island consumerArray;
    int consumerIndex;
    static final VarHandle CONSUMER_INDEX;

    static final VarHandle NEXT_ISLAND;

    static final Island EMPTY = new Island(0);

    static {
        try {
            ARRAY = MethodHandles.arrayElementVarHandle(Object[].class);
            PRODUCER_ARRAY = MethodHandles.lookup().findVarHandle(MpscLinkedArrayQueue.class, "producerArray", Island.class);
            PRODUCER_INDEX = MethodHandles.lookup().findVarHandle(Island.class, "producerIndex", Integer.TYPE);
            CONSUMER_INDEX = MethodHandles.lookup().findVarHandle(MpscLinkedArrayQueue.class, "consumerIndex", Integer.TYPE);
            NEXT_ISLAND = MethodHandles.lookup().findVarHandle(Island.class, "next", Island.class);
        } catch (Throwable ex) {
            throw new InternalError(ex);
        }
    }

    public MpscLinkedArrayQueue(int capacity) {
        int c = QueueHelper.pow2(Math.max(4, capacity));
        producerArray = consumerArray = new Island(c);
        this.capacity = c;
    }

    @Override
    public boolean offer(T item) {
        Objects.requireNonNull(item, "item == null");
        int c = capacity;
        for (;;) {
            Island a = (Island)PRODUCER_ARRAY.getAcquire(this);
            int idx = (int)PRODUCER_INDEX.getAndAdd(a, 1);
            if (idx >= c) {
                Island b = (Island)NEXT_ISLAND.getAcquire(a);
                if (b != null) {
                    if (b != EMPTY) {
                        PRODUCER_ARRAY.compareAndSet(this, a, b);
                    }
                    continue;
                }
                b = new Island(c);
                b.array[0] = item;
                b.producerIndex = 1;
                if (NEXT_ISLAND.compareAndSet(a, null, b)) {
                    PRODUCER_ARRAY.compareAndSet(this, a, b);
                    return true;
                }
                continue;
            }
            ARRAY.setRelease(a.array, idx, item);
            return true;
        }
    }

    @Override
    public T poll() {
        Island a = consumerArray;
        int ci = consumerIndex;
        int c = capacity;

        if (ci < c) {
            @SuppressWarnings("unchecked")
            T v = (T)ARRAY.getAcquire(a.array, ci);
            if (v != null) {
                ARRAY.set(a.array, ci, null);
                consumerIndex = ci + 1;
            }
            return v;
        }

        Island b = (Island)NEXT_ISLAND.getAcquire(a);
        if (b == null) {
            return null;
        }
        @SuppressWarnings("unchecked")
        T v = (T)ARRAY.getAcquire(b.array, 0);
        ARRAY.set(b.array, 0, null);
        consumerIndex = 1;
        consumerArray = b;
        NEXT_ISLAND.setRelease(a, EMPTY);
        return v;

    }

    @Override
    public boolean isEmpty() {
        Island a = consumerArray;
        int ci = consumerIndex;

        if (ci >= capacity) {
            return NEXT_ISLAND.getAcquire(a) == null;
        }

        return ARRAY.getAcquire(a, ci) == null;
    }

    @Override
    public void clear() {
        QueueHelper.clear(this);
    }

    static final class Island {
        final Object[] array;

        Island next;

        int producerIndex;

        Island(int capacity) {
            array = new Object[capacity];
        }
    }

}
