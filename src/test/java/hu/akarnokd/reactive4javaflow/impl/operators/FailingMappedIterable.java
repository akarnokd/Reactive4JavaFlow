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

import java.util.Iterator;
import java.util.function.Function;

public final class FailingMappedIterable<R> implements Iterable<R> {
    int iterator;
    final int hasNext;
    final int next;

    final Function<Integer, R> function;

    public FailingMappedIterable(int iterator, int hasNext, int next, Function<Integer, R> function) {
        this.iterator = iterator;
        this.hasNext = hasNext;
        this.next = next;
        this.function = function;
    }

    @Override
    public Iterator<R> iterator() {
        if (--iterator == 0) {
            throw new IllegalStateException("iterator");
        }
        return new FailingIterator<>(hasNext, next, function);
    }

    static final class FailingIterator<R> implements Iterator<R> {
        final Function<Integer, R> function;

        int hasNext;
        int next;

        int count;

        FailingIterator(int hasNext, int next, Function<Integer, R> function) {
            this.hasNext = hasNext;
            this.next = next;
            this.function = function;
        }

        @Override
        public boolean hasNext() {
            if (--hasNext == 0) {
                throw new IllegalStateException("hasNext");
            }
            return true;
        }

        @Override
        public R next() {
            if (--next == 0) {
                throw new IllegalStateException("next");
            }
            return function.apply(++count);
        }
    }
}
