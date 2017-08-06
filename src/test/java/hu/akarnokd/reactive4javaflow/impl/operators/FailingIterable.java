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

public final class FailingIterable implements Iterable<Integer> {
    int iterator;
    final int hasNext;
    final int next;

    public FailingIterable(int iterator, int hasNext, int next) {
        this.iterator = iterator;
        this.hasNext = hasNext;
        this.next = next;
    }

    @Override
    public Iterator<Integer> iterator() {
        if (--iterator == 0) {
            throw new IllegalStateException("iterator");
        }
        return new FailingIterator(hasNext, next);
    }

    static final class FailingIterator implements Iterator<Integer> {
        int hasNext;
        int next;

        int count;

        FailingIterator(int hasNext, int next) {
            this.hasNext = hasNext;
            this.next = next;
        }

        @Override
        public boolean hasNext() {
            if (--hasNext == 0) {
                throw new IllegalStateException("hasNext");
            }
            return true;
        }

        @Override
        public Integer next() {
            if (--next == 0) {
                throw new IllegalStateException("next");
            }
            return ++count;
        }
    }
}
