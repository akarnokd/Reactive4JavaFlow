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
import hu.akarnokd.reactive4javaflow.impl.EmptySubscription;

import java.util.*;
import java.util.concurrent.Flow;

public final class FolyamOrderedMergeIterable<T> extends Folyam<T> {
    final Iterable<? extends Flow.Publisher<? extends T>> sourcesIterable;

    final Comparator<? super T> comparator;

    final boolean delayErrors;

    final int prefetch;

    public FolyamOrderedMergeIterable(Iterable<? extends Flow.Publisher<? extends T>> sourcesIterable,
                         Comparator<? super T> comparator,
                         int prefetch, boolean delayErrors) {
        this.sourcesIterable = sourcesIterable;
        this.comparator = comparator;
        this.prefetch = prefetch;
        this.delayErrors = delayErrors;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        Flow.Publisher<? extends T>[] array = new Flow.Publisher[8];
        int n = 0;
        try {
            for (Flow.Publisher<? extends T> p : sourcesIterable) {
                if (n == array.length) {
                    array = Arrays.copyOf(array, n << 1);
                }
                array[n++] = Objects.requireNonNull(p, "a source is null");
            }
        } catch (Throwable ex) {
            FolyamPlugins.handleFatal(ex);
            EmptySubscription.error(s, ex);
            return;
        }

        FolyamOrderedMergeArray.subscribe(s, array, n, comparator, prefetch, delayErrors);
    }
}
