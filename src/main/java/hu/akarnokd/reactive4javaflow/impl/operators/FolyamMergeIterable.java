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

import java.util.Iterator;
import java.util.concurrent.Flow;

public final class FolyamMergeIterable<T> extends Folyam<T> {

    final Iterable<? extends Flow.Publisher<? extends T>> sources;

    final int prefetch;

    final boolean delayError;

    public FolyamMergeIterable(Iterable<? extends Flow.Publisher<? extends T>> sources, int prefetch, boolean delayError) {
        this.sources = sources;
        this.delayError = delayError;
        this.prefetch = prefetch;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        Iterable<? extends Flow.Publisher<? extends T>> srcs = this.sources;

        Iterator<? extends Flow.Publisher<? extends T>> it;

        try {
            it = srcs.iterator();
            if (!it.hasNext()) {
                EmptySubscription.complete(s);
                return;
            }
        } catch (Throwable ex) {
            FolyamPlugins.handleFatal(ex);
            EmptySubscription.error(s, ex);
            return;
        }

        FolyamSubscriber<? super Flow.Publisher<? extends T>> subscriber =
                FolyamFlatMap.createSubscriber(s, v -> v, Integer.MAX_VALUE, prefetch, delayError);
        subscriber.onSubscribe(new FolyamIterable.IteratorSubscription<>(subscriber, it));
    }
}
