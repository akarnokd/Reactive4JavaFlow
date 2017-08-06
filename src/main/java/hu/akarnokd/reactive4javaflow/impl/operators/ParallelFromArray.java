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

import java.util.concurrent.Flow;

/**
 * Wraps multiple Publishers into a ParallelFolyam which runs them
 * in parallel.
 *
 * @param <T> the value type
 */
public final class ParallelFromArray<T> extends ParallelFolyam<T> {
    final Flow.Publisher<? extends T>[] sources;

    public ParallelFromArray(Flow.Publisher<? extends T>[] sources) {
        this.sources = sources;
    }

    @Override
    public int parallelism() {
        return sources.length;
    }

    @Override
    public void subscribeActual(FolyamSubscriber<? super T>[] subscribers) {
        int n = subscribers.length;

        for (int i = 0; i < n; i++) {
            sources[i].subscribe(subscribers[i]);
        }
    }
}
