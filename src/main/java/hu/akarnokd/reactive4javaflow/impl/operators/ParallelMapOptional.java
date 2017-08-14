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
import hu.akarnokd.reactive4javaflow.functionals.CheckedFunction;
import hu.akarnokd.reactive4javaflow.fused.ConditionalSubscriber;

import java.util.Optional;

public final class ParallelMapOptional<T, R> extends ParallelFolyam<R> {

    final ParallelFolyam<T> source;

    final CheckedFunction<? super T, ? extends Optional<? extends R>> mapper;

    public ParallelMapOptional(ParallelFolyam<T> source, CheckedFunction<? super T, ? extends Optional<? extends R>> mapper) {
        this.source = source;
        this.mapper = mapper;
    }

    @Override
    public int parallelism() {
        return source.parallelism();
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super R>[] subscribers) {
        int n = subscribers.length;

        FolyamSubscriber<T>[] parent = new FolyamSubscriber[n];

        for (int i = 0; i < n; i++) {
            FolyamSubscriber<? super R> s = subscribers[i];
            if (s instanceof ConditionalSubscriber) {
                parent[i] = new FolyamMapOptional.MapConditionalSubscriber<>((ConditionalSubscriber<? super R>) s, mapper);
            } else {
                parent[i] = new FolyamMapOptional.MapSubscriber<>(s, mapper);
            }
        }

        source.subscribe(parent);
    }
}
