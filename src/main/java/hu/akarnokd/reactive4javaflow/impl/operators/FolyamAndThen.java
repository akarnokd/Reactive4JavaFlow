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
 * Consume the main source and ignore its values, then switch to a next
 * publisher and relay its values.
 * @param <T> the main source's value type
 * @param <U> the next source's value type
 * @since 0.1.2
 */
public final class FolyamAndThen<T, U> extends Folyam<U> {

    final FolyamPublisher<T> source;

    final Flow.Publisher<U> next;

    public FolyamAndThen(FolyamPublisher<T> source, Flow.Publisher<U> next) {
        this.source = source;
        this.next = next;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super U> s) {
        EsetlegAndThen.AndThenMainSubscriber<U> parent = new EsetlegAndThen.AndThenMainSubscriber<>(s, next);
        s.onSubscribe(parent);
        parent.drain(source);
    }
}
