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
import hu.akarnokd.reactive4javaflow.functionals.CheckedPredicate;
import hu.akarnokd.reactive4javaflow.fused.ConditionalSubscriber;

public final class EsetlegRetry<T> extends Esetleg<T> {

    final Esetleg<T> source;

    final long times;

    final CheckedPredicate<? super Throwable> condition;

    public EsetlegRetry(Esetleg<T> source, long times, CheckedPredicate<? super Throwable> condition) {
        this.source = source;
        this.times = times;
        this.condition = condition;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        FolyamRetry.AbstractRetrySubscriber parent;
        if (s instanceof ConditionalSubscriber) {
            parent = new FolyamRetry.RetryConditionalSubscriber<>((ConditionalSubscriber<? super T>)s, times, condition, source);
        } else {
            parent = new FolyamRetry.RetrySubscriber<>(s, times, condition, source);
        }

        s.onSubscribe(parent);
        parent.subscribeNext();
    }
}
