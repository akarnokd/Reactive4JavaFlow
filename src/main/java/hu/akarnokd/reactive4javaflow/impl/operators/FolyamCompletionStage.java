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
import hu.akarnokd.reactive4javaflow.impl.DeferredScalarSubscription;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

public final class FolyamCompletionStage<T> extends Folyam<T> {

    final CompletionStage<? extends T> stage;

    public FolyamCompletionStage(CompletionStage<? extends T> stage) {
        this.stage = stage;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        CompletionStageSubscription<T> parent = new CompletionStageSubscription<>(s);
        s.onSubscribe(parent);
        stage.whenComplete(parent.consumer);
    }

    static final class CompletionStageSubscription<T> extends DeferredScalarSubscription<T> implements BiConsumer<T, Throwable> {

        final IndirectBiConsumer<T> consumer;

        public CompletionStageSubscription(FolyamSubscriber<? super T> actual) {
            super(actual);
            this.consumer = new IndirectBiConsumer<>(this);
        }

        @Override
        public void accept(T t, Throwable throwable) {
            consumer.set(null);
            if (throwable != null) {
                error(throwable);
            } else {
                if (t != null) {
                    complete(t);
                } else {
                    complete();
                }
            }
        }

        @Override
        public void cancel() {
            super.cancel();
            consumer.setRelease(null);
        }

        static final class IndirectBiConsumer<T> extends AtomicReference<BiConsumer<T, Throwable>> implements BiConsumer<T, Throwable> {

            IndirectBiConsumer(BiConsumer<T, Throwable> c) {
                setRelease(c);
            }

            @Override
            public void accept(T t, Throwable throwable) {
                BiConsumer<T, Throwable> c = getAcquire();
                if (c != null) {
                    c.accept(t, throwable);
                }
            }
        }
    }
}
