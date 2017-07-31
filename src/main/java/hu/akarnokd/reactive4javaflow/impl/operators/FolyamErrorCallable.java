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
import hu.akarnokd.reactive4javaflow.fused.FusedDynamicSource;
import hu.akarnokd.reactive4javaflow.impl.EmptySubscription;

import java.util.Objects;
import java.util.concurrent.Callable;

public final class FolyamErrorCallable<T> extends Folyam<T> implements FusedDynamicSource<T> {

    final Callable<? extends Throwable> error;

    public FolyamErrorCallable(Callable<? extends Throwable> error) {
        this.error = error;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        Throwable e;

        try {
            e = Objects.requireNonNull(error.call());
        } catch (Throwable ex) {
            e = ex;
        }

        EmptySubscription.error(s, e);
    }

    @Override
    public T value() throws Throwable {
        throw error.call();
    }
}
