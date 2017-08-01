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

package hu.akarnokd.reactive4javaflow.hot;

import hu.akarnokd.reactive4javaflow.Folyam;
import hu.akarnokd.reactive4javaflow.FolyamSubscriber;
import hu.akarnokd.reactive4javaflow.functionals.AutoDisposable;
import hu.akarnokd.reactive4javaflow.impl.SerializedFolyamProcessor;

import java.util.concurrent.Flow;

public abstract class FolyamProcessor<T> extends Folyam<T> implements Flow.Processor<T, T>, FolyamSubscriber<T> {

    public abstract boolean hasThrowable();

    public abstract Throwable getThrowable();

    public abstract boolean hasComplete();

    public abstract boolean hasSubscribers();

    public final FolyamProcessor<T> toSerialized() {
        if (this instanceof SerializedFolyamProcessor) {
            return this;
        }
        return new SerializedFolyamProcessor<>(this);
    }

    public final FolyamProcessor<T> refCount() {
        // TODO implement
        throw new UnsupportedOperationException("Not yet implemented!");
    }
}
