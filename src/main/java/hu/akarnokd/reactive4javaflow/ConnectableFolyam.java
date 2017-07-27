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
package hu.akarnokd.reactive4javaflow;

import hu.akarnokd.reactive4javaflow.functionals.AutoDisposable;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public abstract class ConnectableFolyam<T> extends Folyam<T> {

    public abstract AutoDisposable connect(Consumer<? super AutoDisposable> connectionHandler);

    public final AutoDisposable connect() {
        return connect(v -> { });
    }

    public final Folyam<T> autoConnect() {
        return autoConnect(1, v -> { });
    }

    public final Folyam<T> autoConnect(int minSubscribers) {
        return autoConnect(minSubscribers, v -> { });
    }

    public final Folyam<T> autoConnect(int minSubscribers, Consumer<? super AutoDisposable> connectionHandler) {
        if (minSubscribers <= 0) {
            connect(connectionHandler);
            return this;
        }
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<T> refCount() {
        return refCount(1);
    }

    public final Folyam<T> refCount(int minSubscribers) {
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public final Folyam<T> refCount(int minSubscribers, long timeout, TimeUnit unit, SchedulerService executor) {
        Objects.requireNonNull(unit, "unit == null");
        Objects.requireNonNull(executor, "executor == null");
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }
}
