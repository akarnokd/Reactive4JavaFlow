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
import hu.akarnokd.reactive4javaflow.impl.operators.*;
import hu.akarnokd.reactive4javaflow.impl.schedulers.ImmediateSchedulerService;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public abstract class ConnectableFolyam<T> extends Folyam<T> {

    protected abstract AutoDisposable connectActual(Consumer<? super AutoDisposable> connectionHandler);

    public abstract void reset();

    public final void connect(Consumer<? super AutoDisposable> connectionHandler) {
        connectActual(connectionHandler);
    }

    public final AutoDisposable connect() {
        return connectActual(v -> { });
    }

    public final Folyam<T> autoConnect() {
        return autoConnect(1, v -> { });
    }

    public final Folyam<T> autoConnect(int minSubscribers) {
        return autoConnect(minSubscribers, v -> { });
    }

    public final Folyam<T> autoConnect(int minSubscribers, Consumer<? super AutoDisposable> connectionHandler) {
        Objects.requireNonNull(connectionHandler, "connectionHandler == null");
        if (minSubscribers <= 0) {
            connect(connectionHandler);
            return this;
        }
        return FolyamPlugins.onAssembly(new ConnectableFolyamAutoConnect<>(this, minSubscribers, connectionHandler));
    }

    public final Folyam<T> refCount() {
        return refCount(1);
    }

    public final Folyam<T> refCount(int minSubscribers) {
        return FolyamPlugins.onAssembly(new ConnectableFolyamRefCount<>(this, minSubscribers, 0, TimeUnit.NANOSECONDS, ImmediateSchedulerService.INSTANCE));
    }

    public final Folyam<T> refCount(long timeout, TimeUnit unit, SchedulerService executor) {
        return refCount(1, timeout, unit, executor);
    }

    public final Folyam<T> refCount(int minSubscribers, long timeout, TimeUnit unit, SchedulerService executor) {
        Objects.requireNonNull(unit, "unit == null");
        Objects.requireNonNull(executor, "executor == null");
        return FolyamPlugins.onAssembly(new ConnectableFolyamRefCount<>(this, minSubscribers, timeout, unit, executor));
    }
}
