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
import hu.akarnokd.reactive4javaflow.functionals.AutoDisposable;

import java.lang.invoke.*;
import java.util.function.Consumer;

public final class ConnectableFolyamAutoConnect<T> extends Folyam<T> {

    final ConnectableFolyam<T> source;

    final int minSubscribers;

    final Consumer<? super AutoDisposable> onConnect;

    int count;
    static final VarHandle COUNT;

    static {
        try {
            COUNT = MethodHandles.lookup().findVarHandle(ConnectableFolyamAutoConnect.class, "count", int.class);
        } catch (Throwable ex) {
            throw new InternalError(ex);
        }
    }

    public ConnectableFolyamAutoConnect(ConnectableFolyam<T> source, int minSubscribers, Consumer<? super AutoDisposable> onConnect) {
        this.source = source;
        this.minSubscribers = minSubscribers;
        this.onConnect = onConnect;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        source.subscribe(s);
        if ((int)COUNT.getAndAdd(this, 1) + 1 == minSubscribers) {
            source.connect(onConnect);
        }
    }
}
