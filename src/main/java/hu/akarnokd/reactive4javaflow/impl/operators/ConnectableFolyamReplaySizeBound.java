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
import hu.akarnokd.reactive4javaflow.impl.*;
import hu.akarnokd.reactive4javaflow.processors.CachingProcessor;

import java.lang.invoke.*;
import java.util.concurrent.Flow;
import java.util.function.Consumer;

public final class ConnectableFolyamReplaySizeBound<T> extends ConnectableFolyam<T> {

    final Folyam<T> source;

    final int maxSize;

    CachingProcessor<T> processor;
    static final VarHandle PROCESSOR = VH.find(MethodHandles.lookup(), ConnectableFolyamReplaySizeBound.class, "processor", CachingProcessor.class);

    static final Flow.Subscription CONNECT = new BooleanSubscription();

    public ConnectableFolyamReplaySizeBound(Folyam<T> source, int maxSize) {
        this.source = source;
        this.maxSize = maxSize;
    }

    @Override
    protected AutoDisposable connectActual(Consumer<? super AutoDisposable> connectionHandler) {
        for (; ; ) {
            CachingProcessor<T> mp = (CachingProcessor<T>) PROCESSOR.getAcquire(this);
            if (mp == null) {
                mp = new CachingProcessor<>(maxSize);
                if (!PROCESSOR.compareAndSet(this, null, mp)) {
                    continue;
                }
            }
            boolean b = mp.prepare(CONNECT);
            connectionHandler.accept(mp);
            if (b) {
                source.subscribe(mp);
            }

            return mp;
        }
    }

    @Override
    public void reset() {
        CachingProcessor<T> mp = (CachingProcessor<T>) PROCESSOR.getAcquire(this);
        if (mp != null) {
            if (mp.hasTerminated()) {
                PROCESSOR.compareAndSet(this, mp, null);
            }
        }
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        for (; ; ) {
            CachingProcessor<T> mp = (CachingProcessor<T>) PROCESSOR.getAcquire(this);
            if (mp == null) {
                mp = new CachingProcessor<>(maxSize);
                if (!PROCESSOR.compareAndSet(this, null, mp)) {
                    continue;
                }
            }
            mp.subscribe(s);
            break;
        }
    }

}