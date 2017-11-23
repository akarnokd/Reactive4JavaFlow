/*
 * Copyright 2017 David Karnok
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
 * the License for the specific language governing permissions and limitations under the License.
 */

package hu.akarnokd.reactive4javaflow.impl.operators;

import hu.akarnokd.reactive4javaflow.*;
import hu.akarnokd.reactive4javaflow.impl.EmptySubscription;

import java.io.IOException;

/**
 * Signals two onErrors to each subscriber for testing purposes.
 */
public final class ParallelInvalid extends ParallelFolyam<Object> {

    @Override
    public void subscribeActual(FolyamSubscriber<? super Object>[] subscribers) {
        IOException ex = new IOException();
        for (FolyamSubscriber<? super Object> s : subscribers) {
            EmptySubscription.error(s, ex);
            s.onError(ex);
            s.onNext(0);
            s.onComplete();
            s.onComplete();
        }
    }

    @Override
    public int parallelism() {
        return 4;
    }

}
