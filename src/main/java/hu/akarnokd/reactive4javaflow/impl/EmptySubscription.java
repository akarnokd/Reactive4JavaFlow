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

package hu.akarnokd.reactive4javaflow.impl;

import hu.akarnokd.reactive4javaflow.FolyamPlugins;
import hu.akarnokd.reactive4javaflow.fused.FusedSubscription;

import java.util.concurrent.Flow;

public enum EmptySubscription implements FusedSubscription<Object> {
    INSTANCE
    ;

    @Override
    public Object poll() throws Throwable {
        return null;
    }

    @Override
    public boolean isEmpty() {
        return true;
    }

    @Override
    public void clear() {
        // deliberately no-op
    }


    @Override
    public int requestFusion(int mode) {
        return ASYNC;
    }

    @Override
    public void request(long n) {
        if (n <= 0L) {
            FolyamPlugins.onError(new IllegalArgumentException("n <= 0L"));
        }
    }

    @Override
    public void cancel() {
        // deliberately no-op
    }

    public static void complete(Flow.Subscriber<?> s) {
        s.onSubscribe(INSTANCE);
        s.onComplete();
    }

    public static void error(Flow.Subscriber<?> s, Throwable ex) {
        s.onSubscribe(INSTANCE);
        s.onError(ex);
    }
}
