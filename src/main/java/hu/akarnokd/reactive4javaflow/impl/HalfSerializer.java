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
import hu.akarnokd.reactive4javaflow.FolyamSubscriber;
import hu.akarnokd.reactive4javaflow.fused.ConditionalSubscriber;

import java.lang.invoke.VarHandle;

public final class HalfSerializer {

    private HalfSerializer() {
        throw new IllegalStateException("No instances!");
    }

    public static <T> void onNext(FolyamSubscriber<T> actual, Object target, VarHandle WIP, VarHandle ERRORS, T item) {
        if ((int)WIP.getAcquire(target) == 0 && WIP.compareAndSet(target, 0, 1)) {
            actual.onNext(item);
            if (!WIP.compareAndSet(target, 1, 0)) {
                Throwable ex = ExceptionHelper.terminate(target, ERRORS);
                if (ex == null) {
                    actual.onComplete();
                } else {
                    actual.onError(ex);
                }
            }
        }
    }
    public static <T> boolean tryOnNext(ConditionalSubscriber<T> actual, Object target, VarHandle WIP, VarHandle ERRORS, T item) {
        if ((int)WIP.getAcquire(target) == 0 && WIP.compareAndSet(target, 0, 1)) {
            boolean b = actual.tryOnNext(item);
            if (!WIP.compareAndSet(target, 1, 0)) {
                Throwable ex = ExceptionHelper.terminate(target, ERRORS);
                if (ex == null) {
                    actual.onComplete();
                } else {
                    actual.onError(ex);
                }
                return false;
            }
            return b;
        }
        return false;
    }

    public static void onError(FolyamSubscriber<?> actual, Object target, VarHandle WIP, VarHandle ERRORS, Throwable t) {
        if (ExceptionHelper.addThrowable(target, ERRORS, t)) {
            if ((int) WIP.getAndAdd(target, 1) == 0) {
                Throwable ex = ExceptionHelper.terminate(target, ERRORS);
                actual.onError(ex);
                return;
            }
        }
        FolyamPlugins.onError(t);
    }

    public static void onComplete(FolyamSubscriber<?> actual, Object target, VarHandle WIP, VarHandle ERRORS) {
        if ((int) WIP.getAndAdd(target, 1) == 0) {
            Throwable ex = ExceptionHelper.terminate(target, ERRORS);
            if (ex == null) {
                actual.onComplete();
            } else {
                actual.onError(ex);
            }
        }
    }
}
