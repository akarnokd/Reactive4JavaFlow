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

import hu.akarnokd.reactive4javaflow.functionals.AutoDisposable;

import java.lang.invoke.VarHandle;
import java.util.concurrent.atomic.AtomicReference;

public enum DisposableHelper implements AutoDisposable {
    DISPOSED;

    public static boolean replace(AtomicReference<AutoDisposable> field, AutoDisposable d) {
        for (;;) {
            AutoDisposable a = field.get();
            if (a == DISPOSED) {
                d.close();
                return false;
            }
            if (field.compareAndSet(a, d)) {
                return true;
            }
        }
    }

    public static boolean dispose(AtomicReference<AutoDisposable> field) {
        AutoDisposable a = field.getAcquire();
        if (a != DISPOSED) {
            a = field.getAndSet(DISPOSED);
            if (a != DISPOSED) {
                if (a != null) {
                    a.close();
                }
                return true;
            }
        }
        return false;
    }

    public static boolean dispose(Object target, VarHandle FIELD) {
        AutoDisposable a = (AutoDisposable)FIELD.getAcquire(target);
        if (a != DISPOSED) {
            a = (AutoDisposable)FIELD.getAndSet(target, DISPOSED);
            if (a != DISPOSED) {
                if (a != null) {
                    a.close();
                }
                return true;
            }
        }
        return false;
    }

    public static boolean replace(Object target, VarHandle FIELD, AutoDisposable d) {
        for (;;) {
            AutoDisposable a = (AutoDisposable)FIELD.getAcquire(target);
            if (a == DISPOSED) {
                d.close();
                return false;
            }
            if (FIELD.compareAndSet(target, a, d)) {
                return true;
            }
        }
    }

    public static boolean isDisposed(Object target, VarHandle FIELD) {
        return FIELD.getAcquire(target) == DISPOSED;
    }

    @Override
    public void close() {
        // deliberately no-op
    }
}
