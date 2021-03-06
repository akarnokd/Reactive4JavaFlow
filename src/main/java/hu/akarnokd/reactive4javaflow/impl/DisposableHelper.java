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
import hu.akarnokd.reactive4javaflow.functionals.AutoDisposable;

import java.lang.invoke.VarHandle;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Atomic utility methods to handle the replacing, updating and closing
 * of AutoDisposable containers.
 */
public enum DisposableHelper implements AutoDisposable {
    /**
     * DON'T LEAK; represents a terminal state and indicates any subsequent AutoDisposable
     * should be disposed when replacing/updating an AutoDisposable container.
     */
    CLOSED;

    public static boolean replace(AtomicReference<AutoDisposable> field, AutoDisposable d) {
        for (;;) {
            AutoDisposable a = field.get();
            if (a == CLOSED) {
                if (d != null) {
                    d.close();
                }
                return false;
            }
            if (field.compareAndSet(a, d)) {
                return true;
            }
        }
    }

    public static boolean update(AtomicReference<AutoDisposable> field, AutoDisposable d) {
        for (;;) {
            AutoDisposable a = field.get();
            if (a == CLOSED) {
                if (d != null) {
                    d.close();
                }
                return false;
            }
            if (field.compareAndSet(a, d)) {
                if (a != null) {
                    a.close();
                }
                return true;
            }
        }
    }

    public static boolean update(Object target, VarHandle field, AutoDisposable d) {
        for (;;) {
            AutoDisposable a = (AutoDisposable)field.getAcquire(target);
            if (a == CLOSED) {
                if (d != null) {
                    d.close();
                }
                return false;
            }
            if (field.compareAndSet(target, a, d)) {
                if (a != null) {
                    a.close();
                }
                return true;
            }
        }
    }

    public static boolean close(AtomicReference<AutoDisposable> field) {
        AutoDisposable a = field.getAcquire();
        if (a != CLOSED) {
            a = field.getAndSet(CLOSED);
            if (a != CLOSED) {
                if (a != null) {
                    a.close();
                }
                return true;
            }
        }
        return false;
    }

    public static boolean close(Object target, VarHandle FIELD) {
        AutoDisposable a = (AutoDisposable)FIELD.getAcquire(target);
        if (a != CLOSED) {
            a = (AutoDisposable)FIELD.getAndSet(target, CLOSED);
            if (a != CLOSED) {
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
            if (a == CLOSED) {
                if (d != null) {
                    d.close();
                }
                return false;
            }
            if (FIELD.compareAndSet(target, a, d)) {
                return true;
            }
        }
    }

    public static void closeSilently(AutoCloseable c) {
        if (c != null) {
            try {
                c.close();
            } catch (Throwable ex) {
                FolyamPlugins.onError(ex);
            }
        }
    }

    @Override
    public void close() {
        // deliberately no-op
    }
}
