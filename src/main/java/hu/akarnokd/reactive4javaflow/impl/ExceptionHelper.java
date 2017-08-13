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

import hu.akarnokd.reactive4javaflow.errors.CompositeThrowable;

import java.lang.invoke.VarHandle;
import java.util.concurrent.atomic.AtomicReference;

public final class ExceptionHelper {

    private ExceptionHelper() {
        throw new IllegalStateException("No instances!");
    }

    public static final Throwable TERMINATED = new Terminated();

    static final class Terminated extends Error {

        Terminated() {
            super("No further exceptions can be emitted through onError. Seeing this error indicates a bug in an operator.");
        }

        @Override
        public Throwable fillInStackTrace() {
            return this;
        }
    }

    public static boolean addThrowable(AtomicReference<Throwable> errors, Throwable t) {
        for (;;) {
            Throwable a = (Throwable) errors.getAcquire();
            if (a == TERMINATED) {
                return false;
            }
            Throwable b;
            if (a instanceof CompositeThrowable) {
                b = ((CompositeThrowable) a).copyAndAdd(t);
            } else
            if (a == null) {
                b = t;
            } else {
                b = new CompositeThrowable(a, t);
            }
            if (errors.compareAndSet(a, b)) {
                return true;
            }
        }
    }

    public static boolean addThrowable(Object target, VarHandle ERRORS, Throwable t) {
        for (;;) {
            Throwable a = (Throwable) ERRORS.getAcquire(target);
            if (a == TERMINATED) {
                return false;
            }
            Throwable b;
            if (a instanceof CompositeThrowable) {
                b = ((CompositeThrowable) a).copyAndAdd(t);
            } else
            if (a == null) {
                b = t;
            } else {
                b = new CompositeThrowable(a, t);
            }
            if (ERRORS.compareAndSet(target, a, b)) {
                return true;
            }
        }
    }

    public static Throwable terminate(Object target, VarHandle ERRORS) {
        Throwable a = (Throwable)ERRORS.getAcquire(target);
        if (a != TERMINATED) {
            a = (Throwable)ERRORS.getAndSet(target, TERMINATED);
        }
        return a;
    }


    public static Throwable terminate(AtomicReference<Throwable> errors) {
        Throwable a = (Throwable)errors.getAcquire();
        if (a != TERMINATED) {
            a = (Throwable)errors.getAndSet(TERMINATED);
        }
        return a;
    }
}
