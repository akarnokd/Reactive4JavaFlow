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

import hu.akarnokd.reactive4javaflow.TestHelper;
import org.junit.Test;

import java.io.IOException;
import java.lang.invoke.*;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

public class ExceptionHelperTest {

    Throwable error;
    static final VarHandle ERROR = VH.find(MethodHandles.lookup(), ExceptionHelperTest.class, "error", Throwable.class);

    @Test
    public void utilityClass() {
        TestHelper.checkUtilityClass(ExceptionHelper.class);
    }

    @Test
    public void terminated() {
        AtomicReference<Throwable> error = new AtomicReference<>();
        assertNull(ExceptionHelper.terminate(error));
        assertFalse(ExceptionHelper.addThrowable(error, new IOException()));
    }

    @Test
    public void addTerminateRace() {
        Throwable ex = new IOException();
        for (int i = 0; i < 1000; i++) {
            AtomicReference<Throwable> error = new AtomicReference<>();

            Runnable r1 = () -> ExceptionHelper.terminate(error);
            Runnable r2 = () -> ExceptionHelper.addThrowable(error, ex);

            TestHelper.race(r1, r2);

            assertSame(ExceptionHelper.TERMINATED, error.get());
        }
    }

    @Test
    public void addSuccessVarHandle() {
        assertTrue(ExceptionHelper.addThrowable(this, ERROR, new IOException()));

        assertNotNull(error);
    }

    @Test
    public void terminatedVarHandle() {
        assertNull(ExceptionHelper.terminate(this, ERROR));
        assertFalse(ExceptionHelper.addThrowable(this, ERROR, new IOException()));
    }

    @Test
    public void addTerminateRaceVarHandle() {
        Throwable ex = new IOException();
        for (int i = 0; i < 1000; i++) {
            Runnable r1 = () -> ExceptionHelper.terminate(this, ERROR);
            Runnable r2 = () -> ExceptionHelper.addThrowable(this, ERROR, ex);

            TestHelper.race(r1, r2);

            assertSame(ExceptionHelper.TERMINATED, error);
        }
    }
}
