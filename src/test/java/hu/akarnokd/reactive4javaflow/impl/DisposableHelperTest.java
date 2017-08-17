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
import hu.akarnokd.reactive4javaflow.disposables.BooleanAutoDisposable;
import hu.akarnokd.reactive4javaflow.functionals.AutoDisposable;
import org.junit.Test;

import java.io.IOException;
import java.lang.invoke.*;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DisposableHelperTest {

    AutoDisposable d;
    static final VarHandle D;

    static {
        try {
            D = MethodHandles.lookup().findVarHandle(DisposableHelperTest.class, "d", AutoDisposable.class);
        } catch (Throwable ex) {
            throw new InternalError(ex);
        }
    }

    @Test
    public void closedReplace() {
        AtomicReference<AutoDisposable> ref = new AtomicReference<>();

        BooleanAutoDisposable d = new BooleanAutoDisposable();

        DisposableHelper.close(ref);

        assertFalse(DisposableHelper.replace(ref, null));

        assertFalse(DisposableHelper.replace(ref, d));

        assertTrue(d.isClosed());
    }

    @Test
    public void replaceRace() {
        for (int i = 0; i < 1000; i++) {
            AtomicReference<AutoDisposable> ref = new AtomicReference<>();

            BooleanAutoDisposable d1 = new BooleanAutoDisposable();
            BooleanAutoDisposable d2 = new BooleanAutoDisposable();

            Runnable r1 = () -> DisposableHelper.replace(ref, d1);
            Runnable r2 = () -> DisposableHelper.replace(ref, d2);

            TestHelper.race(r1, r2);
        }
    }

    @Test
    public void closedUpdate() {
        AtomicReference<AutoDisposable> ref = new AtomicReference<>();

        BooleanAutoDisposable d0 = new BooleanAutoDisposable();

        DisposableHelper.update(ref, null);

        DisposableHelper.update(ref, d0);

        BooleanAutoDisposable d1 = new BooleanAutoDisposable();

        DisposableHelper.update(ref, d1);

        assertTrue(d0.isClosed());

        DisposableHelper.close(ref);

        assertTrue(d1.isClosed());

        assertFalse(DisposableHelper.update(ref, null));

        BooleanAutoDisposable d = new BooleanAutoDisposable();

        assertFalse(DisposableHelper.update(ref, d));

        assertTrue(d.isClosed());
    }

    @Test
    public void updateRace() {
        for (int i = 0; i < 1000; i++) {
            AtomicReference<AutoDisposable> ref = new AtomicReference<>();

            BooleanAutoDisposable d1 = new BooleanAutoDisposable();
            BooleanAutoDisposable d2 = new BooleanAutoDisposable();

            Runnable r1 = () -> DisposableHelper.update(ref, d1);
            Runnable r2 = () -> DisposableHelper.update(ref, d2);

            TestHelper.race(r1, r2);

            assertTrue(d1.isClosed() != d2.isClosed());
        }
    }


    @Test
    public void replaceRaceVarHandle() {
        for (int i = 0; i < 1000; i++) {

            BooleanAutoDisposable d1 = new BooleanAutoDisposable();
            BooleanAutoDisposable d2 = new BooleanAutoDisposable();

            Runnable r1 = () -> DisposableHelper.replace(this, D, d1);
            Runnable r2 = () -> DisposableHelper.replace(this, D, d2);

            TestHelper.race(r1, r2);
        }
    }

    @Test
    public void updateRaceVarHandle() {
        for (int i = 0; i < 1000; i++) {
            BooleanAutoDisposable d1 = new BooleanAutoDisposable();
            BooleanAutoDisposable d2 = new BooleanAutoDisposable();

            Runnable r1 = () -> DisposableHelper.update(this, D, d1);
            Runnable r2 = () -> DisposableHelper.update(this, D, d2);

            TestHelper.race(r1, r2);

            assertTrue(d1.isClosed() != d2.isClosed());
        }
    }

    @Test
    public void closeSilently() {
        DisposableHelper.closeSilently(null);
        DisposableHelper.closeSilently(() -> { });

        TestHelper.withErrorTracking(errors -> {
            DisposableHelper.closeSilently(() -> { throw new IOException(); });

            TestHelper.assertError(errors, 0, IOException.class);
        });
    }
}
