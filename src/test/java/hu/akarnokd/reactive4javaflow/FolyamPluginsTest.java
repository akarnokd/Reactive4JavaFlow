/*
 * Copyright 2016-2017 David Karnok
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

package hu.akarnokd.reactive4javaflow;

import org.junit.Test;

import java.io.*;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class FolyamPluginsTest {

    @Test
    public void checkUtility() {
        TestHelper.checkUtilityClass(FolyamPlugins.class);
    }

    @Test
    public void onAssembly() {
        int[] counter = { 0 };
        try {
            FolyamPlugins.setFolyamOnAssembly(v -> { counter[0]++; return v; });
            FolyamPlugins.setEsetlegOnAssembly(v -> { counter[0]++; return v; });
            FolyamPlugins.setConnectableOnAssembly(v -> { counter[0]++; return v; });
            FolyamPlugins.setParallelOnAssembly(v -> { counter[0]++; return v; });

            FolyamPlugins.setFolyamOnSubscribe((p, v) -> { counter[0]++; return v; });
            FolyamPlugins.setEsetlegOnSubscribe((p, v) -> { counter[0]++; return v; });
            FolyamPlugins.setParallelOnSubscribe((p, v) -> { counter[0]++; return v; });

            Folyam.just(1).test().assertResult(1);
            assertEquals(2, counter[0]);

            Esetleg.just(1).test().assertResult(1);
            assertEquals(4, counter[0]);

            Esetleg.just(1).publish().autoConnect().test().assertResult(1);
            assertEquals(11, counter[0]);

            Folyam.just(1).parallel(1).sequential().test().assertResult(1);
            assertEquals(17, counter[0]);
        } finally {
            FolyamPlugins.reset();
        }

        assertNull(FolyamPlugins.getFolyamOnAssembly());
        assertNull(FolyamPlugins.getEsetlegOnAssembly());
        assertNull(FolyamPlugins.getParallelOnAssembly());
        assertNull(FolyamPlugins.getConnectableOnAssembly());

        assertNull(FolyamPlugins.getFolyamOnSubscribe());
        assertNull(FolyamPlugins.getEsetlegOnSubscribe());
        assertNull(FolyamPlugins.getParallelOnSubscribe());

        Folyam.just(1).test().assertResult(1);
        Esetleg.just(1).test().assertResult(1);
        Esetleg.just(1).publish().autoConnect().test().assertResult(1);
        Folyam.just(1).parallel().sequential().test().assertResult(1);

        assertEquals(17, counter[0]);
    }

    @Test
    public void errorHandlerCrash() {
        OutputStream out = System.err;
        try {
            ByteArrayOutputStream bout = new ByteArrayOutputStream();
            System.setErr(new PrintStream(bout));

            assertNull(FolyamPlugins.getOnError());

            FolyamPlugins.setOnError(v -> { throw new RuntimeException(); });

            FolyamPlugins.onError(new IOException());

            String str = bout.toString();

            assertTrue(str, str.contains("IOException"));
            assertTrue(str, str.contains("RuntimeException"));
        } finally {
            FolyamPlugins.reset();
        }
    }

    @Test(expected = InternalError.class)
    public void fatal() {
        FolyamPlugins.handleFatal(new IOException());
        FolyamPlugins.handleFatal(new InternalError());
    }

    @Test
    public void schedulers() {
        SchedulerService sch = SchedulerServices.trampoline();
        try {
            FolyamPlugins.setOnInitComputationSchedulerService(v -> sch);
            FolyamPlugins.setOnInitSingleSchedulerService(v -> sch);
            FolyamPlugins.setOnInitIOSchedulerService(v -> sch);
            FolyamPlugins.setOnInitNewThreadSchedulerService(v -> sch);

            FolyamPlugins.setOnComputationSchedulerService(v -> sch);
            FolyamPlugins.setOnSingleSchedulerService(v -> sch);
            FolyamPlugins.setOnIOSchedulerService(v -> sch);
            FolyamPlugins.setOnNewThreadSchedulerService(v -> sch);

            Folyam.just(1).delay(100, TimeUnit.MILLISECONDS, SchedulerServices.computation())
                    .test()
                    .assertResult(1);

            Folyam.just(1).delay(100, TimeUnit.MILLISECONDS, SchedulerServices.single())
                    .test()
                    .assertResult(1);

            Folyam.just(1).delay(100, TimeUnit.MILLISECONDS, SchedulerServices.io())
                    .test()
                    .assertResult(1);

            Folyam.just(1).delay(100, TimeUnit.MILLISECONDS, SchedulerServices.newThread())
                    .test()
                    .assertResult(1);
        } finally {
            FolyamPlugins.reset();
        }

        assertNull(FolyamPlugins.getOnInitComputationSchedulerService());
        assertNull(FolyamPlugins.getOnInitSingleSchedulerService());
        assertNull(FolyamPlugins.getOnInitIOSchedulerService());
        assertNull(FolyamPlugins.getOnInitNewThreadSchedulerService());

        assertNull(FolyamPlugins.getOnComputationSchedulerService());
        assertNull(FolyamPlugins.getOnSingleSchedulerService());
        assertNull(FolyamPlugins.getOnIOSchedulerService());
        assertNull(FolyamPlugins.getOnNewThreadSchedulerService());

        String str = Folyam.just(1)
                .subscribeOn(SchedulerServices.computation())
                .map(v -> Thread.currentThread().getName())
                .blockingLast(null);

        assertTrue(str, str.contains("Reactive4JavaFlow"));

        str = Folyam.just(1)
                .subscribeOn(SchedulerServices.single())
                .map(v -> Thread.currentThread().getName())
                .blockingLast(null);

        assertTrue(str, str.contains("Reactive4JavaFlow"));

        str = Folyam.just(1)
                .subscribeOn(SchedulerServices.io())
                .map(v -> Thread.currentThread().getName())
                .blockingLast(null);

        assertTrue(str, str.contains("Reactive4JavaFlow"));

        str = Folyam.just(1)
                .subscribeOn(SchedulerServices.newThread())
                .map(v -> Thread.currentThread().getName())
                .blockingLast(null);

        assertTrue(str, str.contains("Reactive4JavaFlow"));
    }
}
