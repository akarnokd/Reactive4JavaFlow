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
import hu.akarnokd.reactive4javaflow.hot.*;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;

import static org.junit.Assert.*;

public class FolyamBlockingStreamTest {
    @Test
    public void normal() {
        List<Integer> list = Folyam.range(1, 5)
                .blockingStream()
                .limit(3)
                .collect(Collectors.toList());

        assertEquals(Arrays.asList(1, 2, 3), list);
    }

    @Test
    public void normal2() {
        List<Integer> list = Folyam.range(1, 5)
                .blockingStream(1)
                .limit(3)
                .collect(Collectors.toList());

        assertEquals(Arrays.asList(1, 2, 3), list);
    }

    @Test(timeout = 1000)
    public void asyncSupplied() throws Exception {
        DirectProcessor<Integer> dp = new DirectProcessor<>();
        CountDownLatch cdl = new CountDownLatch(1);

        AutoDisposable d = SchedulerServices.single().schedule(() -> {
            while (!dp.hasSubscribers()) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException ex) {
                    cdl.countDown();
                    return;
                }
            }

            for (int i = 1; i < 6; i++) {
                if (!dp.tryOnNext(i)) {
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException ex) {
                        cdl.countDown();
                        return;
                    }
                }
            }

            while (dp.hasSubscribers()) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException ex) {
                    cdl.countDown();
                    return;
                }
            }

            cdl.countDown();
        });

        try (d) {
            try (Stream<Integer> st = dp.blockingStream()
                    .limit(3)) {
                List<Integer> list = st
                        .collect(Collectors.toList());

                assertEquals(Arrays.asList(1, 2, 3), list);
            }
            assertTrue(cdl.await(5, TimeUnit.SECONDS));
        }
    }
}
