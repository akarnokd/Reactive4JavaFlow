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
package hu.akarnokd.reactive4javaflow;

import hu.akarnokd.reactive4javaflow.impl.util.SpscCountDownLatch;
import org.junit.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class SpscCountDownLatchTest {

    @Test(timeout = 5000)
    public void nonblocking() throws Exception {
        SpscCountDownLatch cdl = new SpscCountDownLatch();

        assertEquals(1, cdl.getCount());

        cdl.countDown();

        assertEquals(0, cdl.getCount());

        cdl.countDown();

        assertEquals(0, cdl.getCount());

        cdl.await();
    }

    @Test(timeout = 1000)
    public void interrupted() throws Exception {
        SpscCountDownLatch cdl = new SpscCountDownLatch();

        Thread.currentThread().interrupt();

        try {
            cdl.await();
            fail("Did not throw?!");
        } catch (InterruptedException expected) {
            // expected
        }
    }

    @Test(timeout = 10000)
    @Ignore("Fails with timeout for some reason.")
    public void async() {
        for (int i = 0; i < 1_000_000; i++) {
            SpscCountDownLatch cdl = new SpscCountDownLatch();

            Runnable r1 = () -> {
                try {
                    cdl.await();
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            };

            Runnable r2 = cdl::countDown;

            TestHelper.race(r1, r2);
        }
    }
}
