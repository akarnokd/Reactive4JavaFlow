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

package hu.akarnokd.reactive4javaflow.impl.consumers;

import hu.akarnokd.reactive4javaflow.*;
import org.junit.Test;

import java.io.IOException;

public class BlockingLambdaConsumerIgnoreTest {

    @Test(timeout = 1000)
    public void normalSync() {
        Folyam.range(1, 5).blockingSubscribe();
    }

    @Test(timeout = 1000)
    public void normalAsync1() {
        Folyam.range(1, 5)
                .subscribeOn(SchedulerServices.single()).blockingSubscribe();
    }


    @Test(timeout = 1000)
    public void normalAsync2() {
        Folyam.range(1, 5)
                .observeOn(SchedulerServices.single()).blockingSubscribe();
    }

    @Test(timeout = 1000)
    public void normalSyncError() {
        TestHelper.withErrorTracking(errors -> {
            Folyam.error(new IOException()).blockingSubscribe();

            TestHelper.assertError(errors, 0, IOException.class);
        });
    }

    @Test
    public void interrupt() {
        TestHelper.withErrorTracking(errors -> {
            try {

                Thread.currentThread().interrupt();

                Folyam.never().blockingSubscribe();

            } finally {
                Thread.interrupted();
            }

            TestHelper.assertError(errors, 0, InterruptedException.class);
        });
    }
}
