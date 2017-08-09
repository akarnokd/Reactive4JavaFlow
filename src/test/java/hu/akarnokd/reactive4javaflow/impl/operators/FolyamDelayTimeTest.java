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
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class FolyamDelayTimeTest {

    @Test
    public void standard() {
        TestHelper.assertResult(
                Folyam.range(1, 5).delay(1, TimeUnit.MILLISECONDS, SchedulerServices.single())
                , 1, 2, 3, 4, 5
        );
    }

    @Test
    public void error() {
        for (int i = 0; i < 100; i++) {
            Folyam.range(1, 5).concatWith(Folyam.error(new IOException()))
                    .delay(1, TimeUnit.MILLISECONDS, SchedulerServices.single())
                    .test()
                    .awaitDone(5, TimeUnit.SECONDS)
                    .assertFailure(IOException.class, 1, 2, 3, 4, 5);
        }
    }
}
