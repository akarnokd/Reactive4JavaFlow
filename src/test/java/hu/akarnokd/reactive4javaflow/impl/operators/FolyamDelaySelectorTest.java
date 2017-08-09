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

public class FolyamDelaySelectorTest {

    @Test
    public void standard() {
        TestHelper.assertResult(
                Folyam.range(1, 10)
                .delay(v -> Folyam.timer(v, TimeUnit.MILLISECONDS, SchedulerServices.single())),
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10
        );
    }

    @Test
    public void standard1() {
        TestHelper.assertResult(
                Folyam.range(1, 10)
                        .delay(v -> Folyam.just(1)),
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10
        );
    }

    @Test
    public void standard2() {
        TestHelper.assertResult(
                Folyam.range(1, 10)
                        .delay(v -> Folyam.empty()),
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10
        );
    }

    @Test
    public void error() {
        Folyam.error(new IOException())
                .delay(v -> Folyam.just(1))
        .test()
        .assertFailure(IOException.class);
    }

    @Test
    public void errorInner() {
        Folyam.range(1, 5)
                .delay(v -> v == 3 ? Folyam.error(new IOException()) : Folyam.just(v))
                .test()
                .assertFailure(IOException.class, 1, 2, 4, 5);
    }

    @Test
    public void selectorCrash() {
        Folyam.range(1, 5)
                .delay(v -> { throw new IOException(); })
                .test()
                .assertFailure(IOException.class);
    }

}
