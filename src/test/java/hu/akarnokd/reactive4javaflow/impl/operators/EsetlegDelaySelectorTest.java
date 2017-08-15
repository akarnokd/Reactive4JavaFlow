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

public class EsetlegDelaySelectorTest {

    @Test
    public void standard() {
        TestHelper.assertResult(
                Esetleg.just(1)
                .delay(v -> Esetleg.timer(v, TimeUnit.MILLISECONDS, SchedulerServices.single())),
                1
        );
    }

    @Test
    public void standard1() {
        TestHelper.assertResult(
                Esetleg.just(1)
                        .delay(v -> Esetleg.just(2)),
                1
        );
    }

    @Test
    public void standard2() {
        TestHelper.assertResult(
                Esetleg.just(1)
                        .delay(v -> Esetleg.empty()),
                1
        );
    }

    @Test
    public void error() {
        Esetleg.error(new IOException())
                .delay(v -> Esetleg.just(1))
        .test()
        .assertFailure(IOException.class);
    }

    @Test
    public void errorInner() {
        Esetleg.just(1)
                .delay(v -> v == 1 ? Esetleg.error(new IOException()) : Esetleg.just(v))
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void selectorCrash() {
        Esetleg.just(1)
                .delay(v -> { throw new IOException(); })
                .test()
                .assertFailure(IOException.class);
    }

}
