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
import java.util.concurrent.*;

public class EsetlegTimeoutSelectorTest {

    @Test
    public void standard() {
        TestHelper.assertResult(
                Esetleg.just(1)
                .timeout(v -> Esetleg.timer(1, TimeUnit.MINUTES, SchedulerServices.single()))
                , 1
        );
    }


    @Test
    public void standardHidden() {
        TestHelper.assertResult(
                Esetleg.just(1).hide()
                        .timeout(v -> Esetleg.timer(1, TimeUnit.MINUTES, SchedulerServices.single()))
                , 1
        );
    }

    @Test
    public void error() {
        TestHelper.assertFailureComposed(-1,
                f -> f.timeout(v -> Esetleg.timer(1, TimeUnit.MINUTES, SchedulerServices.single())),
                IOException.class);
    }

    @Test
    public void timeoutFirst() {
        Esetleg.never()
                .timeout(Esetleg.timer(10, TimeUnit.MILLISECONDS, SchedulerServices.single()))
                .test()
                .awaitDone(5, TimeUnit.SECONDS)
                .assertFailureAndMessage(TimeoutException.class, "Timeout awaiting item index: 0");
    }


    @Test
    public void timeoutSecond() {
        Esetleg.just(1).concatWith(Esetleg.never())
                .timeout(v -> Esetleg.timer(10, TimeUnit.MILLISECONDS, SchedulerServices.single()))
                .test()
                .awaitDone(5, TimeUnit.SECONDS)
                .assertFailureAndMessage(TimeoutException.class, "Timeout awaiting item index: 1", 1);
    }


    @Test
    public void timeoutFirstConditional() {
        Esetleg.never()
                .timeout(Esetleg.timer(10, TimeUnit.MILLISECONDS, SchedulerServices.single()))
                .filter(v -> true)
                .test()
                .awaitDone(5, TimeUnit.SECONDS)
                .assertFailureAndMessage(TimeoutException.class, "Timeout awaiting item index: 0");
    }


    @Test
    public void timeoutSecondConditional() {
        Esetleg.just(1).concatWith(Esetleg.never())
                .timeout(v -> Esetleg.timer(10, TimeUnit.MILLISECONDS, SchedulerServices.single()))
                .filter(v -> true)
                .test()
                .awaitDone(5, TimeUnit.SECONDS)
                .assertFailureAndMessage(TimeoutException.class, "Timeout awaiting item index: 1", 1);
    }

    @Test
    public void itemTimeoutSelectorCrash() {
        Esetleg.just(1).concatWith(Esetleg.never())
                .timeout(v -> { throw new IOException(); })
                .test()
                .assertFailure(IOException.class, 1);
    }


    @Test
    public void itemTimeoutSelectorCrashConditional() {
        Esetleg.just(1).concatWith(Esetleg.never())
                .timeout(v -> { throw new IOException(); })
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class, 1);
    }

    @Test
    public void timeoutError() {
        Esetleg.just(1).concatWith(Esetleg.never())
                .timeout(v -> Esetleg.error(new IOException()))
                .test()
                .assertFailure(IOException.class, 1);
    }


    @Test
    public void timeoutConditional() {
        Esetleg.just(1).concatWith(Esetleg.never())
                .timeout(v -> Esetleg.error(new IOException()))
                .test()
                .assertFailure(IOException.class, 1);
    }

    @Test
    public void timeoutEmpty() {
        Esetleg.just(1).concatWith(Esetleg.never())
                .timeout(v -> Esetleg.empty())
                .test()
                .assertFailure(TimeoutException.class, 1);
    }

    @Test
    public void timeoutEmptyConditional() {
        Esetleg.just(1).concatWith(Esetleg.never())
                .timeout(v -> Esetleg.empty())
                .filter(v -> true)
                .test()
                .assertFailure(TimeoutException.class, 1);
    }
}
