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
import java.util.List;

public class EsetlegSwitchIfEmptyManyTest {

    @Test
    public void standard() {
        TestHelper.assertResult(
                Esetleg.just(1)
                        .switchIfEmptyMany(List.of(Esetleg.empty(), Esetleg.just(6))),
                1
        );
    }


    @Test
    public void standardHidden() {
        TestHelper.assertResult(
                Esetleg.just(1).hide()
                        .switchIfEmptyMany(List.of(Esetleg.empty(), Esetleg.just(6))),
                1
        );
    }


    @Test
    public void standard2() {
        TestHelper.assertResult(
                Esetleg.empty()
                        .switchIfEmptyMany(List.of(Esetleg.empty(), Esetleg.just(6))),
                6
        );
    }

    @Test
    public void error() {
        TestHelper.assertFailureComposed(-1,
                f -> f.switchIfEmptyMany(List.of(Esetleg.empty(), Esetleg.just(6))),
                IOException.class);
    }

    @Test
    public void errorInner() {
        TestHelper.assertFailureComposed(0,
                f -> f.switchIfEmptyMany(List.of(Esetleg.empty(), Esetleg.error(new IOException()))),
                IOException.class);
    }

    @Test
    public void iteratorCrash() {
        Esetleg.empty()
                .switchIfEmptyMany(new FailingMappedIterable<>(1, 10, 10, v -> Esetleg.just(1)))
                .test()
                .assertFailure(IllegalStateException.class);
    }

    @Test
    public void hasNextCrash() {
        Esetleg.empty()
                .switchIfEmptyMany(new FailingMappedIterable<>(10, 1, 10, v -> Esetleg.just(1)))
                .test()
                .assertFailure(IllegalStateException.class);
    }


    @Test
    public void nextCrash() {
        Esetleg.empty()
                .switchIfEmptyMany(new FailingMappedIterable<>(10, 10, 1, v -> Esetleg.just(1)))
                .test()
                .assertFailure(IllegalStateException.class);
    }

    @Test
    public void iteratorCrashConditional() {
        Esetleg.empty()
                .switchIfEmptyMany(new FailingMappedIterable<>(1, 10, 10, v -> Esetleg.just(1)))
                .filter(v -> true)
                .test()
                .assertFailure(IllegalStateException.class);
    }

    @Test
    public void hasNextCrashConditional() {
        Esetleg.empty()
                .switchIfEmptyMany(new FailingMappedIterable<>(10, 1, 10, v -> Esetleg.just(1)))
                .filter(v -> true)
                .test()
                .assertFailure(IllegalStateException.class);
    }


    @Test
    public void nextCrashConditional() {
        Esetleg.empty()
                .switchIfEmptyMany(new FailingMappedIterable<>(10, 10, 1, v -> Esetleg.just(1)))
                .filter(v -> true)
                .test()
                .assertFailure(IllegalStateException.class);
    }

    @Test
    public void allEmpty() {
        Esetleg.empty()
                .switchIfEmptyMany(List.of(Esetleg.empty(), Esetleg.empty()))
                .test()
                .assertResult();
    }


    @Test
    public void allEmptyConditional() {
        Esetleg.empty()
                .switchIfEmptyMany(List.of(Esetleg.empty(), Esetleg.empty()))
                .filter(v -> true)
                .test()
                .assertResult();
    }
}
