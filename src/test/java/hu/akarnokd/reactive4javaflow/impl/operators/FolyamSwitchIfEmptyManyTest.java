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

public class FolyamSwitchIfEmptyManyTest {

    @Test
    public void standard() {
        TestHelper.assertResult(
                Folyam.range(1, 5)
                        .switchIfEmptyMany(List.of(Folyam.empty(), Folyam.range(6, 5))),
                1, 2, 3, 4, 5
        );
    }


    @Test
    public void standardHidden() {
        TestHelper.assertResult(
                Folyam.range(1, 5).hide()
                        .switchIfEmptyMany(List.of(Folyam.empty(), Folyam.range(6, 5))),
                1, 2, 3, 4, 5
        );
    }


    @Test
    public void standard2() {
        TestHelper.assertResult(
                Folyam.empty()
                        .switchIfEmptyMany(List.of(Folyam.empty(), Folyam.range(6, 5))),
                6, 7, 8, 9, 10
        );
    }

    @Test
    public void error() {
        TestHelper.assertFailureComposed(-1,
                f -> f.switchIfEmptyMany(List.of(Folyam.empty(), Folyam.range(6, 5))),
                IOException.class);
    }

    @Test
    public void errorInner() {
        TestHelper.assertFailureComposed(0,
                f -> f.switchIfEmptyMany(List.of(Folyam.empty(), Folyam.error(new IOException()))),
                IOException.class);
    }

    @Test
    public void iteratorCrash() {
        Folyam.empty()
                .switchIfEmptyMany(new FailingMappedIterable<>(1, 10, 10, v -> Folyam.just(1)))
                .test()
                .assertFailure(IllegalStateException.class);
    }

    @Test
    public void hasNextCrash() {
        Folyam.empty()
                .switchIfEmptyMany(new FailingMappedIterable<>(10, 1, 10, v -> Folyam.just(1)))
                .test()
                .assertFailure(IllegalStateException.class);
    }


    @Test
    public void nextCrash() {
        Folyam.empty()
                .switchIfEmptyMany(new FailingMappedIterable<>(10, 10, 1, v -> Folyam.just(1)))
                .test()
                .assertFailure(IllegalStateException.class);
    }

    @Test
    public void iteratorCrashConditional() {
        Folyam.empty()
                .switchIfEmptyMany(new FailingMappedIterable<>(1, 10, 10, v -> Folyam.just(1)))
                .filter(v -> true)
                .test()
                .assertFailure(IllegalStateException.class);
    }

    @Test
    public void hasNextCrashConditional() {
        Folyam.empty()
                .switchIfEmptyMany(new FailingMappedIterable<>(10, 1, 10, v -> Folyam.just(1)))
                .filter(v -> true)
                .test()
                .assertFailure(IllegalStateException.class);
    }


    @Test
    public void nextCrashConditional() {
        Folyam.empty()
                .switchIfEmptyMany(new FailingMappedIterable<>(10, 10, 1, v -> Folyam.just(1)))
                .filter(v -> true)
                .test()
                .assertFailure(IllegalStateException.class);
    }

    @Test
    public void allEmpty() {
        Folyam.empty()
                .switchIfEmptyMany(List.of(Folyam.empty(), Folyam.empty()))
                .test()
                .assertResult();
    }


    @Test
    public void allEmptyConditional() {
        Folyam.empty()
                .switchIfEmptyMany(List.of(Folyam.empty(), Folyam.empty()))
                .filter(v -> true)
                .test()
                .assertResult();
    }
}
