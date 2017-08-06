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

import java.util.*;

public class FolyamIterableTest {

    @Test
    public void standard() {
        TestHelper.assertResult(
                Folyam.fromIterable(List.of(1, 2, 3, 4, 5)),
                1, 2, 3, 4, 5);
    }

    @Test
    public void standardConditional() {
        TestHelper.assertResult(
                Folyam.fromIterable(List.of(1, 2, 3, 4, 5))
                    .filter(v -> true),
                1, 2, 3, 4, 5);
    }

    @Test
    public void standard1() {
        TestHelper.assertResult(
                Folyam.fromIterable(List.of(1)),
                1);
    }


    @Test
    public void standard1Conditional() {
        TestHelper.assertResult(
                Folyam.fromIterable(List.of(1))
                        .filter(v -> true),
                1);
    }

    @Test
    public void empty() {
        Folyam.fromIterable(List.of())
                .test()
                .assertResult();
    }

    @Test
    public void iteratorFail() {
        Folyam.fromIterable(new FailingIterable(1, 10, 10))
                .test()
                .assertFailureAndMessage(IllegalStateException.class, "iterator");
    }

    @Test
    public void hasNextInitialFail() {
        Folyam.fromIterable(new FailingIterable(10, 1, 10))
                .test()
                .assertFailureAndMessage(IllegalStateException.class, "hasNext");
    }

    @Test
    public void hasNextFail() {
        Folyam.fromIterable(new FailingIterable(10, 2, 10))
                .test()
                .assertFailureAndMessage(IllegalStateException.class, "hasNext", 1);
    }


    @Test
    public void hasNextFailConditional() {
        Folyam.fromIterable(new FailingIterable(10, 2, 10))
                .filter(v -> true)
                .test()
                .assertFailureAndMessage(IllegalStateException.class, "hasNext", 1);
    }


    @Test
    public void hasNextFailBackpressured() {
        Folyam.fromIterable(new FailingIterable(10, 2, 10))
                .test(2)
                .assertFailureAndMessage(IllegalStateException.class, "hasNext", 1);
    }


    @Test
    public void hasNextFailConditionalBackpressured() {
        Folyam.fromIterable(new FailingIterable(10, 2, 10))
                .filter(v -> true)
                .test(2)
                .assertFailureAndMessage(IllegalStateException.class, "hasNext", 1);
    }

    @Test
    public void nextFail() {
        Folyam.fromIterable(new FailingIterable(10, 10, 1))
                .test()
                .assertFailureAndMessage(IllegalStateException.class, "next");
    }

    @Test
    public void nextFailConditional() {
        Folyam.fromIterable(new FailingIterable(10, 10, 1))
                .filter(v -> true)
                .test()
                .assertFailureAndMessage(IllegalStateException.class, "next");
    }

    @Test
    public void nextFailBackpressured() {
        Folyam.fromIterable(new FailingIterable(10, 10, 1))
                .test(2)
                .assertFailureAndMessage(IllegalStateException.class, "next");
    }

    @Test
    public void nextFailConditionalBackpressured() {
        Folyam.fromIterable(new FailingIterable(10, 10, 1))
                .filter(v -> true)
                .test(2)
                .assertFailureAndMessage(IllegalStateException.class, "next");
    }
}
