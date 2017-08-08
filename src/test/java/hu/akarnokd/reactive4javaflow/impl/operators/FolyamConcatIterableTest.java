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

public class FolyamConcatIterableTest {

    @Test
    public void standard() {
        TestHelper.assertResult(Folyam.concat(List.of(Folyam.range(1, 3), Folyam.range(4, 3)))
        , 1, 2, 3, 4, 5, 6);
    }


    @Test
    public void standard2() {
        TestHelper.assertResult(Folyam.concatDelayError(List.of(Folyam.range(1, 3), Folyam.range(4, 3)))
                , 1, 2, 3, 4, 5, 6);
    }

    @Test
    public void error() {
        Folyam.concat(List.of(Folyam.error(new IOException()), Folyam.just(1)))
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void error2() {
        Folyam.concat(List.of(Folyam.just(1), Folyam.error(new IOException())))
                .test()
                .assertFailure(IOException.class, 1);
    }


    @Test
    public void errorConditional() {
        Folyam.concat(List.of(Folyam.error(new IOException()), Folyam.just(1)))
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void error2Conditional() {
        Folyam.concat(List.of(Folyam.just(1), Folyam.error(new IOException())))
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class, 1);
    }

    @Test
    public void errorDelayError() {
        Folyam.concatDelayError(List.of(Folyam.error(new IOException()), Folyam.just(1)))
                .test()
                .assertFailure(IOException.class, 1);
    }

    @Test
    public void error2DelayError() {
        Folyam.concatDelayError(List.of(Folyam.just(1), Folyam.error(new IOException())))
                .test()
                .assertFailure(IOException.class, 1);
    }


    @Test
    public void errorConditionalDelayError() {
        Folyam.concatDelayError(List.of(Folyam.error(new IOException()), Folyam.just(1)))
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class, 1);
    }

    @Test
    public void error2ConditionalDelayError() {
        Folyam.concatDelayError(List.of(Folyam.just(1), Folyam.error(new IOException())))
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class, 1);
    }

    @Test
    public void iteratorCrash() {
        Folyam.concat(new FailingMappedIterable<>(1, 10, 10, v -> Folyam.just(1)))
                .test()
                .assertFailure(IllegalStateException.class);
    }

    @Test
    public void hasNextCrash() {
        Folyam.concat(new FailingMappedIterable<>(10, 1, 10, v -> Folyam.just(1)))
                .test()
                .assertFailure(IllegalStateException.class);
    }

    @Test
    public void nextCrash() {
        Folyam.concat(new FailingMappedIterable<>(10, 10, 1, v -> Folyam.just(1)))
                .test()
                .assertFailure(IllegalStateException.class);
    }


    @Test
    public void hasNextCrashConditonal() {
        Folyam.concat(new FailingMappedIterable<>(10, 1, 10, v -> Folyam.just(1)))
                .filter(v -> true)
                .test()
                .assertFailure(IllegalStateException.class);
    }

    @Test
    public void nextCrashConditonal() {
        Folyam.concat(new FailingMappedIterable<>(10, 10, 1, v -> Folyam.just(1)))
                .filter(v -> true)
                .test()
                .assertFailure(IllegalStateException.class);
    }

    @Test
    public void cancelUpFront() {
        Folyam.concat(List.of(Folyam.just(1), Folyam.just(2)))
                .test(0, true, 0)
                .assertEmpty();
    }


    @Test
    public void cancelUpFrontConditional() {
        Folyam.concat(List.of(Folyam.just(1), Folyam.just(2)))
                .filter(v -> true)
                .test(0, true, 0)
                .assertEmpty();
    }
}
