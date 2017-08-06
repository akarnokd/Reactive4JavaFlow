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
import hu.akarnokd.reactive4javaflow.errors.CompositeThrowable;
import hu.akarnokd.reactive4javaflow.fused.FusedSubscription;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class FolyamConcatArrayTest {

    @Test
    public void standard() {
        TestHelper.assertResult(
                Folyam.concatArray(Folyam.just(1), Folyam.range(2, 2), Folyam.range(4, 3)),
                1, 2, 3, 4, 5, 6
        );
    }

    @Test
    public void standard2() {
        TestHelper.assertResult(
                Folyam.concatArrayDelayError(Folyam.just(1), Folyam.range(2, 2), Folyam.range(4, 3)),
                1, 2, 3, 4, 5, 6
        );
    }

    @Test
    public void errorNoDelay() {
        Folyam.concatArray(Folyam.error(new IOException()), Folyam.range(2, 2), Folyam.range(4, 3))
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void errorWithDelay() {
        Folyam.concatArrayDelayError(Folyam.error(new IOException()), Folyam.range(2, 2), Folyam.range(4, 3))
                .test()
                .assertFailure(IOException.class, 2, 3, 4, 5, 6);
    }

    @Test
    public void errorMany() {
        Folyam.concatArrayDelayError(
                Folyam.just(1),
                Folyam.error(new IOException("First")),
                Folyam.range(2, 2),
                Folyam.error(new IOException("Second")),
                Folyam.range(4, 3),
                Folyam.error(new IOException("Third"))
                )
        .test()
        .assertFailure(CompositeThrowable.class, 1, 2, 3, 4, 5, 6)
        .assertInnerErrors(errors -> {
            TestHelper.assertError(errors, 0, IOException.class, "First");
            TestHelper.assertError(errors, 1, IOException.class, "Second");
            TestHelper.assertError(errors, 2, IOException.class, "Third");
        });
    }

    @Test
    public void errorManyConditional() {
        Folyam.concatArrayDelayError(
                Folyam.just(1),
                Folyam.error(new IOException("First")),
                Folyam.range(2, 2),
                Folyam.error(new IOException("Second")),
                Folyam.range(4, 3),
                Folyam.error(new IOException("Third"))
        )
                .filter(v -> true)
                .test()
                .assertFailure(CompositeThrowable.class, 1, 2, 3, 4, 5, 6)
                .assertInnerErrors(errors -> {
                    TestHelper.assertError(errors, 0, IOException.class, "First");
                    TestHelper.assertError(errors, 1, IOException.class, "Second");
                    TestHelper.assertError(errors, 2, IOException.class, "Third");
                });
    }

    @Test
    public void nullSource() {
        Folyam.concatArray(Folyam.just(1), null)
                .test()
                .assertFailure(NullPointerException.class, 1);
    }

    @Test
    public void allEmpty() {
        Folyam.concatArray(Folyam.empty(), Folyam.empty())
                .test()
                .assertResult();
    }

    @Test
    public void concatWith() {
        Folyam<Integer> f = Folyam.just(1).concatWith(Folyam.just(2)).concatWith(Folyam.just(3));

        assertEquals(3, ((FolyamConcatArray<Integer>)f).sources.length);

        f.test().assertResult(1, 2, 3);
    }

    @Test
    public void cancel() {
        Folyam.concatArray(Folyam.empty(), Folyam.empty())
                .test(0, true, FusedSubscription.NONE)
                .assertEmpty();
    }
}
