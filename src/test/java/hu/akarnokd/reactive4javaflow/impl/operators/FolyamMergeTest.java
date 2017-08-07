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

public class FolyamMergeTest {

    @Test
    public void standard() {
        TestHelper.assertResult(
                Folyam.mergeArray(Folyam.range(1, 3),
                        Folyam.range(4, 3)),
                1, 2, 3, 4, 5, 6);
    }

    @Test
    public void standard2() {
        TestHelper.assertResult(
                Folyam.merge(List.of(Folyam.range(1, 3),
                        Folyam.range(4, 3))),
                1, 2, 3, 4, 5, 6);
    }

    @Test
    public void standard3() {
        TestHelper.assertResult(
                Folyam.mergeArray()
                );
    }

    @Test
    public void standard4() {
        TestHelper.assertResult(
                Folyam.merge(List.of())
        );
    }

    @Test
    public void mergeWithMany() {
        Folyam<Integer> f = Folyam.empty();
        for (int i = 0; i < 1000; i++) {
            f = f.mergeWith(Folyam.empty());
        }
        f = f.mergeWith(Folyam.range(1, 5));

        f.test().assertResult(1, 2, 3, 4, 5);

        f.filter(v -> true).test().assertResult(1, 2, 3, 4, 5);
    }

    @Test
    public void mergeWithDelayErrorMany() {
        Folyam<Integer> f = Folyam.empty();
        for (int i = 0; i < 1000; i++) {
            f = f.mergeWithDelayError(Folyam.empty());
        }
        f = f.mergeWithDelayError(Folyam.range(1, 5));

        f.test().assertResult(1, 2, 3, 4, 5);

        f.filter(v -> true).test().assertResult(1, 2, 3, 4, 5);
    }

    @Test
    public void mergeIterableCrash() {
        Folyam.merge(() -> { throw new IllegalArgumentException(); })
                .test()
                .assertFailure(IllegalArgumentException.class);
    }

    @Test
    public void delayError() {
            Folyam.mergeDelayError(List.of(Folyam.error(new IOException()), Folyam.range(1, 5)))
            .test()
            .assertFailure(IOException.class, 1, 2, 3, 4, 5);
    }

    @Test
    public void delayError2() {
            Folyam.mergeDelayError(List.of(Folyam.error(new IOException()), Folyam.range(1, 5)), 1)
                    .test()
                    .assertFailure(IOException.class, 1, 2, 3, 4, 5);
    }

    @Test
    public void arrayDelayError() {
        Folyam.mergeArrayDelayError(Folyam.error(new IOException()), Folyam.range(1, 5))
                .test()
                .assertFailure(IOException.class, 1, 2, 3, 4, 5);
    }
}
