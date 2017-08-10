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

public class FolyamScanTest {

    @Test
    public void standard() {
        TestHelper.assertResult(
                Folyam.range(1, 5)
                        .scan((a, b) -> a + b),
                1, 3, 6, 10, 15
        );
    }

    @Test
    public void standardHidden() {
        TestHelper.assertResult(
                Folyam.range(1, 5).hide()
                        .scan((a, b) -> a + b),
                1, 3, 6, 10, 15
        );
    }

    @Test
    public void error() {
        TestHelper.assertFailureComposed(-1,
                f -> f.scan((a, b) -> a + b), IOException.class);
    }

    @Test
    public void scannerCrash() {
        Folyam.range(1, 2)
                .scan((a, b) -> { throw new IOException(); })
                .test()
                .assertFailure(IOException.class, 1);
    }

    @Test
    public void scannerCrashConditional() {
        Folyam.range(1, 2)
                .scan((a, b) -> { throw new IOException(); })
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class, 1);
    }

    @Test
    public void checkBadSource() {
        TestHelper.checkBadSource(f -> f.scan((a, b) -> a + b));
    }
}
