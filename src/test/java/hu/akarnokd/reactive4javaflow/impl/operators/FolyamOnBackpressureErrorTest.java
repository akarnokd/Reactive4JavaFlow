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

import hu.akarnokd.reactive4javaflow.Folyam;
import org.junit.Test;

import java.io.IOException;

public class FolyamOnBackpressureErrorTest {

    @Test
    public void normal() {
        Folyam.range(1, 5)
                .onBackpressureError()
                .test()
                .assertResult(1, 2, 3, 4, 5);
    }


    @Test
    public void normalConditional() {
        Folyam.range(1, 5)
                .onBackpressureError()
                .filter(v -> true)
                .test()
                .assertResult(1, 2, 3, 4, 5);
    }

    @Test
    public void normalBackpressured() {
        Folyam.range(1, 5)
                .onBackpressureError()
                .test(3)
                .assertFailure(IllegalStateException.class, 1, 2, 3);
    }

    @Test
    public void normalBackpressuredConditional() {
        Folyam.range(1, 5)
                .onBackpressureError()
                .filter(v -> true)
                .test(3)
                .assertFailure(IllegalStateException.class, 1, 2, 3);
    }

    @Test
    public void error() {
        Folyam.error(new IOException())
                .onBackpressureError()
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void errorConditional() {
        Folyam.error(new IOException())
                .onBackpressureError()
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }
}
