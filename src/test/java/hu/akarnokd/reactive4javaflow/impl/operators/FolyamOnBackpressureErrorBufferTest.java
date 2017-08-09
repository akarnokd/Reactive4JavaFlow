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

public class FolyamOnBackpressureErrorBufferTest {

    @Test
    public void standard() {
        TestHelper.assertResult(
                Folyam.range(1, 5)
                .onBackpressureError(16)
                , 1, 2, 3, 4, 5
        );
    }

    @Test
    public void bounded() {
        Folyam.range(1, 5)
                .onBackpressureError(2)
                .test(1)
                .assertValues(1)
                .requestMore(2)
                .assertFailure(IllegalStateException.class, 1, 2, 3);
    }


    @Test
    public void bounded1() {
        Folyam.range(1, 5)
                .onBackpressureError(1)
                .test(0)
                .assertValues()
                .requestMore(1)
                .assertFailure(IllegalStateException.class, 1);
    }
}
