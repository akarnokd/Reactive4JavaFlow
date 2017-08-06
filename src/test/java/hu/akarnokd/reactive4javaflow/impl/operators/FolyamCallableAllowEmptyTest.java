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

public class FolyamCallableAllowEmptyTest {

    @Test
    public void standard() {
        TestHelper.assertResult(Folyam.fromCallableAllowEmpty(() -> 1), 1);
    }

    @Test
    public void standard2() {
        TestHelper.assertResult(Folyam.fromCallableAllowEmpty(() -> null));
    }


    @Test
    public void nullReturn() {
        Folyam.fromCallableAllowEmpty(() -> null)
                .test()
                .assertResult();
    }

    @Test
    public void error() {
        Folyam.fromCallableAllowEmpty(() -> { throw new IOException(); })
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void cancelUpFront() {
        Folyam.fromCallableAllowEmpty(() -> 1)
                .test(1, true, 0)
                .assertEmpty();
    }
}
