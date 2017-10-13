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

public class FolyamAndThenTest {

    @Test
    public void standard() {
        TestHelper.assertResult(Folyam.just(1).andThen(Esetleg.just(2)), 2);
    }

    @Test
    public void standard2() {
        TestHelper.assertResult(Folyam.just(1).andThen(Folyam.range(2, 3)), 2, 3, 4);
    }

    @Test
    public void mainError() {
        Folyam.error(new IOException())
                .andThen(Esetleg.just(2))
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void mainError2() {
        Folyam.error(new IOException())
                .andThen(Folyam.range(2, 3))
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void nextError() {
        Folyam.just(1)
                .andThen(Esetleg.error(new IOException()))
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void nextError2() {
        Folyam.just(1)
                .andThen(Folyam.error(new IOException()))
                .test()
                .assertFailure(IOException.class);
    }
}
