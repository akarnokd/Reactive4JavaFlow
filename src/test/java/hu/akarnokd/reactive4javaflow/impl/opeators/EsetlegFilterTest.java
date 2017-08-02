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

package hu.akarnokd.reactive4javaflow.impl.opeators;

import hu.akarnokd.reactive4javaflow.*;
import org.junit.Test;

import java.io.IOException;

public class EsetlegFilterTest {

    @Test
    public void standard0() {
        TestHelper.assertResult(Esetleg.just(1).filter(v -> (v & 1) == 0));
    }

    @Test
    public void standard1() {
        TestHelper.assertResult(Esetleg.just(2).filter(v -> (v & 1) == 0), 2);
    }

    @Test
    public void standard2() {
        TestHelper.assertResult(Esetleg.just(1).filter(v -> true), 1);
    }

    @Test
    public void standard3() {
        TestHelper.assertResult(Esetleg.just(1).filter(v -> false));
    }

    @Test
    public void error() {
        Esetleg.error(new IOException())
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void errorConditional() {
        Esetleg.error(new IOException())
                .filter(v -> true)
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void predicateCrash() {
        Esetleg.just(1)
                .filter(v -> { throw new IOException(); })
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void predicateCrashConditional() {
        Esetleg.just(1)
                .filter(v -> { throw new IOException(); })
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }
}
