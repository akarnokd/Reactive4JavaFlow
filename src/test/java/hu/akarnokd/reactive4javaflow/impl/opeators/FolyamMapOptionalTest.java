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
import java.util.Optional;

public class FolyamMapOptionalTest {

    @Test
    public void standard1() {
        TestHelper.assertResult(
                Folyam.range(1, 10)
                        .mapOptional(v -> (v & 1) == 0 ? Optional.of(v) : Optional.empty()),
                2, 4, 6, 8, 10);
    }

    @Test
    public void standard2() {
        TestHelper.assertResult(
                Folyam.range(1, 5).mapOptional(Optional::of),
                1, 2, 3, 4, 5);
    }

    @Test
    public void standard3() {
        TestHelper.assertResult(
                Folyam.range(1, 5).mapOptional(v -> Optional.empty()));
    }

    @Test
    public void error() {
        Folyam.error(new IOException())
                .mapOptional(Optional::of)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void errorConditional() {
        Folyam.error(new IOException())
                .mapOptional(Optional::of)
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void predicateCrash() {
        Folyam.just(1)
                .mapOptional(v -> { throw new IOException(); })
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void predicateCrashConditional() {
        Folyam.just(1)
                .mapOptional(v -> { throw new IOException(); })
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void donePath() {
        TestHelper.folyamDonePath(f -> f.mapOptional(Optional::of), 1);
    }

    @Test
    public void donePathConditional() {
        TestHelper.folyamDonePath(f -> f.mapOptional(Optional::of).filter(v -> true), 1);
    }
}
