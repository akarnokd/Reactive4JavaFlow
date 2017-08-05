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

public class FolyamSumTest {

    @Test
    public void intStandard() {
        TestHelper.assertResult(
                Folyam.range(1, 5).sumInt(v -> v),
                15);

        TestHelper.assertResult(
                Folyam.just(1).sumInt(v -> v),
                1);

        TestHelper.assertResult(
                Folyam.<Integer>empty().sumInt(v -> v)
                );

        Folyam.error(new IOException())
                .sumInt(v -> 0)
                .test()
                .assertFailure(IOException.class);

        Folyam.range(1, 5)
                .sumInt(v -> { throw new IOException(); })
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void longStandard() {
        TestHelper.assertResult(
                Folyam.range(1, 5).sumLong(v -> v),
                15L);

        TestHelper.assertResult(
                Folyam.just(1).sumLong(v -> v),
                1L);

        TestHelper.assertResult(
                Folyam.<Integer>empty().sumLong(v -> v)
        );

        Folyam.error(new IOException())
                .sumLong(v -> 0L)
                .test()
                .assertFailure(IOException.class);

        Folyam.range(1, 5)
                .sumLong(v -> { throw new IOException(); })
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void floatStandard() {
        TestHelper.assertResult(
                Folyam.range(1, 6).sumFloat(v -> v + 0.5f),
                24f);

        TestHelper.assertResult(
                Folyam.just(1).sumFloat(v -> v + 0.5f),
                1.5f);

        TestHelper.assertResult(
                Folyam.<Integer>empty().sumFloat(v -> v + 0.5f)
        );

        Folyam.error(new IOException())
                .sumFloat(v -> 0.5f)
                .test()
                .assertFailure(IOException.class);

        Folyam.range(1, 5)
                .sumFloat(v -> { throw new IOException(); })
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void doubleStandard() {
        TestHelper.assertResult(
                Folyam.range(1, 6).sumDouble(v -> v + 0.5d),
                24d);

        TestHelper.assertResult(
                Folyam.just(1).sumDouble(v -> v + 0.5d),
                1.5d);

        TestHelper.assertResult(
                Folyam.<Integer>empty().sumDouble(v -> v + 0.5d)
        );

        Folyam.error(new IOException())
                .sumDouble(v -> 0.5d)
                .test()
                .assertFailure(IOException.class);

        Folyam.range(1, 5)
                .sumDouble(v -> { throw new IOException(); })
                .test()
                .assertFailure(IOException.class);
    }

}
