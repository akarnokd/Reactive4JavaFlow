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

public class FolyamFilterWhenTest {

    @Test
    public void standard() {
        TestHelper.assertResult(
                Folyam.range(1, 5)
                .filterWhen(v -> Folyam.just(true))
                , 1, 2, 3, 4, 5
        );
    }


    @Test
    public void standardHide() {
        TestHelper.assertResult(
                Folyam.range(1, 5)
                        .filterWhen(v -> Folyam.just(true).hide())
                , 1, 2, 3, 4, 5
        );
    }


    @Test
    public void standard1() {
        TestHelper.assertResult(
                Folyam.range(1, 5)
                        .filterWhen(v -> Folyam.just(true), 1)
                , 1, 2, 3, 4, 5
        );
    }


    @Test
    public void standard1Hide() {
        TestHelper.assertResult(
                Folyam.range(1, 5)
                        .filterWhen(v -> Folyam.just(true).hide(), 1)
                , 1, 2, 3, 4, 5
        );
    }

    @Test
    public void standard2() {
        TestHelper.assertResult(
                Folyam.range(1, 5)
                        .filterWhen(v -> Folyam.just(false))
        );
    }


    @Test
    public void standard2Hide() {
        TestHelper.assertResult(
                Folyam.range(1, 5)
                        .filterWhen(v -> Folyam.just(false).hide())
        );
    }

    @Test
    public void standard3() {
        TestHelper.assertResult(
                Folyam.range(1, 5)
                        .filterWhen(v -> Folyam.empty())
        );
    }


    @Test
    public void standard3Hide() {
        TestHelper.assertResult(
                Folyam.range(1, 5)
                        .filterWhen(v -> Folyam.<Boolean>empty().hide())
        );
    }

    @Test
    public void error() {
        Folyam.error(new IOException())
                .filterWhen(v -> Folyam.just(true))
                .test()
                .assertFailure(IOException.class);
    }


    @Test
    public void errorConditional() {
        Folyam.error(new IOException())
                .filterWhen(v -> Folyam.just(true))
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void filterCrash() {
        Folyam.just(1)
                .filterWhen(v -> { throw new IOException(); })
                .test()
                .assertFailure(IOException.class);
    }


    @Test
    public void filterCrashConditional() {
        Folyam.just(1)
                .filterWhen(v -> { throw new IOException(); })
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }


    @Test
    public void filterCrashDelayError() {
        Folyam.just(1)
                .filterWhenDelayError(v -> { throw new IOException(); })
                .test()
                .assertFailure(IOException.class);
    }


    @Test
    public void filterCrashDelayErrorConditional() {
        Folyam.just(1)
                .filterWhenDelayError(v -> { throw new IOException(); })
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void filterError() {
        Folyam.range(1, 5)
                .filterWhen(v -> Folyam.error(new IOException()))
                .test()
                .assertFailure(IOException.class);
    }


    @Test
    public void filterErrorHidden() {
        Folyam.range(1, 5)
                .filterWhen(v -> Folyam.<Boolean>error(new IOException()).hide())
                .test()
                .assertFailure(IOException.class);
    }


    @Test
    public void filterErrorConditional() {
        Folyam.range(1, 5)
                .filterWhen(v -> Folyam.error(new IOException()))
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }


    @Test
    public void filterErrorHiddenConditional() {
        Folyam.range(1, 5)
                .filterWhen(v -> Folyam.<Boolean>error(new IOException()).hide())
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void filterDelayError() {
        Folyam.range(1, 3)
                .filterWhenDelayError(v -> {
                    if (v == 2) {
                        return Folyam.error(new IOException());
                    }
                    return Folyam.just(true);
                })
                .test()
                .assertFailure(IOException.class, 1, 3);
    }

    @Test
    public void filterDelayErrorConditional() {
        Folyam.range(1, 3)
                .filterWhenDelayError(v -> {
                    if (v == 2) {
                        return Folyam.error(new IOException());
                    }
                    return Folyam.just(true);
                })
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class, 1, 3);
    }

    @Test
    public void longEmpty() {
        Folyam.range(1, 1000)
                .filterWhen(v -> Folyam.empty())
                .test()
                .assertResult();
    }


    @Test
    public void longEmptyHidden() {
        Folyam.range(1, 1000)
                .filterWhen(v -> Folyam.<Boolean>empty().hide())
                .test()
                .assertResult();
    }


    @Test
    public void longEmptyConditional() {
        Folyam.range(1, 1000)
                .filterWhen(v -> Folyam.empty())
                .filter(v -> true)
                .test()
                .assertResult();
    }


    @Test
    public void longEmptyHiddenConditional() {
        Folyam.range(1, 1000)
                .filterWhen(v -> Folyam.<Boolean>empty().hide())
                .filter(v -> true)
                .test()
                .assertResult();
    }
}
