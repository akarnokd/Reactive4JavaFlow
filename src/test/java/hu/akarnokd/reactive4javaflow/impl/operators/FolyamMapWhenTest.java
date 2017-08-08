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

public class FolyamMapWhenTest {

    @Test
    public void normal() {
        Folyam.range(1, 5)
                .mapWhen(Folyam::just)
                .test()
                .assertResult(1, 2, 3, 4, 5);
    }

    @Test
    public void normalConditional() {
        Folyam.range(1, 5)
                .mapWhen(Folyam::just)
                .filter(v -> true)
                .test()
                .assertResult(1, 2, 3, 4, 5);
    }

    @Test
    public void standard() {
        TestHelper.assertResult(
                Folyam.range(1, 5)
                .mapWhen(Folyam::just),
                1, 2, 3, 4, 5);
    }


    @Test
    public void standardHidden() {
        TestHelper.assertResult(
                Folyam.range(1, 5)
                        .mapWhen(item -> Folyam.just(item).hide()),
                1, 2, 3, 4, 5);
    }

    @Test
    public void standard2() {
        TestHelper.assertResult(
                Folyam.range(1, 5)
                        .mapWhen(Folyam::just, 1),
                1, 2, 3, 4, 5);
    }

    @Test
    public void standard3() {
        TestHelper.assertResult(
                Folyam.range(1, 5)
                        .mapWhen(Folyam::just, (a, b) -> a + b),
                2, 4, 6, 8, 10);
    }

    @Test
    public void standard4() {
        TestHelper.assertResult(
                Folyam.range(1, 5)
                        .mapWhen(Folyam::just, (a, b) -> a + b, 1),
                2, 4, 6, 8, 10);
    }

    @Test
    public void standard5() {
        TestHelper.assertResult(
                Folyam.range(1, 5)
                        .mapWhenDelayError(Folyam::just, (a, b) -> a + b, 128),
                2, 4, 6, 8, 10);
    }

    @Test
    public void standard6() {
        TestHelper.assertResult(
                Folyam.range(1, 5)
                        .mapWhenDelayError(Folyam::just, (a, b) -> a + b, 1),
                2, 4, 6, 8, 10);
    }

    @Test
    public void standard7() {
        TestHelper.assertResult(
                Folyam.range(1, 5)
                        .mapWhen(v -> Folyam.range(v, 2)),
                1, 2, 3, 4, 5);
    }

    @Test
    public void emptyOuter() {
        TestHelper.assertResult(
                Folyam.empty()
                        .mapWhen(Folyam::just)
                );
    }


    @Test
    public void emptyInner() {
        TestHelper.assertResult(
                Folyam.range(1, 5)
                        .mapWhen(v -> Folyam.empty())
                );
    }


    @Test
    public void emptyInnerHidden() {
        TestHelper.assertResult(
                Folyam.range(1, 5)
                        .mapWhen(v -> Folyam.empty().hide())
        );
    }

    @Test
    public void mainError() {
        Folyam.error(new IOException())
                .mapWhen(Folyam::just)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void mainErrorConditional() {
        Folyam.error(new IOException())
                .mapWhen(Folyam::just)
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void innerError() {
        Folyam.just(1)
                .mapWhen(v -> Folyam.error(new IOException()))
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void innerErrorConditional() {
        Folyam.just(1)
                .mapWhen(v -> Folyam.error(new IOException()))
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void innerErrorHidden() {
        Folyam.just(1)
                .mapWhen(v -> Folyam.error(new IOException()).hide())
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void innerErrorConditionalHidden() {
        Folyam.just(1)
                .mapWhen(v -> Folyam.error(new IOException()).hide())
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }


    @Test
    public void innerErrorDelayed() {
        Folyam.just(1)
                .mapWhenDelayError(v -> Folyam.error(new IOException()))
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void innerErrorConditionalDelayed() {
        Folyam.just(1)
                .mapWhenDelayError(v -> Folyam.error(new IOException()))
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void someInnerErrorDelayed() {
        Folyam.range(1, 3)
                .mapWhenDelayError(v -> {
                    if (v == 2) {
                        return Folyam.error(new IOException());
                    }
                    return Folyam.just(v);
                })
                .test()
                .assertFailure(IOException.class, 1, 3);
    }

    @Test
    public void someInnerErrorDelayedConditional() {
        Folyam.range(1, 3)
                .mapWhenDelayError(v -> {
                    if (v == 2) {
                        return Folyam.error(new IOException());
                    }
                    return Folyam.just(v);
                })
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class, 1, 3);
    }

    @Test
    public void mapperCrash() {
        Folyam.range(1, 3)
                .mapWhen(v -> { throw new IOException(); })
                .test()
                .assertFailure(IOException.class);
    }


    @Test
    public void mapperCrashConditional() {
        Folyam.range(1, 3)
                .mapWhen(v -> { throw new IOException(); })
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void mapperCrashDelayError() {
        Folyam.range(1, 3)
                .mapWhenDelayError(v -> { throw new IOException(); })
                .test()
                .assertFailure(IOException.class);
    }


    @Test
    public void mapperCrashConditionalDelayError() {
        Folyam.range(1, 3)
                .mapWhenDelayError(v -> { throw new IOException(); })
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void combinerCrash() {
        Folyam.range(1, 3)
                .mapWhen(Folyam::just, (a, b) -> { throw new IOException(); })
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void combinerCrashConditional() {
        Folyam.range(1, 3)
                .mapWhen(Folyam::just, (a, b) -> { throw new IOException(); })
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void combinerCrashDelayError() {
        Folyam.range(1, 3)
                .mapWhenDelayError(Folyam::just, (a, b) -> { throw new IOException(); })
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void combinerCrashConditionalDelayError() {
        Folyam.range(1, 3)
                .mapWhenDelayError(Folyam::just, (a, b) -> { throw new IOException(); })
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void normalLong() {
        Folyam.range(1, 1000)
                .mapWhen(Folyam::just)
                .test()
                .assertValueCount(1000)
                .assertNoErrors()
                .assertComplete();
    }

    @Test
    public void normalLongConditional() {
        Folyam.range(1, 1000)
                .mapWhen(Folyam::just)
                .filter(v -> true)
                .test()
                .assertValueCount(1000)
                .assertNoErrors()
                .assertComplete();
    }

    @Test
    public void combinerCrashDelayErrorHidden() {
        Folyam.range(1, 3)
                .mapWhenDelayError(v -> Folyam.just(1).hide(), (a, b) -> { throw new IOException(); })
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void combinerCrashConditionalDelayErrorHidden() {
        Folyam.range(1, 3)
                .mapWhenDelayError(v -> Folyam.just(1).hide(), (a, b) -> { throw new IOException(); })
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void normalLongHidden() {
        Folyam.range(1, 1000)
                .mapWhen(v -> Folyam.just(v).hide())
                .test()
                .assertValueCount(1000)
                .assertNoErrors()
                .assertComplete();
    }

    @Test
    public void normalLongConditionalHidden() {
        Folyam.range(1, 1000)
                .mapWhen(v -> Folyam.just(v).hide())
                .filter(v -> true)
                .test()
                .assertValueCount(1000)
                .assertNoErrors()
                .assertComplete();
    }

    @Test
    public void emptyLongHidden() {
        Folyam.range(1, 1000)
                .mapWhen(v -> Folyam.empty().hide())
                .test()
                .assertResult();
    }

    @Test
    public void emptyLongConditionalHidden() {
        Folyam.range(1, 1000)
                .mapWhen(v -> Folyam.empty().hide())
                .filter(v -> true)
                .test()
                .assertResult();
    }
}
