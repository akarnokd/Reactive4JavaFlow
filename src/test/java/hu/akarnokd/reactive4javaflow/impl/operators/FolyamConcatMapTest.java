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

public class FolyamConcatMapTest {

    @Test
    public void standard() {
        TestHelper.assertResult(
                Folyam.range(1, 5)
                    .concatMap(Folyam::just), 1, 2, 3, 4, 5);
    }

    @Test
    public void standardHidden() {
        TestHelper.assertResult(
                Folyam.range(1, 5).hide()
                        .concatMap(Folyam::just), 1, 2, 3, 4, 5);
    }

    @Test
    public void standardDelayError() {
        TestHelper.assertResult(
                Folyam.range(1, 5)
                        .concatMapDelayError(Folyam::just), 1, 2, 3, 4, 5);
    }

    @Test
    public void standardEmpty() {
        TestHelper.assertResult(
                Folyam.range(1, 5)
                        .concatMap(v -> Folyam.empty()));
    }

    @Test
    public void standardConditional() {
        TestHelper.assertResult(
                Folyam.range(1, 5)
                .concatMap(v -> Folyam.range(v, 2))
                .filter(v -> true),
            1, 2, 2, 3, 3, 4, 4, 5, 5, 6);
    }

    @Test
    public void standardConditionalDelayError() {
        TestHelper.assertResult(
                Folyam.range(1, 5)
                        .concatMapDelayError(v -> Folyam.range(v, 2))
                        .filter(v -> true),
                1, 2, 2, 3, 3, 4, 4, 5, 5, 6);
    }

    @Test
    public void standardDelayErrorEmpty() {
        TestHelper.assertResult(
                Folyam.range(1, 5)
                        .concatMapDelayError(v -> Folyam.empty()));
    }

    @Test
    public void error() {
        Folyam.error(new IOException())
                .concatMap(Folyam::just)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void innerError() {
        Folyam.just(1)
                .concatMap(v -> Folyam.error(new IOException()))
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void innerErrorHidden() {
        Folyam.just(1).hide()
                .concatMap(v -> Folyam.error(new IOException()))
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void innerErrorHidden2() {
        Folyam.just(1).hide()
                .concatMap(v -> Folyam.error(new IOException()).hide())
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void mapperThrows() {
        Folyam.range(1, 3)
                .concatMap(v -> { throw new IOException(); })
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void delayError() {
        Folyam.range(1, 3)
                .concatMapDelayError(v -> {
                    if (v == 2) {
                        return Folyam.error(new IOException());
                    }
                    return Folyam.just(v);
                })
                .test()
                .assertFailure(IOException.class, 1, 3);
    }

    @Test
    public void noDelayError() {
        Folyam.range(1, 3)
                .concatMap(v -> {
                    if (v == 2) {
                        return Folyam.error(new IOException());
                    }
                    return Folyam.just(v);
                })
                .test()
                .assertFailure(IOException.class, 1);
    }

    @Test
    public void publisher() {
        Folyam.concat(Folyam.fromArray(Folyam.range(1, 3), Folyam.range(4, 3)))
                .test()
                .assertResult(1, 2, 3, 4, 5, 6);
    }

    @Test
    public void publisher2() {
        Folyam.concat(Folyam.fromArray(Folyam.range(1, 3), Folyam.range(4, 3)), 1)
                .test()
                .assertResult(1, 2, 3, 4, 5, 6);
    }

    @Test
    public void publisher3() {
        Folyam.concatDelayError(Folyam.fromArray(Folyam.range(1, 3), Folyam.range(4, 3)))
                .test()
                .assertResult(1, 2, 3, 4, 5, 6);
    }

    @Test
    public void publisher4() {
        Folyam.concatDelayError(Folyam.fromArray(Folyam.range(1, 3), Folyam.range(4, 3)), 1)
                .test()
                .assertResult(1, 2, 3, 4, 5, 6);
    }
    @Test
    public void publisher5() {
        Folyam.concatDelayError(Folyam.fromArray(Folyam.range(1, 3), Folyam.error(new IOException()), Folyam.range(4, 3)))
                .test()
                .assertFailure(IOException.class, 1, 2, 3, 4, 5, 6);
    }

    @Test
    public void publisher6() {
        Folyam.concatDelayError(Folyam.fromArray(Folyam.range(1, 3), Folyam.error(new IOException()), Folyam.range(4, 3)), 1)
                .test()
                .assertFailure(IOException.class, 1, 2, 3, 4, 5, 6);
    }
}
