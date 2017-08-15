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

public class EsetlegSwitchIfEmptyTest {

    @Test
    public void standard() {
        TestHelper.assertResult(
                Esetleg.just(1).switchIfEmpty(Esetleg.just(2)),
                1);
    }

    @Test
    public void standardHidden() {
        TestHelper.assertResult(
                Esetleg.just(1).hide()
                        .switchIfEmpty(Esetleg.just(2)),
                1);
    }

    @Test
    public void standard2() {
        TestHelper.assertResult(
                Esetleg.empty().switchIfEmpty(Esetleg.just(2)),
                2);
    }

    @Test
    public void standard3() {
        TestHelper.assertResult(
                Esetleg.just(1).defaultIfEmpty(6),
                1);
    }

    @Test
    public void standard4() {
        TestHelper.assertResult(
                Esetleg.empty().defaultIfEmpty(6),
                6);
    }

    @Test
    public void error() {
        Esetleg.error(new IOException())
                .switchIfEmpty(Esetleg.just(2))
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void errorConditional() {
        Esetleg.error(new IOException())
                .switchIfEmpty(Esetleg.just(2))
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }


    @Test
    public void errorOther() {
        Esetleg.empty()
                .switchIfEmpty(Esetleg.error(new IOException()))
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void errorOtherConditional() {
        Esetleg.empty()
                .switchIfEmpty(Esetleg.error(new IOException()))
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }
}
