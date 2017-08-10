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

public class FolyamWithLatestFromTest {

    @Test
    public void standard() {
        TestHelper.assertResult(
                Folyam.range(1, 5).withLatestFrom(Folyam.just(10), Integer::sum)
                , 11, 12, 13, 14, 15
        );
    }


    @Test
    public void standardHidden() {
        TestHelper.assertResult(
                Folyam.range(1, 5)
                        .hide()
                        .withLatestFrom(Folyam.just(10), Integer::sum)
                , 11, 12, 13, 14, 15
        );
    }

    @Test
    public void standard2() {
        TestHelper.assertResult(
                Folyam.range(1, 5).withLatestFrom(Folyam.never(), Integer::sum)
        );
    }

    @Test
    public void standard2Hide() {
        TestHelper.assertResult(
                Folyam.range(1, 5).hide().withLatestFrom(Folyam.never(), Integer::sum)
        );
    }

    @Test
    public void standard3() {
        TestHelper.assertResult(
                Folyam.range(1, 5).withLatestFrom(Folyam.empty(), Integer::sum)
        );
    }

    @Test
    public void standard3HIde() {
        TestHelper.assertResult(
                Folyam.range(1, 5).hide().withLatestFrom(Folyam.empty(), Integer::sum)
        );
    }

    @Test
    public void mainError() {
        TestHelper.assertFailureComposed(-1,
                f -> f.withLatestFrom(Folyam.just(10), Integer::sum),
                IOException.class
        );
    }

    @Test
    public void otherError() {
        TestHelper.assertFailureComposed(5,
                f -> f.withLatestFrom(Folyam.<Integer>error(new IOException()), Integer::sum),
                IOException.class
        );
    }

    @Test
    public void combinerCrash() {
        TestHelper.assertFailureComposed(5,
                f -> f.withLatestFrom(Folyam.just(10), (a, b) -> { throw new IOException(); }),
        IOException.class);
    }

}
