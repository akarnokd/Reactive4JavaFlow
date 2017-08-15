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

public class EsetlegRepeatTest {

    @Test
    public void normal() {
        Esetleg.just(1).repeat(2)
                .test()
                .assertResult(1, 1);
    }


    @Test
    public void normalConditional() {
        Esetleg.just(1).repeat(2)
                .filter(v -> true)
                .test()
                .assertResult(1, 1);
    }

    @Test
    public void error() {
        Esetleg.error(new IOException())
                .repeat(2)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void errorConditional() {
        Esetleg.error(new IOException())
                .repeat(2)
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void condition() {
        int[] counter = { 2 };

        Esetleg.just(1)
                .repeat(() -> counter[0]-- != 0)
                .test()
                .assertResult(1, 1, 1);
    }

    @Test
    public void conditionConditional() {
        int[] counter = { 2 };

        Esetleg.just(1)
                .repeat(() -> counter[0]-- != 0)
                .filter(v -> true)
                .test()
                .assertResult(1, 1, 1);
    }

    @Test
    public void countAndCondition() {
        int[] counter = { 2 };

        Esetleg.just(1)
                .repeat(2, () -> counter[0]-- != 0)
                .test()
                .assertResult(1, 1);
    }

    @Test
    public void conditionCrash() {
        Esetleg.just(1)
                .repeat(() -> { throw new IOException(); })
                .test()
                .assertFailure(IOException.class, 1);
    }

    @Test(timeout = 1000)
    public void cancelled() {
        Esetleg.just(1)
                .repeat()
                .test(1, true, 0)
                .assertEmpty();
    }
}
