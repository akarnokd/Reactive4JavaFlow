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

import static org.junit.Assert.assertEquals;

public class FolyamCacheTest {

    @Test
    public void standard() {
        Folyam<Integer> f = Folyam.range(1, 5).cache();
        TestHelper.assertResult(f, 1, 2, 3, 4, 5);
    }

    @Test
    public void consumeOnce() {
        int[] counter = { 0 };

        Folyam<Integer> f = Folyam.range(1, 5)
                .doOnSubscribe(s -> ++counter[0])
                .cache();

        f.test().assertResult(1, 2, 3, 4, 5);
        f.test().assertResult(1, 2, 3, 4, 5);
        f.test().assertResult(1, 2, 3, 4, 5);

        assertEquals(1, counter[0]);
    }
}
