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

public class FolyamRangeLongTest {

    @Test
    public void standard0() {
        TestHelper.assertResult(Folyam.rangeLong(1, 0));
    }

    @Test
    public void standard1() {
        TestHelper.assertResult(Folyam.rangeLong(1, 1), 1L);
    }

    @Test
    public void standard2() {
        TestHelper.assertResult(Folyam.rangeLong(1, 2), 1L, 2L);
    }

    @Test
    public void standard5() {
        TestHelper.assertResult(Folyam.rangeLong(1, 5), 1L, 2L, 3L, 4L, 5L);
    }

    @Test
    public void noOverflow() {
        Folyam.rangeLong(Long.MAX_VALUE, 1);
        Folyam.rangeLong(Long.MAX_VALUE - 1, 2);
        Folyam.rangeLong(0, Long.MAX_VALUE);
        Folyam.rangeLong(Long.MIN_VALUE, Long.MAX_VALUE);
        Folyam.rangeLong(1, Long.MAX_VALUE);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void overflow1() {
        Folyam.rangeLong(Long.MAX_VALUE, 2);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void overflow2() {
        Folyam.rangeLong(Long.MAX_VALUE, Long.MAX_VALUE);
    }
}
