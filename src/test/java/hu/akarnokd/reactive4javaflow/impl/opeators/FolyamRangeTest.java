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

public class FolyamRangeTest {

    @Test
    public void standard0() {
        TestHelper.assertResult(Folyam.range(1, 0));
    }

    @Test
    public void standard1() {
        TestHelper.assertResult(Folyam.range(1, 1), 1);
    }

    @Test
    public void standard2() {
        TestHelper.assertResult(Folyam.range(1, 2), 1, 2);
    }

    @Test
    public void standard5() {
        TestHelper.assertResult(Folyam.range(1, 5), 1, 2, 3, 4, 5);
    }

    @Test
    public void noOverflow() {
        Folyam.range(Integer.MAX_VALUE, 1);
        Folyam.range(Integer.MAX_VALUE - 1, 2);
        Folyam.range(0, Integer.MAX_VALUE);
        Folyam.range(Integer.MIN_VALUE, Integer.MAX_VALUE);
        Folyam.range(1, Integer.MAX_VALUE);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void overflow1() {
        Folyam.range(Integer.MAX_VALUE, 2);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void overflow2() {
        Folyam.range(Integer.MAX_VALUE, Integer.MAX_VALUE);
    }
}
