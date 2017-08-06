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

public class FolyamArrayTest {

    @Test
    public void standard0() {
        TestHelper.assertResult(Folyam.fromArray());
    }

    @Test
    public void standard1() {
        TestHelper.assertResult(Folyam.fromArray(1), 1);
    }

    @Test
    public void standard2() {
        TestHelper.assertResult(Folyam.fromArray(1, 2), 1, 2);
    }

    @Test
    public void standard5() {
        TestHelper.assertResult(Folyam.fromArray(1, 2, 3, 4, 5), 1, 2, 3, 4, 5);
    }

    @Test
    public void nullItem() {
        Folyam.fromArray(1, (Integer)null)
                .test()
                .assertFailure(NullPointerException.class, 1);
    }

    @Test
    public void nullItemBackpressured() {
        Folyam.fromArray(1, (Integer)null)
                .test(2)
                .assertFailure(NullPointerException.class, 1);
    }

    @Test
    public void range() {
        Folyam.fromArrayRange(1, 4, 1, 2, 3, 4, 5)
                .test()
                .assertResult(2, 3, 4);
    }

    @Test
    public void rangeEmpty() {
        Folyam.fromArrayRange(5, 5, 1, 2, 3, 4, 5)
                .test()
                .assertResult();
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void rangeInvalid1() {
        Folyam.fromArrayRange(-1, 1, 1, 2, 3, 4, 5);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void rangeInvalid2() {
        Folyam.fromArrayRange(0, 6, 1, 2, 3, 4, 5);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void rangeInvalid3() {
        Folyam.fromArrayRange(0, -1, 1, 2, 3, 4, 5);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void rangeInvalid4() {
        Folyam.fromArrayRange(6, 1, 1, 2, 3, 4, 5);
    }
}
