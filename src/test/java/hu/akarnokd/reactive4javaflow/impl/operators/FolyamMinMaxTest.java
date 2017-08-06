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

import hu.akarnokd.reactive4javaflow.Folyam;
import org.junit.Test;

public class FolyamMinMaxTest {

    @Test
    public void min1() {
        Folyam.characters("abcde")
                .min(Integer::compare)
                .test()
                .assertResult((int)'a');
    }


    @Test
    public void min2() {
        Folyam.characters("edcba")
                .min(Integer::compare)
                .test()
                .assertResult((int)'a');
    }


    @Test
    public void max1() {
        Folyam.characters("abcde")
                .max(Integer::compare)
                .test()
                .assertResult((int)'e');
    }


    @Test
    public void max2() {
        Folyam.characters("edcba")
                .max(Integer::compare)
                .test()
                .assertResult((int)'e');
    }
}
