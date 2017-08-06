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

public class FolyamCharactersTest {

    @Test
    public void standard() {
        TestHelper.assertResult(Folyam.characters("abcde"),
                (int)'a', (int)'b', (int)'c', (int)'d', (int)'e'
                );
    }


    @Test
    public void standard2() {
        TestHelper.assertResult(Folyam.characters("abcde", 1, 4),
                (int)'b', (int)'c', (int)'d'
        );
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void badIndex() {
        Folyam.characters("a", 1, 2);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void badIndex1() {
        Folyam.characters("a", -1, 2);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void badIndex2() {
        Folyam.characters("a", 0, -2);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void badIndex3() {
        Folyam.characters("a", 0, 3);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void badIndex4() {
        Folyam.characters("a", 2, 3);
    }
}
