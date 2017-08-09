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

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FolyamOnBackpressureDropTest {

    @Test
    public void normal() {
        Folyam.range(1, 5).onBackpressureDrop()
                .test()
                .assertResult(1, 2, 3, 4, 5);
    }


    @Test
    public void normal1() {
        List<Integer> list = new ArrayList<>();
        Folyam.range(1, 5).onBackpressureDrop(list::add)
                .test()
                .assertResult(1, 2, 3, 4, 5);

        assertTrue(list.toString(), list.isEmpty());
    }

    @Test
    public void normalBackpressured() {
        Folyam.range(1, 5).onBackpressureDrop()
                .test(3)
                .assertResult(1, 2, 3);
    }

    @Test
    public void normal1Backpressured() {
        List<Integer> list = new ArrayList<>();
        Folyam.range(1, 5).onBackpressureDrop(list::add)
                .test(3)
                .assertResult(1, 2, 3);

        assertEquals(Arrays.asList(4, 5), list);
    }

    @Test
    public void error() {
        Folyam.error(new IOException())
                .onBackpressureDrop()
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void errorDropCrash() {
        Folyam.range(1, 5)
                .onBackpressureDrop(v -> { throw new IOException(); })
                .test(0)
                .assertFailure(IOException.class);
    }


    @Test
    public void normalConditional() {
        Folyam.range(1, 5).onBackpressureDrop()
                .filter(v -> true)
                .test()
                .assertResult(1, 2, 3, 4, 5);
    }


    @Test
    public void normal1Conditional() {
        List<Integer> list = new ArrayList<>();
        Folyam.range(1, 5).onBackpressureDrop(list::add)
                .filter(v -> true)
                .test()
                .assertResult(1, 2, 3, 4, 5);

        assertTrue(list.toString(), list.isEmpty());
    }

    @Test
    public void normalBackpressuredConditional() {
        Folyam.range(1, 5).onBackpressureDrop()
                .filter(v -> true)
                .test(3)
                .assertResult(1, 2, 3);
    }

    @Test
    public void normal1BackpressuredConditional() {
        List<Integer> list = new ArrayList<>();
        Folyam.range(1, 5).onBackpressureDrop(list::add)
                .filter(v -> true)
                .test(3)
                .assertResult(1, 2, 3);

        assertEquals(Arrays.asList(4, 5), list);
    }

    @Test
    public void errorConditional() {
        Folyam.error(new IOException())
                .onBackpressureDrop()
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void errorDropCrashConditional() {
        Folyam.range(1, 5)
                .onBackpressureDrop(v -> { throw new IOException(); })
                .filter(v -> true)
                .test(0)
                .assertFailure(IOException.class);
    }

    @Test
    public void take() {
        Folyam.range(1, 5)
                .onBackpressureDrop()
                .take(3)
                .test()
                .assertResult(1, 2, 3);
    }
}
