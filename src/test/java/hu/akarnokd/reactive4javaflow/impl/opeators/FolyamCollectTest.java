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

import java.io.IOException;
import java.util.*;

public class FolyamCollectTest {

    @Test
    public void standard() {
        TestHelper.<List<Integer>>assertResult(
                Folyam.range(1, 5)
                .collect(ArrayList::new, List::add),
                Arrays.asList(1, 2, 3, 4, 5)
        );
    }

    @Test
    public void error() {
        Folyam.error(new IOException())
                .collect(ArrayList::new, ArrayList::add)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void collectionSupplierCrash() {
        Folyam.range(1, 5)
                .collect(() -> { throw new IOException(); }, (a, b) -> { })
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void collectorCrash() {
        Folyam.range(1, 5)
                .collect(() -> 1, (a, b) -> { throw new IOException(); })
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void donePath() {
        TestHelper.<List<Integer>>folyamDonePath(f -> f.collect(ArrayList::new, List::add), List.of(1));
    }
}
