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

import java.util.*;
import java.util.concurrent.TimeUnit;

public class ParallelMinMaxTest {

    @Test
    public void normalMin() {
        Random rnd = new Random(0);

        Integer[] array = new Integer[1000];
        for (int i = 0; i < array.length; i++) {
            array[i] = i;
        }

        Collections.shuffle(Arrays.asList(array));

        Folyam.fromArray(array)
                .parallel()
                .runOn(SchedulerServices.computation())
                .min(Comparator.naturalOrder())
                .test()
                .awaitDone(5, TimeUnit.SECONDS)
                .assertResult(0);
    }

    @Test
    public void normalMax() {
        Random rnd = new Random(0);

        Integer[] array = new Integer[1000];
        for (int i = 0; i < array.length; i++) {
            array[i] = i;
        }

        Collections.shuffle(Arrays.asList(array));

        Folyam.fromArray(array)
                .parallel()
                .runOn(SchedulerServices.computation())
                .max(Comparator.naturalOrder())
                .test()
                .awaitDone(5, TimeUnit.SECONDS)
                .assertResult(999);
    }
}
