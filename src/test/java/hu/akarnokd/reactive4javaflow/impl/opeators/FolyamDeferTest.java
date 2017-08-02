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
import hu.akarnokd.reactive4javaflow.fused.FusedSubscription;
import org.junit.Test;

import java.io.IOException;

public class FolyamDeferTest {
    @Test
    public void normal() {
        Folyam<String> f = Folyam.defer(() -> {
            int[] counter = { 0 };
            return Folyam.range(1, 5)
                .map(v -> (counter[0]++) + "-" + v);
        });

        f.test().assertResult("0-1", "1-2", "2-3", "3-4", "4-5");
        f.test().assertResult("0-1", "1-2", "2-3", "3-4", "4-5");
    }

    @Test
    public void differentReturns() {
        int[] counter = { 0 };
        Folyam<Integer> f = Folyam.defer(() -> Folyam.just(++counter[0]));

        f.test().assertResult(1);
        f.test().assertResult(2);
        f.test().assertResult(3);
    }

    @Test
    public void factoryCrashes() {
        Folyam.defer(() -> {
            throw new IOException();
        })
        .test()
        .assertFailure(IOException.class);
    }

    @Test
    public void fusionAvailable() {
        Folyam.defer(() -> Folyam.range(1, 5))
                .test(5, false, FusedSubscription.ANY)
                .assertFusionMode(FusedSubscription.SYNC)
                .assertResult(1, 2, 3, 4, 5);
    }
}
