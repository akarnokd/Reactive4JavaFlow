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
import hu.akarnokd.reactive4javaflow.functionals.CheckedFunction;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

public class FolyamZipLatestTest {

    CheckedFunction<Object[], Integer> sum = a -> {
        int sum = 0;
        for (Object o : a) {
            sum += (Integer)o;
        }
        return sum;
    };

    @Test
    public void standard() {
        TestHelper.assertResult(
                Folyam.zipLatestArray(sum, Folyam.just(1), Folyam.just(2), Folyam.just(3)),
                6
        );
    }

    @Test
    public void standardHidden() {
        TestHelper.assertResult(
                Folyam.zipLatestArray(sum, Folyam.just(1).hide(), Folyam.just(2).hide(), Folyam.just(3).hide()),
                6
        );
    }


    @Test
    public void standard2() {
        TestHelper.assertResult(
                Folyam.zipLatest(List.of(Folyam.just(1), Folyam.just(2), Folyam.just(3)), sum),
                6
        );
    }

    @Test
    public void standard2Hidden() {
        TestHelper.assertResult(
                Folyam.zipLatest(List.of(Folyam.just(1).hide(), Folyam.just(2).hide(), Folyam.just(3).hide()), sum),
                6
        );
    }


    @Test
    public void standard3() {
        TestHelper.assertResult(
                Folyam.zipLatestArray(sum, Folyam.range(1, 5), Folyam.range(11, 5), Folyam.just(3)),
                23
        );
    }

    @Test
    public void normal() {
        Folyam.zipLatestArray(sum, Folyam.just(1), Folyam.just(2), Folyam.just(3))
                .test()
                .assertResult(6);
    }

    @Test
    public void iterableManyAndCrash() {
        List<Folyam<Integer>> list = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            list.add(Folyam.never());
        }
        list.add(null);

        Folyam.zipLatest(list, sum)
                .test()
                .assertFailure(NullPointerException.class);
    }

    @Test
    public void innerError() {
        Folyam.zipLatestArray(sum, Folyam.just(1), Folyam.error(new IOException()))
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void innerErrorBackpressured() {
        Folyam.zipLatestArray(sum, Folyam.just(1), Folyam.error(new IOException()))
                .test(0L)
                .assertFailure(IOException.class);
    }

    @Test
    public void emptyArray() {
        Folyam.zipLatestArray(sum)
                .test()
                .assertResult();
    }

    @Test
    public void cancel() {
        Folyam.zipLatestArray(sum, Folyam.never(), Folyam.never())
                .test()
                .cancel()
                .assertEmpty();
    }

    @Test
    public void zipperCrash() {
        Folyam.zipLatestArray(a -> { throw new IOException(); }, Folyam.just(1), Folyam.just(2))
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void takeBackpressured() {
        Folyam.zipLatestArray(sum, Folyam.just(1), Folyam.just(2))
                .take(1)
                .test(1)
                .assertResult(3);
    }
}
