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
import hu.akarnokd.reactive4javaflow.fused.FusedSubscription;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

public class FolyamCombineLatestTest {

    @Test
    public void standard() {
        TestHelper.assertResult(Folyam.combineLatestArray(Arrays::toString, Folyam.just(1), Folyam.just(2)),
                "[1, 2]");
    }

    @Test
    public void standard2() {
        TestHelper.assertResult(Folyam.combineLatestArray(Arrays::toString, Folyam.just(1), Folyam.range(1, 5)),
                "[1, 1]", "[1, 2]", "[1, 3]", "[1, 4]", "[1, 5]");
    }

    @Test
    public void standard3() {
        TestHelper.assertResult(Folyam.combineLatestArray(Arrays::toString, Folyam.just(1), Folyam.empty())
                );
    }

    @Test
    public void normal() {
        Folyam.combineLatestArray(Arrays::toString, Folyam.just(1), Folyam.just(2))
                .test()
                .assertResult("[1, 2]");
    }

    @Test
    public void normalConditional() {
        Folyam.combineLatestArray(Arrays::toString, Folyam.just(1), Folyam.just(2))
                .filter(v -> true)
                .test()
                .assertResult("[1, 2]");
    }

    @Test
    public void normalDelayError() {
        Folyam.combineLatestArrayDelayError(Arrays::toString, Folyam.just(1), Folyam.just(2))
                .test()
                .assertResult("[1, 2]");
    }

    @Test
    public void normalConditionalDelayError() {
        Folyam.combineLatestArrayDelayError(Arrays::toString, Folyam.just(1), Folyam.just(2))
                .filter(v -> true)
                .test()
                .assertResult("[1, 2]");
    }

    @Test
    public void normalIterable() {
        Folyam.combineLatest(List.of(Folyam.just(1), Folyam.just(2)), Arrays::toString)
                .test()
                .assertResult("[1, 2]");
    }

    @Test
    public void normalConditionalIterable() {
        Folyam.combineLatest(List.of(Folyam.just(1), Folyam.just(2)), Arrays::toString)
                .filter(v -> true)
                .test()
                .assertResult("[1, 2]");
    }

    @Test
    public void normalDelayErrorIterable() {
        Folyam.combineLatestDelayError(List.of(Folyam.just(1), Folyam.just(2)), Arrays::toString)
                .test()
                .assertResult("[1, 2]");
    }

    @Test
    public void normalConditionalDelayErrorIterable() {
        Folyam.combineLatestDelayError(List.of(Folyam.just(1), Folyam.just(2)), Arrays::toString)
                .filter(v -> true)
                .test()
                .assertResult("[1, 2]");
    }

    @Test
    public void errorFirst() {
        Folyam.combineLatestArray(Arrays::toString, Folyam.error(new IOException()), Folyam.just(2))
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void errorFirstConditional() {
        Folyam.combineLatestArray(Arrays::toString, Folyam.error(new IOException()), Folyam.just(2))
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }


    @Test
    public void errorSecond() {
        Folyam.combineLatestArray(Arrays::toString, Folyam.just(1), Folyam.error(new IOException()))
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void errorSecondConditional() {
        Folyam.combineLatestArray(Arrays::toString, Folyam.just(2), Folyam.error(new IOException()))
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }


    @Test
    public void errorFirstDelayError() {
        Folyam.combineLatestArrayDelayError(Arrays::toString, Folyam.error(new IOException()), Folyam.just(2))
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void errorFirstConditionalDelayError() {
        Folyam.combineLatestArrayDelayError(Arrays::toString, Folyam.error(new IOException()), Folyam.just(2))
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }


    @Test
    public void errorSecondDelayError() {
        Folyam.combineLatestArrayDelayError(Arrays::toString, Folyam.just(1), Folyam.error(new IOException()))
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void errorSecondConditionalDelayError() {
        Folyam.combineLatestArrayDelayError(Arrays::toString, Folyam.just(2), Folyam.error(new IOException()))
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void empty() {
        Folyam.combineLatestArray(Arrays::toString)
                .test()
                .assertResult();
    }

    @Test
    public void oneSource() {
        Folyam.combineLatestArray(Arrays::toString, Folyam.range(1, 5))
                .test()
                .assertResult("[1]", "[2]", "[3]", "[4]", "[5]");
    }

    @Test
    public void oneSourceConditional() {
        Folyam.combineLatestArray(Arrays::toString, Folyam.range(1, 5))
                .filter(v -> true)
                .test()
                .assertResult("[1]", "[2]", "[3]", "[4]", "[5]");
    }

    @Test
    public void iterableManySource() {
        List<Folyam<Integer>> list = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            list.add(Folyam.just(1));
        }
        Folyam.combineLatest(list, Arrays::toString)
        .test()
        .assertResult("[1, 1, 1, 1, 1, 1, 1, 1, 1, 1]");
    }

    @Test
    public void firstNull() {
        Folyam.combineLatestArray(Arrays::toString, null, Folyam.just(1))
                .test()
                .assertFailure(NullPointerException.class);
    }


    @Test
    public void secondNull() {
        Folyam.combineLatestArray(Arrays::toString, Folyam.just(1), null)
                .test()
                .assertFailure(NullPointerException.class);
    }

    @Test
    public void combinerCrash() {
        Folyam.combineLatestArray(a -> { throw new IOException(); }, Folyam.just(1), Folyam.just(2))
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void combinerCrashConditional() {
        Folyam.combineLatestArray(a -> { throw new IOException(); }, Folyam.just(1), Folyam.just(2))
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void longSource() {
        Folyam.combineLatestArray(a -> a, Folyam.just(1), Folyam.range(1, 1000))
                .test()
                .assertValueCount(1000)
                .assertNoErrors()
                .assertComplete();
    }

    @Test
    public void longSourceConditional() {
        Folyam.combineLatestArray(a -> a, Folyam.just(1), Folyam.range(1, 1000))
                .filter(v -> true)
                .test()
                .assertValueCount(1000)
                .assertNoErrors()
                .assertComplete();
    }

    @Test
    public void iterableCrash() {
        Folyam.combineLatest(new FailingMappedIterable<>(1, 10, 10, v -> Folyam.just(1)), Arrays::toString)
                .test()
                .assertFailure(IllegalStateException.class);
    }

    @Test
    public void fusedNoDelayErrorFail() {
        Folyam.combineLatestArray(a -> a, Folyam.just(1), Folyam.<Integer>error(new IOException()))
                .test(Long.MAX_VALUE, false, FusedSubscription.ANY)
                .assertFailure(IOException.class);
    }


    @Test
    public void fusedNoDelayErrorFailConditional() {
        Folyam.combineLatestArray(a -> a, Folyam.just(1), Folyam.<Integer>error(new IOException()))
                .filter(v -> true)
                .test(Long.MAX_VALUE, false, FusedSubscription.ANY)
                .assertFailure(IOException.class);
    }


    @Test
    public void fusedDelayErrorFail() {
        Folyam.combineLatestArrayDelayError(a -> a, Folyam.just(1), Folyam.<Integer>error(new IOException()))
                .test(Long.MAX_VALUE, false, FusedSubscription.ANY)
                .assertFailure(IOException.class);
    }


    @Test
    public void fusedDelayErrorFailConditional() {
        Folyam.combineLatestArrayDelayError(a -> a, Folyam.just(1), Folyam.<Integer>error(new IOException()))
                .filter(v -> true)
                .test(Long.MAX_VALUE, false, FusedSubscription.ANY)
                .assertFailure(IOException.class);
    }

    @Test
    public void fusedEmpty() {
        Folyam.combineLatestArrayDelayError(a -> a, Folyam.just(1), Folyam.<Integer>empty())
                .test(Long.MAX_VALUE, false, FusedSubscription.ANY)
                .assertResult();
    }

    @Test
    public void fusedEmptyConditional() {
        Folyam.combineLatestArrayDelayError(a -> a, Folyam.just(1), Folyam.<Integer>empty())
                .filter(v -> true)
                .test(Long.MAX_VALUE, false, FusedSubscription.ANY)
                .assertResult();
    }
}
