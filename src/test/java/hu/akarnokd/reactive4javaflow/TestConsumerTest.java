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

package hu.akarnokd.reactive4javaflow;

import hu.akarnokd.reactive4javaflow.fused.FusedSubscription;
import hu.akarnokd.reactive4javaflow.processors.*;
import hu.akarnokd.reactive4javaflow.impl.*;
import org.junit.Test;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class TestConsumerTest {
    @Test
    public void onSubscribeNull() {
        TestConsumer<Integer> tc = new TestConsumer<>();
        tc.onSubscribe(null);

        tc.assertError(NullPointerException.class)
                .assertErrorMessage("subscription == null in TestConsumer");
    }

    @Test
    public void onSubscribeMultiple() {
        TestConsumer<Integer> tc = new TestConsumer<>();
        BooleanSubscription bs1 = new BooleanSubscription();
        BooleanSubscription bs2 = new BooleanSubscription();
        tc.onSubscribe(bs1);
        tc.onSubscribe(bs2);

        assertFalse(bs1.isCancelled());
        assertTrue(bs2.isCancelled());

        tc.assertError(IllegalStateException.class)
                .assertErrorMessage("onSubscribe called again in TestConsumer");
    }


    @Test
    public void onSubscribeMultipleCancelled() {
        TestConsumer<Integer> tc = new TestConsumer<>();
        BooleanSubscription bs1 = new BooleanSubscription();
        BooleanSubscription bs2 = new BooleanSubscription();
        tc.onSubscribe(bs1);

        tc.cancel();

        tc.onSubscribe(bs2);

        assertTrue(bs1.isCancelled());
        assertTrue(bs2.isCancelled());

        tc.assertNoErrors();
    }

    @Test
    public void onNextWithoutOnSubscribe() {
        TestConsumer<Integer> tc = new TestConsumer<>();

        tc.onNext(1);

        tc.assertError(IllegalStateException.class)
                .assertErrorMessage("onSubscribe was not called before onNext in TestConsumer");
    }

    @Test
    public void onNextNull() {
        TestConsumer<Integer> tc = new TestConsumer<>();
        tc.onSubscribe(new BooleanSubscription());

        tc.onNext(null);

        tc.assertError(NullPointerException.class)
                .assertErrorMessage("item == null in TestConsumer");
    }

    @Test
    public void onErrorWithoutOnSubscribe() {
        TestConsumer<Integer> tc = new TestConsumer<>();

        tc.onError(new IOException());

        tc.assertInnerErrors(errors -> {
            TestHelper.assertError(errors, 0, IllegalStateException.class, "onSubscribe was not called before onError in TestConsumer");
            TestHelper.assertError(errors, 1, IOException.class);
        });
    }


    @Test
    public void onErrorNull() {
        TestConsumer<Integer> tc = new TestConsumer<>();
        tc.onSubscribe(new BooleanSubscription());

        tc.onError(null);

        tc.assertError(NullPointerException.class)
                .assertErrorMessage("throwable == null in TestConsumer");
    }

    @Test
    public void onCompleteWithoutOnSubscribe() {
        TestConsumer<Integer> tc = new TestConsumer<>();

        tc.onComplete();

        tc.assertError(IllegalStateException.class)
                .assertErrorMessage("onSubscribe was not called before onComplete in TestConsumer");
    }

    @Test
    public void asyncFusedCrash() {
        TestConsumer<Integer> tc = new TestConsumer<>();
        tc.requestFusionMode(FusedSubscription.ASYNC);

        SolocastProcessor<Integer> sp = new SolocastProcessor<>();

        sp.map(v -> (Integer)null).subscribe(tc);

        sp.onNext(1);

        tc.assertFailure(NullPointerException.class);
    }

    @Test
    public void syncFusionOnNextCall() {
        TestConsumer<Integer> tc = new TestConsumer<>();
        tc.requestFusionMode(FusedSubscription.SYNC);
        tc.onSubscribe(new FusedSubscription<Integer>() {
            @Override
            public void request(long n) {

            }

            @Override
            public void cancel() {

            }

            @Override
            public Integer poll() throws Throwable {
                return null;
            }

            @Override
            public boolean isEmpty() {
                return true;
            }

            @Override
            public void clear() {

            }

            @Override
            public int requestFusion(int mode) {
                return SYNC;
            }
        });
        tc.onNext(1);

        tc.assertError(IllegalStateException.class)
                .assertErrorMessage("Should not call onNext in SYNC mode.");
    }

    @Test
    public void onCompleteMultiple() {
        TestConsumer<Integer> tc = new TestConsumer<>();
        tc.onSubscribe(new BooleanSubscription());

        tc.onComplete();
        tc.onComplete();

        tc.assertError(IllegalStateException.class)
                .assertErrorMessage("onComplete called again: 2");
    }

    @Test
    public void failMessage() {
        TestConsumer<Integer> tc = new TestConsumer<>();
        tc.onSubscribe(new BooleanSubscription());
        tc.onNext(1);
        tc.onComplete();

        try {
            tc.assertFailure(Throwable.class, 1);
            fail("Should have thrown");
        } catch (AssertionError ex) {
            assertEquals(ex.getMessage(),
                    "No errors. (items: 1, errors: 0, completions: 1, latch: 0)",
                    ex.getMessage());
        }
    }

    @Test
    public void failMessageWithError() {
        TestConsumer<Integer> tc = new TestConsumer<>();
        tc.onSubscribe(new BooleanSubscription());
        tc.onNext(1);
        tc.onError(new IOException());

        try {
            tc.assertResult(1);
            fail("Should have thrown");
        } catch (AssertionError ex) {
            assertEquals(ex.getMessage(),
                    "Error(s) present. (items: 1, errors: 1, completions: 0, latch: 0)",
                    ex.getMessage());
        }
    }

    @Test
    public void failMessageTagCancelTimeout() {
        TestConsumer<Integer> tc = new TestConsumer<>();
        tc.onSubscribe(new BooleanSubscription());
        assertNull(tc.getTag());

        tc.withTag("Tag");
        tc.onNext(1);

        tc.awaitDone(1, TimeUnit.MILLISECONDS);

        assertEquals("Tag", tc.getTag());
        try {
            tc.assertFailure(Throwable.class, 1);
            fail("Should have thrown");
        } catch (AssertionError ex) {
            assertEquals(ex.getMessage(),
                    "No errors. (items: 1, errors: 0, completions: 0, latch: 1, timeout!, cancelled!, tag: Tag)",
                    ex.getMessage());
        }
    }

    @Test
    public void assertFusionMode() {
        try {
            Folyam.just(1)
                    .test()
                    .assertFusionMode(FusedSubscription.SYNC);
            fail("Should have thrown");
        } catch (AssertionError ex) {
            assertTrue(ex.toString(), ex.toString().contains("Wrong fusion mode."));
        }

        try {
            Folyam.just(1)
                    .test()
                    .assertFusionMode(FusedSubscription.ASYNC);
            fail("Should have thrown");
        } catch (AssertionError ex) {
            assertTrue(ex.toString(), ex.toString().contains("Wrong fusion mode."));
        }

        try {
            Folyam.just(1)
                    .test(1, false, FusedSubscription.SYNC)
                    .assertFusionMode(FusedSubscription.NONE);
            fail("Should have thrown");
        } catch (AssertionError ex) {
            assertTrue(ex.toString(), ex.toString().contains("Wrong fusion mode."));
        }

        try {
            Folyam.just(1).hide()
                    .test(1, false, FusedSubscription.SYNC)
                    .assertFusionMode(FusedSubscription.NONE);
            fail("Should have thrown");
        } catch (AssertionError ex) {
            assertTrue(ex.toString(), ex.toString().contains("Wrong fusion mode."));
        }

        assertEquals("??? 100", TestConsumer.fusionMode(100));
    }

    @Test
    public void awaitInterrupted() {
        DirectProcessor<Integer> dp = new DirectProcessor<>();

        try {
            Thread.currentThread().interrupt();
            dp.test()
                    .awaitDone(1, TimeUnit.MILLISECONDS);
            fail("Should have thrown");
        } catch (AssertionError ex) {
            assertTrue(ex.toString(), ex.toString().contains("Wait interrupted"));
        } finally {
            Thread.interrupted();
        }
        assertFalse(dp.hasSubscribers());
    }

    @Test
    public void wrongOutcome() {
        TestConsumer<Integer> tc = Folyam.just(1).test();

        try {
            tc.assertValues(1, 2);
            fail("Should have thrown");
        } catch (AssertionError ex) {
            assertTrue(ex.toString(), ex.toString().contains("Number of items differ."));
        }

        try {
            tc.assertValues(2);
            fail("Should have thrown");
        } catch (AssertionError ex) {
            assertTrue(ex.toString(), ex.toString().contains("Item #0 differs."));
        }

        try {
            tc.assertNotComplete();
            fail("Should have thrown");
        } catch (AssertionError ex) {
            assertTrue(ex.toString(), ex.toString().contains("Completed."));
        }

        try {
            tc.assertError(IOException.class);
            fail("Should have thrown");
        } catch (AssertionError ex) {
            assertTrue(ex.toString(), ex.toString().contains("No errors."));
        }
    }

    @Test
    public void terminalProblems() {
        TestConsumer<Integer> tc = new TestConsumer<>();
        try {
            tc.assertOnSubscribe();
            fail("Should have thrown");
        } catch (AssertionError ex) {
            assertTrue(ex.toString(), ex.toString().contains("onSubscribe not called."));
        }

        tc.onSubscribe(new BooleanSubscription());

        try {
            tc.assertComplete();
            fail("Should have thrown");
        } catch (AssertionError ex) {
            assertTrue(ex.toString(), ex.toString().contains("Not completed."));
        }

        try {
            tc.assertInnerErrors(errors -> { });
            fail("Should have thrown");
        } catch (AssertionError ex) {
            assertTrue(ex.toString(), ex.toString().contains("No errors."));
        }

        tc.onComplete();
        tc.onComplete();

        try {
            tc.assertComplete();
            fail("Should have thrown");
        } catch (AssertionError ex) {
            assertTrue(ex.toString(), ex.toString().contains("Multiple completions."));
        }
    }

    @Test
    public void errorProblem() {
        TestConsumer<Integer> tc = new TestConsumer<>();
        tc.onSubscribe(new BooleanSubscription());

        try {
            tc.assertErrorMessage("Message");
            fail("Should have thrown");
        } catch (AssertionError ex) {
            assertTrue(ex.toString(), ex.toString().contains("No errors."));
        }

        tc.onError(new IOException("Message"));

        try {
            tc.assertError(IllegalArgumentException.class);
            fail("Should have thrown");
        } catch (AssertionError ex) {
            assertTrue(ex.toString(), ex.toString().contains("Error not present."));
        }

        tc.onError(new IllegalArgumentException());

        try {
            tc.assertError(IllegalArgumentException.class);
            fail("Should have thrown");
        } catch (AssertionError ex) {
            assertTrue(ex.toString(), ex.toString().contains("Error present but not alone."));
        }

        try {
            tc.assertErrorMessage("Message");
            fail("Should have thrown");
        } catch (AssertionError ex) {
            assertTrue(ex.toString(), ex.toString().contains("Message present but other errors as well."));
        }
    }

    @Test
    public void valueAndClassNull() {
        assertEquals("null", TestConsumer.valueAndClass(null));
    }

    @Test
    public void wrongErrorMessage() {
        TestConsumer<Integer> tc = new TestConsumer<>();
        tc.onSubscribe(new BooleanSubscription());

        tc.onError(new IOException("Message"));

        try {
            tc.assertErrorMessage("Error");
            fail("Should have thrown");
        } catch (AssertionError ex) {
            assertTrue(ex.toString(), ex.toString().contains("Messages differ."));
        }
    }

    @Test
    public void awaitCountInterrupted() {
        DirectProcessor<Integer> dp = new DirectProcessor<>();

        try {
            Thread.currentThread().interrupt();
            dp.test()
                    .awaitCount(1, 1, 10);
        } finally {
            Thread.interrupted();
        }
        assertFalse(dp.hasSubscribers());
    }

    @Test
    public void syncRequest() {
        try {
            Folyam.just(1)
                    .test(0, false, FusedSubscription.SYNC)
                    .requestMore(1);
            fail("Should have thrown");
        } catch (AssertionError ex) {
            assertTrue(ex.toString(), ex.toString().contains("Requesting in SYNC fused mode is forbidden."));
        }
    }

    @Test
    public void assertValueAt() {
        TestConsumer<Integer> tc = new TestConsumer<>();
        tc.onSubscribe(new BooleanSubscription());

        try {
            tc.assertValueCount(1);
            fail("Should have thrown");
        } catch (AssertionError ex) {
            assertTrue(ex.toString(), ex.toString().contains("Number of items differ."));
        }

        try {
            tc.assertValueSet(Set.of(0));
            fail("Should have thrown");
        } catch (AssertionError ex) {
            assertTrue(ex.toString(), ex.toString().contains("Number of items differ."));
        }

        try {
            tc.assertValueAt(0, 1);
            fail("Should have thrown");
        } catch (AssertionError ex) {
            assertTrue(ex.toString(), ex.toString().contains("Not enough elements"));
        }

        tc.onNext(1);

        try {
            tc.assertValueAt(0, 2);
            fail("Should have thrown");
        } catch (AssertionError ex) {
            assertTrue(ex.toString(), ex.toString().contains("Item @ 0 differs."));
        }

        try {
            tc.assertValueSet(Set.of(0));
            fail("Should have thrown");
        } catch (AssertionError ex) {
            assertTrue(ex.toString(), ex.toString().contains("Item @ 0 not expected: "));
        }
    }

    @Test
    public void timeout() {
        TestConsumer<Integer> tc = new TestConsumer<>();
        tc.onSubscribe(new BooleanSubscription());

        tc.awaitDone(1, TimeUnit.MILLISECONDS);

        try {
            tc.assertNoTimeout();
            fail("Should have thrown");
        } catch (AssertionError ex) {
            assertTrue(ex.toString(), ex.toString().contains("Timeout."));
        }
    }

    @Test
    public void missingSubscription() {
        assertEquals(1, TestConsumer.MissingSubscription.values().length);

        TestConsumer.MissingSubscription.MISSING.cancel();
        TestConsumer.MissingSubscription.MISSING.request(1);

        assertNotNull(TestConsumer.MissingSubscription.valueOf("MISSING"));

    }
}
