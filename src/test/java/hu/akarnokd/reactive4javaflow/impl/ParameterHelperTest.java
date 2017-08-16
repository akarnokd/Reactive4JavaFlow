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

package hu.akarnokd.reactive4javaflow.impl;

import hu.akarnokd.reactive4javaflow.TestHelper;
import org.junit.Test;

import java.lang.reflect.Parameter;

public class ParameterHelperTest {

    @Test
    public void utilityClass() {
        TestHelper.checkUtilityClass(ParameterHelper.class);
    }

    @Test(expected = IllegalArgumentException.class)
    public void verifyPositiveInt() {
        ParameterHelper.verifyPositive(0, "param");
    }

    @Test(expected = IllegalArgumentException.class)
    public void verifyPositiveLong() {
        ParameterHelper.verifyPositive(0L, "param");
    }

    @Test(expected = IllegalArgumentException.class)
    public void verifyNonNegativeInt() {
        ParameterHelper.verifyNonNegative(-1, "param");
    }

    @Test(expected = IllegalArgumentException.class)
    public void verifyNonNegativeLong() {
        ParameterHelper.verifyNonNegative(-1L, "param");
    }

    @Test
    public void verify() {
        ParameterHelper.verifyPositive(1, "param");
        ParameterHelper.verifyPositive(1L, "param");
        ParameterHelper.verifyNonNegative(0, "param");
        ParameterHelper.verifyNonNegative(0L, "param");
        ParameterHelper.verifyNonNegative(1, "param");
        ParameterHelper.verifyNonNegative(1L, "param");
    }
}
