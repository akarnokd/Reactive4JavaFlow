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

package hu.akarnokd.reactive4javaflow.processors;

import hu.akarnokd.reactive4javaflow.TestHelper;
import hu.akarnokd.reactive4javaflow.impl.VH;
import org.junit.Test;

import java.lang.invoke.MethodHandles;

public class VHTest {

    @Test
    public void utilityClass() {
        TestHelper.checkUtilityClass(VH.class);
    }

    @Test(expected = InternalError.class)
    public void findInvalid() {
        VH.find(MethodHandles.lookup(), VHTest.class, "field", Object.class);
    }

    @Test(expected = InternalError.class)
    public void findStatic() {
        VH.findStatic(MethodHandles.lookup(), VHTest.class, "field", Object.class);
    }
}
