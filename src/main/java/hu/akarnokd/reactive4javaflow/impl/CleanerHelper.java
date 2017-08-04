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

import java.lang.ref.Cleaner;

/**
 * Hosts the standard Java 9' new {@link Cleaner} service that allows cleaning up
 * abandoned resources such as {@code newThread()} and {@code io()} SchedulerService
 * workers.
 */
public final class CleanerHelper {

    static final Cleaner cleaner;

    static {
        cleaner = Cleaner.create(r -> new Thread(r, "Reactive4JavaFlow.Cleaner"));
    }

    private CleanerHelper() {
        throw new IllegalStateException("No instances!");
    }

    public static Cleaner.Cleanable register(Object object, Runnable action) {
        return cleaner.register(object, action);
    }
}
