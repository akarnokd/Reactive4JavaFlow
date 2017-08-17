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

import hu.akarnokd.reactive4javaflow.impl.VH;
import org.openjdk.jmh.annotations.*;

import java.lang.invoke.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Example benchmark. Run from command line as
 * <br>
 * gradle jmh -Pjmh="VarHandleCostPerf"
 */
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 5)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(value = 1)
@State(Scope.Thread)
public class VarHandleCostPerf {

    AtomicReference<Flow.Subscription> z = new AtomicReference<>() { };
    AtomicReference<Flow.Subscription> y = new AtomicReference<>() { };
    AtomicReference<Flow.Subscription> x = new AtomicReference<>() { };
    AtomicReference<Flow.Subscription> w = new AtomicReference<>() { };
    AtomicReference<Flow.Subscription> v = new AtomicReference<>() { };
    AtomicReference<Flow.Subscription> u = new AtomicReference<>() { };
    AtomicReference<Flow.Subscription> t = new AtomicReference<>() { };

    A a = new A();
    B b = new B();
    C c = new C();
    D d = new D();
    E e = new E();
    F f = new F();
    G g = new G();

    @Benchmark
    public void baseline1() {
        AtomicReference<Flow.Subscription> t = this.t;

        cancel(t);
        t.setPlain(null);
    }

    @Benchmark
    public void baseline2() {
        cancel(t);
        cancel(u);

        t.setPlain(null);
        u.setPlain(null);
    }

    @Benchmark
    public void baseline3() {
        cancel(t);
        cancel(u);
        cancel(v);

        t.setPlain(null);
        u.setPlain(null);
        v.setPlain(null);
    }

    @Benchmark
    public void baseline4() {
        cancel(t);
        cancel(u);
        cancel(v);
        cancel(w);

        t.setPlain(null);
        u.setPlain(null);
        v.setPlain(null);
        w.setPlain(null);
    }

    @Benchmark
    public void baseline5() {
        cancel(t);
        cancel(u);
        cancel(v);
        cancel(w);
        cancel(x);

        t.setPlain(null);
        u.setPlain(null);
        v.setPlain(null);
        w.setPlain(null);
        x.setPlain(null);
    }

    @Benchmark
    public void baseline6() {
        cancel(t);
        cancel(u);
        cancel(v);
        cancel(w);
        cancel(x);
        cancel(y);

        t.setPlain(null);
        u.setPlain(null);
        v.setPlain(null);
        w.setPlain(null);
        x.setPlain(null);
        y.setPlain(null);
    }

    @Benchmark
    public void baseline7() {
        cancel(t);
        cancel(u);
        cancel(v);
        cancel(w);
        cancel(x);
        cancel(y);
        cancel(z);

        t.setPlain(null);
        u.setPlain(null);
        v.setPlain(null);
        w.setPlain(null);
        x.setPlain(null);
        y.setPlain(null);
        z.setPlain(null);
    }

    @Benchmark
    public void bench1() {
        a.cancel();

        a.upstream = null;
    }

    @Benchmark
    public void bench2() {
        a.cancel();
        b.cancel();

        a.upstream = null;
        b.upstream = null;
    }


    @Benchmark
    public void bench3() {
        a.cancel();
        b.cancel();
        c.cancel();

        a.upstream = null;
        b.upstream = null;
        c.upstream = null;
    }

    @Benchmark
    public void bench4() {
        a.cancel();
        b.cancel();
        c.cancel();
        d.cancel();

        a.upstream = null;
        b.upstream = null;
        c.upstream = null;
        d.upstream = null;
    }

    @Benchmark
    public void bench5() {
        a.cancel();
        b.cancel();
        c.cancel();
        d.cancel();
        e.cancel();

        a.upstream = null;
        b.upstream = null;
        c.upstream = null;
        d.upstream = null;
        e.upstream = null;
    }

    @Benchmark
    public void bench6() {
        a.cancel();
        b.cancel();
        c.cancel();
        d.cancel();
        e.cancel();
        f.cancel();

        a.upstream = null;
        b.upstream = null;
        c.upstream = null;
        d.upstream = null;
        e.upstream = null;
        f.upstream = null;
    }

    @Benchmark
    public void bench7() {
        a.cancel();
        b.cancel();
        c.cancel();
        d.cancel();
        e.cancel();
        f.cancel();
        g.cancel();

        a.upstream = null;
        b.upstream = null;
        c.upstream = null;
        d.upstream = null;
        e.upstream = null;
        f.upstream = null;
        g.upstream = null;
    }


    static final class A {
        Flow.Subscription upstream;
        static final VarHandle UPSTREAM = VH.find(MethodHandles.lookup(), A.class, "upstream", Flow.Subscription.class);

        void cancel() {
            VarHandleCostPerf.cancel(this, UPSTREAM);
        }
    }

    static final class B {
        Flow.Subscription upstream;
        static final VarHandle UPSTREAM = VH.find(MethodHandles.lookup(), B.class, "upstream", Flow.Subscription.class);

        void cancel() {
            VarHandleCostPerf.cancel(this, UPSTREAM);
        }
    }
    static final class C {
        Flow.Subscription upstream;
        static final VarHandle UPSTREAM = VH.find(MethodHandles.lookup(), C.class, "upstream", Flow.Subscription.class);

        void cancel() {
            VarHandleCostPerf.cancel(this, UPSTREAM);
        }
    }
    static final class D {
        Flow.Subscription upstream;
        static final VarHandle UPSTREAM = VH.find(MethodHandles.lookup(), D.class, "upstream", Flow.Subscription.class);

        void cancel() {
            VarHandleCostPerf.cancel(this, UPSTREAM);
        }
    }
    static final class E {
        Flow.Subscription upstream;
        static final VarHandle UPSTREAM = VH.find(MethodHandles.lookup(), E.class, "upstream", Flow.Subscription.class);

        void cancel() {
            VarHandleCostPerf.cancel(this, UPSTREAM);
        }
    }
    static final class F {
        Flow.Subscription upstream;
        static final VarHandle UPSTREAM = VH.find(MethodHandles.lookup(), F.class, "upstream", Flow.Subscription.class);

        void cancel() {
            VarHandleCostPerf.cancel(this, UPSTREAM);
        }
    }
    static final class G {
        Flow.Subscription upstream;
        static final VarHandle UPSTREAM = VH.find(MethodHandles.lookup(), G.class, "upstream", Flow.Subscription.class);

        void cancel() {
            VarHandleCostPerf.cancel(this, UPSTREAM);
        }
    }

    public static boolean cancel(AtomicReference<Flow.Subscription> field) {
        Flow.Subscription a = field.getAcquire();
        if (a != CANCELLED) {
            a = field.getAndSet(CANCELLED);
            if (a != CANCELLED) {
                if (a != null) {
                    a.cancel();
                }
                return true;
            }
        }
        return false;
    }

    public static boolean cancel(Object target, VarHandle upstream) {
        Flow.Subscription a = (Flow.Subscription)upstream.getAcquire(target);
        if (a != CANCELLED) {
            a = (Flow.Subscription)upstream.getAndSet(target, CANCELLED);
            if (a != CANCELLED) {
                if (a != null) {
                    a.cancel();
                }
                return true;
            }
        }
        return false;
    }

    static final Flow.Subscription CANCELLED = new Flow.Subscription() {

        @Override
        public void request(long n) {

        }

        @Override
        public void cancel() {

        }
    };
}