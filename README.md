# Reactive4JavaFlow

<a href='https://travis-ci.com/akarnokd/Reactive4JavaFlow/builds'><img src='https://travis-ci.com/akarnokd/Reactive4JavaFlow.svg?branch=master'></a>
[![codecov.io](http://codecov.io/github/akarnokd/Reactive4JavaFlow/coverage.svg?branch=master)](http://codecov.io/github/akarnokd/Reactive4JavaFlow?branch=master)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.akarnokd/reactive4javaflow/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.github.akarnokd/reactive4javaflow)


Reactive Programming library based on the Java 9 Flow API and a 4th generation ReactiveX-style architecture.

### Gradle

```groovy
compile "com.github.akarnokd:reactive4javaflow:0.1.5"
```

# Getting Started

The base package of Reactive4JavaFlow is `hu.akarnokd.reactive4javaflow` where the main reactive classes are located.
Note that the library requires a JDK 9 compatible runtime, thus it is not compatible with most of what is
currently available on Android.

The library doesn't declare modules (Project Jigsaw) and should be usable from both classpath and modularized environments.

## `Folyam` 

The `Folyam` base reactive type is the 0 .. N element, backpressure aware, Java 9 Flow.Publisher implementation. 
The word means flow or stream in hungarian and was chosen to avoid name clashing with existing libraries and types (such
as `Flowable`, `Flux`, `Stream` and unfortunately `Flow` too). The name is also one of the few words describing the
target concept that doesn't use accented letters.

`Folyam` features most of the typical [ReactiveX](http://reactivex.io/documentation/operators.html) operators commonly 
found in RxJava and Reactor and couple of the less common extension operators from 
[RxJava 2 Extensions](https://github.com/akarnokd/RxJava2Extensions#features).

### Hello world!

```java
import hu.akarnokd.reactive4javaflow.*;

Folyam.just("Hello world!")
      .subscribe(System.out::println);
```

Operators that work with multiple sources and may need to delay errors now have separate methods with `DelayError`
postfix: `concatDelayError`, `flatMapDelayError`.

## `Esetleg`

The `Esetleg` base reactive type is the 0 .. 1 element, backpressure aware, Java 9 FLow.Publisher implementation.
It resembles the RxJava 2 `Maybe` type and is similar to Reactor's `Mono` type. The word itself means maybe or
perhaps.

### Hello world!

```java
AutoDisposable d = Esetleg.fromCallable(() -> "Hello world")
.subscribeWith(new AbstractFolyamSubscriber<>() {
    @Override public void onNext(String s) {
        System.out.print(s);
    }
    @Override public void onError(Throwable ex) {
        ex.printStackTrace();
    }
    @Override public void onComplete() {
        System,out.println("!");
    }
});

d.close();
```

## `AutoDisposable`

The resource manager type is the `AutoDisposable` interface, which extends from `java.util.AutoCloseable` and thus can
be used in **try-with-resources**. Unlike RxJava 2 `Disposable`, the interface doesn't offer any means to check if
the resource is disposed.

The `AbstractFolyamSubscriber` shown in the previous section's example also implements `AutoDisposable` and together
with `hu.akarnokd.reactive4javaflow.disposables.CompositeAutoDisposable` can be used to track and mass-close resources.

The `disposables.BooleanAutoDisposable` can help in testing and `SequentialAutoDisposable` may help with
custom operators or individual resource tracking.

## `ParallelFolyam`

Just like RxJava 2 and Reactor 3, the library offers parallel operations through the `Folyam.parallel()` method. The
`ParallelFolyam` features the same operators as RxJava 2 does plus several of the sequential and less common operators
such as the asynchronous `mapWhen()`.

```java
Folyam.range(1, 100)
.parallel()
.runOn(SchedulerServices.computation())
.sumInt()
.test()
.assertResult(5050);
```

## Schedulers

The library offers the standard set of global schedulers in `SchedulerServices` (named to avoid too much conflict with
RxJava and Reactor): `computation`, `io`, `newThread`, `single`, `trampoline` and `newExecutor`. Testing can be done with the
`TestSchedulerService` class. 

In addition, the `SchedulerServices` allows creating custom schedulers, parameterized by name, priority and daemon-ness
via `newParallel`, `newIO`, `newThread` and `newSingle`. The utility class also supports creating blocking schedulers via
`newBlocking` and sharing an existing worker via `newShared`.

The `SchedulerService` interface implemented by the scheduler offers the ability to schedule tasks of
`Runnable` directly or on a `Worker`; immediately, delayed or periodically. The `Worker` interface
implements `AutoDisposable` and all of the pending tasks can be cancelled via `Worker.close()`.

The current API design in `Folyam` and `Esetleg` doesn't offer operators with default schedulers; the
`SchedulerService` to be used must be specified as parameter to these operators.

```java
Folyam.intervalRange(1, 5, 100, 100, TimeUnit.MILLISECONDS, SchedulerServices.single())
.blockingSubscribe(System.out::println);
```

## Functional interfaces

Unfortunately, Java's default functional interfaces don't support throwing checked exceptions, therefore
a set of new interfaces were added and used throughout the API:

```java
import hu.akarnokd.reactive4javaflow.functionals.*;

CheckedFunction<Integer, Integer> f = v -> { throw new IOException(); };

Folyam.just(1).map(f).test().assertFailure(IOException.class);
```

## Processors (hot sources)

The library features almost all standard "subject" types from RxJava in the `processors` subpackage:

- `DirectProcessor`: emit items directly to multiple subscribers, signals error if a particular subscriber is not ready to receive items (similar to RxJava's `PublishProcessor`),
- `MulticastProcessor`: coordinates the backpressure between subscribers (no standard RxJava equivalent),
- `FirstProcessor`: takes and caches the first item or terminal signal and exposes it as an `Esetleg` reactive type (similar to `MaybeSubject`),
- `LastProcessor`: waits for and caches the last item or terminal signal and exposes it as a `Folyam` (similar to `AsyncProcessor`),
- `CachingProcessor`: caches items (unbounded, size and/or time bound) and replays them to current and future subscribers (similar to `ReplayProcessor`),
- `SolocastProcessor`: buffers items until a single subscriber is able to consume them (similar to `UnicastProcessor`).

The `BehaviorProcessor` is currently not replicated in this library and can be emulated via `new CachingProcessor<>(1)`.

## Connectable flows

The cold-to-hot conversion is available via the `Folyam.publish()` and `Folyam.replay()` operators which return a
`ConnectableFolyam` instance.

Unlike RxJava and Reactor, the `ConnectableFolyam` has 3 states: fresh, running, terminated. In order to get back
to the fresh state, one has to call `ConnectableFolyam.reset()` in the terminated state.

The reason for this design difference is that the RxJava behavior often causes trouble in preparing subscribers
after the first round has completed, which can lead to data loss in case of `publish()`.

```java
ConnectableFolyam<Integer> cf = Folyam.range(1, 5).publish();

cf.subscribe(System.out::println);
cf.subscribe(System.out::println);

cf.connect();

// the source has run to completion and
// publish will complete any latecommers

cf.test().assertResult();

// go back to the fresh state

cf.reset();

// prepare the next set of subscribers without
// the source to rush ahead

cf.subscribe(System.out::println);
cf.subscribe(System.out::println);

cf.connect();

```

A more prominent effect is when `replay` or `replayLast()` is used. Once the source completed, all or
the last items are still available to late subscribers until `reset` is called.


The usual `autoConnect()` and `refCount` operators are supported along with `refCount` overloads that
allows specifying the minimum subscriber count and/or a grace period before closing the connection after
the last subscriber has cancelled.

## Testing

Testing the `Folyam` and `Esetleg` types can be done via the convenient `.test()` method which uses
a `TestConsumer` class behind the scenes (similar to `TestSubscriber`).

Scheduler-dependent operators can be tested with the help of the `TestSchedulerService` and its `advanceTimeBy` method
as the mean to move time forward.

```java
TestSchedulerService sch = new TestSchedulerService();

TestConsumer<Integer> tc = Folyam.just(1).delay(5, TimeUnit.MILLISECONDS, sch).test();

tc.assertEmpty();

tc.advanceTimeBy(5, TimeUnit.MILLISECONDS);

tc.assertResult(1);

```

## Plugins & hooks

The `FolyamPlugins` offers the ability to hook the assembling of operators (`onAssembly`), when they get
subscribed `onSubscribe` and when there is an undeliverable exception `onError`. 

The `FolyamPluins` also allows overriding the initial and current schedulers returned by `SchedulerServices`.
Note that accessing `SchedulerServices` - for example, to create a custom scheduler via one of the `newXXX` methods
may prematurely initialize the other schedulers. Therefore, it is recommended to hook the
current schedulers only (`setOnComputationSchedulerService`).

## Operator fusion

The **Reactive4JavaFlow** library is a 4th generation reactive solution that supports the operator fusion concept
more extensively than RxJava 2 does at the moment. Since there is no standard, multi-library API for operator fusion anyway, this
library defines the core interfaces in the `hu.akarnokd.reactive4javaflow.fused` package:

- `ConditionalSubscriber`: with its `tryOnNext` method, such subscriber can avoid `request(1)` calls and keep draining longer without costly atomic decrements
- `FusedQueue`: base interface for queues with minimal number of operations and a `poll()` method that can throw a `Throwable`
- `FusedSubscription`: the combination of `FusedQueue` and the Java 9 `Flow.Subscription` interfaces to establish and run fused queues
- `FusedDynamicSource`: represents a source that can produce an item, null or throw an exception at the runtime of the flow
- `FusedStaticSource`: special variant of the dynamic source that can hosts a constant (or is empty) and allows assembly time optimizations.
