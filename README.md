# corama

This project is an experiment, aiming to improve some specific areas of [rama].

# Rama is great technology.

I'm really excited about the abstractions and computing model it provides. However, I think there are a few narrow (but important!) areas where it can be improved. I have three specific areas in mind, in order of significance:

## Rama's java API is not first-class

You can tell the java interface is exactly what it is - a port of an untyped clojure DSL. It's pretty unusual for a typed language:

 - rama variables aren't java variables, they're strings
 - the type of thing a variable refers to is determined by the prfix of the string (e.g '*', '$$', '$')
 - everything is of type `Object`, so you need to cast things correctly or you get runtime errors

The upshot of this is that the compiler is useless for telling you whether your rama topology is valid - you may find out at startup, or you may not find out until you actually hit the specific code path with a value whose type is not what the code expects.

I assume this is not a big focus for the rama devs, who mainly just need a workable alternative to the much terser clojure API (where the lack of compile-time types is par for the course). And to be honest I don't know if you could make a particularly nice API in java.

But with Scala, I think it can be done better.

## Rama's error messages are often unreadable

The experience of trying new things in rama can be an exercise in poring through large stacktraces with very little signal. Simple runtime errors (like a type mismatch) will often produce 1k lines of stacktrace with mostly irrelevant details. "Compilation" errors (as rama compiles the topology into some internal representation at runtime) are even harder to understand, since they're usually detached from the source locations that cause the actual issue.

For example, multiple times I've run into the following exception cause chain (with a screenful of stacktrace lines between each cause):

```
clojure.lang.ExceptionInfo: Failed to generate constants class
Caused by: clojure.lang.ExceptionInfo: Failed to generate constants class
Caused by: clojure.lang.Compiler$CompilerException: Syntax error compiling at (198:18).
Caused by: clojure.lang.ExceptionInfo: Failed to generate constants class
Caused by: clojure.lang.ExceptionInfo: Object cache disallowed
```

The cause: "somewhere, an object which isn't serializable has been included in the topology definition". In one example I passed a function a literal object instead of that object's ID field, with no assistance from the compiler to track this down since the API accepts `Object`.

Some of this may be inherent to the problem of compiling a high level declarative topology into a lower level execution, or necessary to convert a naive representation into a more efficient runtime plan. But I suspect there are definitely some improvements that could be made.

## Rama's serialization could be improved

This is the least important part, at least for now. But I hope that a typeclass-based codec approach can be more ergonomic than rama's cumbersome serialization registration API.

In particular, I don't believe rama's API can handle generic container types well (like `List[A]`), due to type erasure. It also has no compile-time (or even startup-time) guarantees that the types in use can be serialized, errors are only surfaced when it encounters an object at runtime that it doesn't know how to serialize.

# The plan:

Firstly, I'm building a scala DSL which can compile to rama topologies. My goal is to make the entire DSL fully typed - the user should never have to perform a runtime cast.

This can improve the API, but not the serialization or error messages (aside from moving most type mismatch errors from runtime to compile-time).

I'm also building a trivial interpreter, mainly for fast testing and validating that the rama backend matches the intended behaviour.

At some point, I _may_ attempt to build a new backend for this DSL. I expect this to be a toy rather than a viable alternative, because there's a huge list of operational aspects (cluster management, upgrades, etc) which rama already does extremely well, and which would be a huge investment to replicate. But for the sake of learning I'm interested to better understand how the core replication / execution mechanisms work, as well as playing around with deterministic simulation.

[rama]: https://redplanetlabs.com/
