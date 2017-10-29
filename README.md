# DiskVec

A thread-safe, on-disk vector for Copy types.

[Documentation](https://docs.rs/diskvec/)

# Limitations
* A value read from the diskarray might have been corrupted by faulty
  writes. For this reason, it is recommended that `T` carries its own
  checksum capability.
* Write locks are done with a finite amount of mutexes, that may be less
  than the amount of elements in the vector, so deadlocks are possible even
  if you try to obtain mutable references to two different index positions.
* Since reads are lock-free, there is no guarantee that the value you're
  holding a reference to will not change behind your back.
# Guarantees
* Writes are done using locks, so no writes will trample each other.