
# Hash function

Note that: Shrinking window filter (SWF) uses a very quick rather than near perfect hash function.

We observed that although the complexity of hashing is O (1),
the cost of hash calculation in XXHash is about 10~20ns.
As a result, when there are too many timestamps to check,
the hash function calculation will become a performance bottleneck for filtering.

In this paper, we employ linear congruential hashing to compute hash values for SWF.

### quick hash function source code (C++ version)
```c++
// More hash function can be seen: https://github.com/prasanthj/hasher
// See Martin Dietzfelbinger, "Universal hashing and k-wise independent random
// variables via integer arithmetic without primes".
class TwoIndependentMultiplyShift {
  unsigned __int128 multiply_, add_;

 public:
  TwoIndependentMultiplyShift() {
    ::std::random_device random;
    for (auto v : {&multiply_, &add_}) {
      *v = random();
      for (int i = 1; i <= 4; ++i) {
        *v = *v << 32;
        *v |= random();
      }
    }
  }

  uint64_t operator()(uint64_t key) const {
    return (add_ + multiply_ * static_cast<decltype(multiply_)>(key)) >> 64;
  }
};
```

### Java version
```java
import java.util.Random;

public class TwoIndependentMultiplyShift {
    private static long multiply_ = 0x9E3779B185EBCA87L;
    private static long add_ = 0x165667B19E3779F9L;

    public TwoIndependentMultiplyShift() {
        Random random = new Random();
        multiply_ = random.nextLong();
        add_ = random.nextLong();

        for (int i = 0; i < 4; i++) {
            multiply_ = (multiply_ << 32) | random.nextLong();
            add_ = (add_ << 32) | random.nextLong();
        }
    }

    public long apply(long key) {
        return (add_ + multiply_ * key) >>> 64;
    }
}
```

