#pragma once

#include <memory>

namespace mongo
{
    using std::shared_ptr;
    const int64_t kDefaultLimits = 100;

    class Limiter {
        public:
        explicit Limiter() = default;
        virtual ~Limiter() = default;

        virtual bool Acquire() = 0;
        virtual void Release() = 0; 
        virtual int64_t Running() = 0;
    };

    extern shared_ptr<Limiter> NewCountLimiter(int64_t limitNum);
} // mongo 