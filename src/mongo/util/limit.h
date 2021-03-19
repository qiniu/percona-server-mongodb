#if defined(MONGO_UTIL_LIMIT_H_)
#error \
    "mongo/util/limit.h cannot be included multiple times. " \
       "This may occur when log.h is included in a header. " \
       "Please check your #include's."
#else  // MONGO_UTIL_LIMIT_H_
#define MONGO_UTIL_LIMIT_H_

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
} // namespace 


#endif //MONGO_UTIL_LIMIT_H_