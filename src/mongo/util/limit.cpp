#include "limit.h"
#include <atomic>

namespace mongo 
{
    class CountLimiter: public Limiter {
        public:
            CountLimiter(int64_t limits) {
                if (limits < 0) {
                    _limits = kDefaultLimits; 
                } else {
                    _limits = limits;
                }
            }
            virtual ~CountLimiter() = default;
            virtual bool Acquire() {
                if(_limits.fetch_sub(1) >= 1) {
                    return true;
                }
                _limits.fetch_add(1);
                return false;
            }

            virtual void Release() {
                _limits.fetch_add(1);
            }

            virtual int64_t Running() {
                return _limits;
            }


        private:
            std::atomic<int64_t> _limits;
    };

    shared_ptr<Limiter> NewCountLimiter(int64_t limitNum) {
        return std::make_shared<CountLimiter>(limitNum);
    }
} // namespace mongo 
