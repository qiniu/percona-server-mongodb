#include "mongo/platform/basic.h"
#include "mongo/util/limit.h"
#include "mongo/unittest/unittest.h"

namespace {
    using namespace mongo;

    TEST(Limiter, Acquire) {
        auto iter = NewCountLimiter(1);
        ASSERT_NE(iter, nullptr);

        ASSERT_EQ(iter->Acquire(), true);
        ASSERT_EQ(iter->Acquire(), false);
    }

    TEST(Limiter, Release) {
        auto iter = NewCountLimiter(1);
        ASSERT_NE(iter, nullptr);

        ASSERT_EQ(iter->Acquire(), true);
        ASSERT_EQ(iter->Running(), 0);
        iter->Release();
        ASSERT_EQ(iter->Running(), 1);
    }

    TEST(Limiter, Running) {
        auto iter = NewCountLimiter(10);
        ASSERT_NE(iter, nullptr);

        ASSERT_EQ(iter->Acquire(), true);
        ASSERT_EQ(iter->Running(), 9);
        ASSERT_EQ(iter->Acquire(), true);
        ASSERT_EQ(iter->Running(), 8);
        iter->Release();
        ASSERT_EQ(iter->Running(), 9);
        iter->Release();
        ASSERT_EQ(iter->Running(), 10);
    }

}