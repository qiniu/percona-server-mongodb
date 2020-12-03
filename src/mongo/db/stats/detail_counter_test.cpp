/**
 * Copyright (C) 2016 MongoDB Inc.
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects
 * for all of the code used other than as permitted herein. If you modify
 * file(s) with this exception, you may extend this exception to your
 * version of the file(s), but you are not obligated to do so. If you do not
 * wish to do so, delete this exception statement from your version. If you
 * delete this exception statement from all source files in the program,
 * then also delete it in the license file.
 */

#include "mongo/platform/basic.h"

#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/bsontypes.h"
#include "mongo/db/stats/detail_counter.h"
#include "mongo/unittest/unittest.h"
#include <iostream>

namespace mongo {
    namespace {

        TEST(DetailCmdCounter, Init) {
            DetailCmdCounter tmp("test");
            ASSERT_TRUE(G_D_C_CONTAINER.get("test") != nullptr);
            ASSERT_TRUE(G_D_C_CONTAINER.get("test")->getName() == "test");
        }

        TEST(DetailCmdCounter, Destruct) {
            {
                DetailCmdCounter tmp("erase");
            }
            ASSERT_TRUE(G_D_C_CONTAINER.get("erase") == nullptr);
        }

        TEST(DetailCmdCounter, gotFailure) {
            DetailCmdCounter tmp("failure");
            ASSERT_TRUE(tmp.getFailure() == 0);
            tmp.gotFailure();
            ASSERT_TRUE(tmp.getFailure() == 1);
            tmp.gotFailure();
            ASSERT_TRUE(tmp.getFailure() == 2);
            tmp.gotFailure();
            ASSERT_TRUE(tmp.getFailure() == 3);
            tmp.gotFailure();
            ASSERT_TRUE(tmp.getFailure() == 4);
        } 

        TEST(DetailCmdCounter, getObj) {
            DetailCmdCounter tmp("failure");

            BSONObj obj = tmp.getObj();
            ASSERT_TRUE(obj.nFields() == 2);
            auto failureCnt = obj.getIntField("failureCnt");
            ASSERT_TRUE(failureCnt == 0);

            auto commandInfos = obj.getObjectField("commands");
            ASSERT_TRUE(commandInfos.nFields() == 3);
            ASSERT_TRUE(commandInfos.getIntField("latency") == 0);
            ASSERT_TRUE(commandInfos.getIntField("ops") == 0);

            tmp.gotLatency(100);
            obj = tmp.getObj();
            commandInfos = obj.getObjectField("commands");

            std::cout << commandInfos.toString() << std::endl;
            ASSERT_TRUE(commandInfos.getIntField("latency") != 0);
            ASSERT_TRUE(commandInfos.getIntField("ops") != 0);
        }

        TEST(DetailCmdCounterContainer, append) {
            DetailCmdCounterContainer tmp;
            ASSERT_TRUE(tmp.size() == 0);

            DetailCmdCounter c1("1");

            BaseDetailCmdCounter* tmpC1 = &c1;
            tmp.append(tmpC1);
            ASSERT_TRUE(tmp.size() == 1);
            ASSERT_TRUE(tmp.get("1") == tmpC1);

            tmp.append(&c1);
            ASSERT_TRUE(tmp.size() == 1);
            ASSERT_TRUE(tmp.get("1") == tmpC1);

            DetailCmdCounter c2("2");
            tmp.append(&c2);
            ASSERT_TRUE(tmp.size() == 2);

            tmp.append(nullptr);
            ASSERT_TRUE(tmp.size() == 2);
        }

        TEST(DetailCmdCounterContainer, remove) {
            DetailCmdCounterContainer tmp;
            ASSERT_TRUE(tmp.size() == 0);

            DetailCmdCounter c1("1");

            BaseDetailCmdCounter* tmpC1 = &c1;
            tmp.append(tmpC1);
            ASSERT_TRUE(tmp.size() == 1);
            ASSERT_TRUE(tmp.get("1") == tmpC1);

            tmp.remove("1");
            ASSERT_TRUE(tmp.size() == 0);
        }
    }
}