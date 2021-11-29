/**
 *    Copyright (C) 2015 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kASIO

#include "mongo/platform/basic.h"

#include "mongo/executor/async_stream.h"
#include "mongo/executor/async_stream_common.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/log.h"
#include "mongo/base/system_error.h"
#include "mongo/db/stats/sockscounter.h"
#include "mongo/db/server_options.h"
namespace mongo {
namespace executor {

using asio::ip::tcp;

AsyncStream::AsyncStream(asio::io_service::strand* strand)
    : _strand(strand), _stream(_strand->get_io_service()) {}

AsyncStream::~AsyncStream() {
    destroyStream(&_stream, _connected);
}

const SockAddr& AsyncStream::getRemoteAddr() {
    return _realRemoteAddr;
}

bool AsyncStream::switchSocket(int64_t newFd) {
    if (newFd <= 0 ) {
        LOG(0) << "[MongoStat][Asio][Switch] switch new fd is invalid";
        return false;
    }

    int oldFd = _stream.native_handle();
    //需要先关闭之前的socket
    destroyStream(&this->_stream, _connected);
    
    asio::ip::tcp::socket newSocketFd(_strand->get_io_service(), asio::ip::tcp::v4(), newFd);
    this->_stream = std::move(newSocketFd);
    auto errorCode = setStreamNonBlocking(&this->_stream);
    if (errorCode) {
        LOG(0) << "[MongoStat][Asio][Switch] set stream non blocking is error, code:" << errorCode.message();
        return false;
    }
    errorCode = setStreamNoDelay(&_stream);
    if (errorCode) {
        LOG(0) << "[MongoStat][Asio][Switch] set stream NoDelay is error, code:" << errorCode.message();
        return false;
    }

    errorCode = setStreamKeepAlive(&_stream);
    if (errorCode) {
        LOG(0) << "[MongoStat][Asio][Switch] set stream keepalive is error, code:" << errorCode.message();
        return false;
    }

    if (isOpen()) {
        log() << "[MongoStat][Asio][Switch][fd1:" << static_cast<int>(oldFd) << " => fd2:" << newFd << "] is success";
        return true;
    } else {
        LOG(0) << "[MongoStat][Asio][Switch][fd1:" << static_cast<int>(oldFd) << " => fd2:" << newFd << "] is failure";
    }

    return false;
}

void AsyncStream::connect(tcp::resolver::iterator iter, ConnectHandler&& connectHandler) {
    asio::async_connect(
        _stream,
        std::move(iter),
        // We need to wrap this with a lambda of the right signature so it compiles, even
        // if we don't actually use the resolver iterator.
        _strand->wrap([this, connectHandler](std::error_code ec, tcp::resolver::iterator iter) {
            if (ec) {
                return connectHandler(ec);
            }

            // We assume that our owner is responsible for keeping us alive until we call
            // connectHandler, so _connected should always be a valid memory location.
            ec = setStreamNonBlocking(&_stream);
            if (ec) {
                return connectHandler(ec);
            }

            ec = setStreamNoDelay(&_stream);
            if (ec) {
                return connectHandler(ec);
            }

            ec = setStreamKeepAlive(&_stream);
            if (ec) {
                return connectHandler(ec);
            }

            _connected = true;

            //开关，如果不需要的话就不进行判断
            if (serverGlobalParams.authproxyModel) {
                tcp::endpoint tmp = iter->endpoint();
                try {
                    if (tmp.address().is_v4()) {
                        _realRemoteAddr = SockAddr(tmp.address().to_v4().to_string(), tmp.port());
                    } else {
                        LOG(0) << "i just support ip v4 addr, so i will exit";
                        invariant(false);
                    }
                } catch (...) {
                    LOG(0) << "get real remote addr is error, addr:" << tmp.address().to_string();
                    return connectHandler(make_error_code(ErrorCodes::InvalidRemoteAddr));
                }
            }
            return connectHandler(ec);
        }));
}

void AsyncStream::write(asio::const_buffer buffer, StreamHandler&& streamHandler) {
    writeStream(&_stream, _strand, _connected, buffer, std::move(streamHandler));
}

void AsyncStream::read(asio::mutable_buffer buffer, StreamHandler&& streamHandler) {
    readStream(&_stream, _strand, _connected, buffer, std::move(streamHandler));
}

void AsyncStream::cancel() {
    cancelStream(&_stream);
}

bool AsyncStream::isOpen() {
    return checkIfStreamIsOpen(&_stream, _connected);
}

int64_t AsyncStream::getSocketFd() {
    if (this->isOpen()) {
        return _stream.native_handle();
    } else {
        return -1;
    }
}
}  // namespace executor
}  // namespace mongo
