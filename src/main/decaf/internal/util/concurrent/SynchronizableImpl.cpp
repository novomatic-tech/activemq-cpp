/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "SynchronizableImpl.h"

using namespace decaf;
using namespace decaf::internal;
using namespace decaf::internal::util;
using namespace decaf::internal::util::concurrent;

////////////////////////////////////////////////////////////////////////////////
SynchronizableImpl::SynchronizableImpl() {}

////////////////////////////////////////////////////////////////////////////////
SynchronizableImpl::~SynchronizableImpl() {}

////////////////////////////////////////////////////////////////////////////////
void SynchronizableImpl::lock() throw( lang::Exception ) {
    mutex.lock();
}

////////////////////////////////////////////////////////////////////////////////
void SynchronizableImpl::unlock() throw( lang::Exception ) {
    mutex.unlock();
}

////////////////////////////////////////////////////////////////////////////////
void SynchronizableImpl::wait() throw( lang::Exception ) {
    mutex.wait();
}

////////////////////////////////////////////////////////////////////////////////
void SynchronizableImpl::wait( long long millisecs ) throw( lang::Exception ) {
    mutex.wait( millisecs );
}

////////////////////////////////////////////////////////////////////////////////
void SynchronizableImpl::wait( long long millisecs, int nanos ) throw( lang::Exception ) {
    mutex.wait( millisecs, nanos );
}

////////////////////////////////////////////////////////////////////////////////
void SynchronizableImpl::notify() throw( lang::Exception ) {
    mutex.notify();
}

////////////////////////////////////////////////////////////////////////////////
void SynchronizableImpl::notifyAll() throw( lang::Exception ) {
    mutex.notifyAll();
}
