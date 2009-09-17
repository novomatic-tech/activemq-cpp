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

#ifndef _DECAF_CONCURRENT_MUTEX_H_
#define _DECAF_CONCURRENT_MUTEX_H_

#include <decaf/util/concurrent/Synchronizable.h>
#include <decaf/util/concurrent/Concurrent.h>
#include <decaf/lang/Thread.h>
#include <decaf/util/Config.h>

#include <memory>

namespace decaf{
namespace util{
namespace concurrent{

    class MutexProperties;

    /**
     * Creates a pthread_mutex_t object. The object is created
     * such that successive locks from the same thread is allowed
     * and will be successful.
     */
    class DECAF_API Mutex : public Synchronizable {
    private:

        std::auto_ptr<MutexProperties> properties;

    private:

        Mutex( const Mutex& src );
        Mutex& operator= ( const Mutex& src );

    public:

        Mutex();

        virtual ~Mutex();

        virtual void lock() throw( lang::Exception );

        virtual bool tryLock() throw( lang::Exception );

        virtual void unlock() throw( lang::Exception );

        virtual void wait() throw( lang::Exception );

        virtual void wait( long long millisecs ) throw( lang::Exception );

        virtual void wait( long long millisecs, int nanos ) throw( lang::Exception );

        virtual void notify() throw( lang::Exception );

        virtual void notifyAll() throw( lang::Exception );

    };

}}}

#endif /*_DECAF_CONCURRENT_MUTEX_H_*/
