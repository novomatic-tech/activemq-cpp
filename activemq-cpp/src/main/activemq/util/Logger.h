#ifndef _ACTIVEMQ_UTIL_LOGGERFACTORY_H_
#define _ACTIVEMQ_UTIL_LOGGERFACTORY_H_

#include <activemq/util/Config.h>
#include <decaf/lang/Pointer.h>
#include <Engine/runtime/cpp/src/LBLogger.h>
#include <Engine/runtime/cpp/src/LBLogger_Wrapper.h>
#include <vector>
#include <string>

namespace activemq {
namespace util {

    class AMQCPP_API Logger {
    public:
        static void initialize(LightBridge::Middleware::Logger::ILogger* logger);

        static void log(const std::string& entry, LightBridge::Middleware::Logger::Severity severity, const std::string& category);
        static void log(const std::string& entry, LightBridge::Middleware::Logger::Severity severity, const std::vector<std::string>& categories);

        /*
         * Low-level tracing methods.
         */
        static void trace(const std::string& entry, const std::string& category);
        static void trace(const std::string& entry, const std::vector<std::string>& categories);

        /*
         * Tracing of network Input/Output.
         *
         * Logging all communication can result in enormous amount of logs.
         * Separate method is used, to allow fine-grained tracing.
         */
        static void traceIO(const std::string& entry, const std::vector<std::string>& categories);
    private:
        static decaf::lang::Pointer<LightBridge::Middleware::Logger::LBLogger> externalLogger;
        static bool tracingEnabled;
        static bool IOtracingEnabled;
    };

}}


#endif //_ACTIVEMQ_UTIL_LOGGERFACTORY_H_
