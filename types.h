#pragma once

#include <cstdint>
#include <string>

#if defined(_MSC_VER)
#    define ALWAYS_INLINE __forceinline
#    define NO_INLINE static __declspec(noinline)
#    define MAY_ALIAS
#else
#    define ALWAYS_INLINE __attribute__((__always_inline__))
#    define NO_INLINE __attribute__((__noinline__))
#    define MAY_ALIAS __attribute__((__may_alias__))
#endif

using UInt8 = uint8_t;
using UInt16 = uint16_t;
using UInt32 = uint32_t;
using UInt64 = uint64_t;

using Int8 = int8_t;
using Int16 = int16_t;
using Int32 = int32_t;
using Int64 = int64_t;
using String = std::string;

namespace DB
{

using UInt8 = ::UInt8;
using UInt16 = ::UInt16;
using UInt32 = ::UInt32;
using UInt64 = ::UInt64;

using Int8 = ::Int8;
using Int16 = ::Int16;
using Int32 = ::Int32;
using Int64 = ::Int64;
    

using Float32 = float;
using Float64 = double;

using String = std::string;

class IAST;
using ASTPtr = std::shared_ptr<IAST>;
using ASTs = std::vector<ASTPtr>;

using UUID = __uint128_t;
const UUID Nil{};

enum class IdentifierQuotingStyle
{
    None,            /// Write as-is, without quotes.
    Backticks,       /// `clickhouse` style
    DoubleQuotes,    /// "postgres" style
    BackticksMySQL,  /// `mysql` style, most same as Backticks, but it uses '``' to escape '`'
};

class Exception : public std::exception 
{
public:
    explicit Exception(const std::string & message)
        : message_(message) {}

    // 重写 what() 方法，返回 C 风格字符串
    const char* what() const noexcept override {
        return message_.c_str();
    }

private:
    std::string message_;
};

}