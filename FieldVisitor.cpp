//#include <IO/ReadBuffer.h>
//#include <IO/WriteBuffer.h>
#include "types.h"
#include <WriteHelpers.h>
//#include <IO/WriteBufferFromString.h>
//#include <IO/Operators.h>
#include <FieldVisitors.h>
#include <SipHash.h>
#include <sstream>


namespace DB
{

template <typename T>
static inline String formatQuoted(T x)
{
    std::ostringstream wb;
    writeQuoted(x, wb);
    return wb.str();
}

template <typename T>
static inline String formatDoubleQuoted(T x)
{
    std::ostringstream wb;
    writeDoubleQuoted(x, wb);
    return wb.str();
}

template <typename T>
static inline String formatQuotedWithPrefix(T x, const char * prefix)
{
    std::ostringstream wb;
    writeCString(prefix, wb);
    writeQuoted(x, wb);
    return wb.str();
}

// template <typename T>
// static inline void writeQuoted(const DecimalField<T> & x, WriteBuffer & buf)
// {
//     writeChar('\'', buf);
//     writeText(x.getValue(), x.getScale(), buf);
//     writeChar('\'', buf);
// }

// String FieldVisitorDump::operator() (const Null &) const { return "NULL"; }
// String FieldVisitorDump::operator() (const NegativeInfinity &) const { return "-Inf"; }
// String FieldVisitorDump::operator() (const PositiveInfinity &) const { return "+Inf"; }
String FieldVisitorDump::operator() (const UInt64 & x) const { return formatQuotedWithPrefix(x, "UInt64_"); }
String FieldVisitorDump::operator() (const Int64 & x) const { return formatQuotedWithPrefix(x, "Int64_"); }
String FieldVisitorDump::operator() (const Float64 & x) const { return formatQuotedWithPrefix(x, "Float64_"); }
// String FieldVisitorDump::operator() (const DecimalField<Decimal32> & x) const { return formatQuotedWithPrefix(x, "Decimal32_"); }
// String FieldVisitorDump::operator() (const DecimalField<Decimal64> & x) const { return formatQuotedWithPrefix(x, "Decimal64_"); }
// String FieldVisitorDump::operator() (const DecimalField<Decimal128> & x) const { return formatQuotedWithPrefix(x, "Decimal128_"); }
// String FieldVisitorDump::operator() (const DecimalField<Decimal256> & x) const { return formatQuotedWithPrefix(x, "Decimal256_"); }
// String FieldVisitorDump::operator() (const UInt128 & x) const { return formatQuotedWithPrefix(x, "UInt128_"); }
// String FieldVisitorDump::operator() (const UInt256 & x) const { return formatQuotedWithPrefix(x, "UInt256_"); }
// String FieldVisitorDump::operator() (const Int128 & x) const { return formatQuotedWithPrefix(x, "Int128_"); }
// String FieldVisitorDump::operator() (const Int256 & x) const { return formatQuotedWithPrefix(x, "Int256_"); }
// String FieldVisitorDump::operator() (const UUID & x) const { return formatQuotedWithPrefix(x, "UUID_"); }


String FieldVisitorDump::operator() (const String & x) const
{
    std::ostringstream wb;
    writeQuoted(x, wb);
    return wb.str();
}

String FieldVisitorDump::operator() (const Array & x) const
{
    std::ostringstream wb;

    wb << "Array_[";
    for (auto it = x.begin(); it != x.end(); ++it)
    {
        if (it != x.begin())
            wb << ", ";
        wb << applyVisitor(*this, *it);
    }
    wb << ']';

    return wb.str();
}

String FieldVisitorDump::operator() (const Tuple & x) const
{
    std::ostringstream wb;

    wb << "Tuple_(";
    for (auto it = x.begin(); it != x.end(); ++it)
    {
        if (it != x.begin())
            wb << ", ";
        wb << applyVisitor(*this, *it);
    }
    wb << ')';

    return wb.str();
}

String FieldVisitorDump::operator() (const Map & x) const
{
    std::ostringstream wb;

    wb << "Map_(";
    for (auto it = x.begin(); it != x.end(); ++it)
    {
        if (it != x.begin())
            wb << ", ";
        wb << applyVisitor(*this, *it);
    }
    wb << ')';

    return wb.str();
}

String FieldVisitorDump::operator() (const AggregateFunctionStateData & x) const
{
    std::ostringstream wb;
    wb << "AggregateFunctionState_(";
    writeQuoted(x.name, wb);
    wb << ", ";
    writeQuoted(x.data, wb);
    wb << ')';
    return wb.str();
}

/** In contrast to writeFloatText (and writeQuoted),
  *  even if number looks like integer after formatting, prints decimal point nevertheless (for example, Float64(1) is printed as 1.).
  * - because resulting text must be able to be parsed back as Float64 by query parser (otherwise it will be parsed as integer).
  *
  * Trailing zeros after decimal point are omitted.
  *
  * NOTE: Roundtrip may lead to loss of precision.
  */
static String formatFloat(const Float64 x)
{
//     // DoubleConverter<true>::BufferType buffer;
//     // double_conversion::StringBuilder builder{buffer, sizeof(buffer)};

//     // const auto result = DoubleConverter<true>::instance().ToShortest(x, &builder);

//     // if (!result)
//     //     throw Exception("Cannot print float or double number", ErrorCodes::CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER);

//     // return { buffer, buffer + builder.position() };
    throw Exception("not implemented");
 }


// String FieldVisitorToString::operator() (const Null &) const { return "NULL"; }
// String FieldVisitorToString::operator() (const NegativeInfinity &) const { return "-Inf"; }
// String FieldVisitorToString::operator() (const PositiveInfinity &) const { return "+Inf"; }
String FieldVisitorToString::operator() (const UInt64 & x) const { return formatQuoted(x); }
String FieldVisitorToString::operator() (const Int64 & x) const { return formatQuoted(x); }
String FieldVisitorToString::operator() (const Float64 & x) const { return formatFloat(x); }
String FieldVisitorToString::operator() (const String & x) const { return formatQuoted(x); }
// String FieldVisitorToString::operator() (const DecimalField<Decimal32> & x) const { return formatQuoted(x); }
// String FieldVisitorToString::operator() (const DecimalField<Decimal64> & x) const { return formatQuoted(x); }
// String FieldVisitorToString::operator() (const DecimalField<Decimal128> & x) const { return formatQuoted(x); }
// String FieldVisitorToString::operator() (const DecimalField<Decimal256> & x) const { return formatQuoted(x); }
// String FieldVisitorToString::operator() (const Int128 & x) const { return formatQuoted(x); }
// String FieldVisitorToString::operator() (const UInt128 & x) const { return formatQuoted(x); }
// String FieldVisitorToString::operator() (const UInt256 & x) const { return formatQuoted(x); }
// String FieldVisitorToString::operator() (const Int256 & x) const { return formatQuoted(x); }
// String FieldVisitorToString::operator() (const UUID & x) const { return formatQuoted(x); }
String FieldVisitorToString::operator() (const AggregateFunctionStateData & x) const { return formatQuoted(x.data); }

String FieldVisitorToString::operator() (const Array & x) const
{
    std::ostringstream wb;

    wb << '[';
    for (Array::const_iterator it = x.begin(); it != x.end(); ++it)
    {
        if (it != x.begin())
            wb.write(", ", 2);
        wb << applyVisitor(*this, *it);
    }
    wb << ']';

    return wb.str();
}

String FieldVisitorToString::operator() (const Tuple & x) const
{
    std::ostringstream wb;

    // For single-element tuples we must use the explicit tuple() function,
    // or they will be parsed back as plain literals.
    if (x.size() > 1)
    {
        wb << '(';
    }
    else
    {
        wb << "tuple(";
    }

    for (auto it = x.begin(); it != x.end(); ++it)
    {
        if (it != x.begin())
            wb << ", ";
        wb << applyVisitor(*this, *it);
    }
    wb << ')';

    return wb.str();
}

String FieldVisitorToString::operator() (const Map & x) const
{
    std::ostringstream wb;

    wb << '(';
    for (auto it = x.begin(); it != x.end(); ++it)
    {
        if (it != x.begin())
            wb << ", ";
        wb << applyVisitor(*this, *it);
    }
    wb << ')';

    return wb.str();
}


// String FieldVisitorToJSONString::operator() (const Null &) const { return "null"; }
// String FieldVisitorToJSONString::operator() (const NegativeInfinity &) const { return "-Inf"; }
// String FieldVisitorToJSONString::operator() (const PositiveInfinity &) const { return "+Inf"; }
// String FieldVisitorToJSONString::operator() (const UInt64 & x) const { return toString(x); }
// String FieldVisitorToJSONString::operator() (const Int64 & x) const { return toString(x); }
// String FieldVisitorToJSONString::operator() (const Float64 & x) const
// {
//     std::ostringstream buf;
//     //writeFloatText(x, buf);
//     throw Exception("not implemented");
//     return buf.str();
// }
// String FieldVisitorToJSONString::operator() (const String & x) const { return formatDoubleQuoted(x); }
// String FieldVisitorToJSONString::operator() (const DecimalField<Decimal32> & x) const { return toString(x); }
// String FieldVisitorToJSONString::operator() (const DecimalField<Decimal64> & x) const { return toString(x); }
// String FieldVisitorToJSONString::operator() (const DecimalField<Decimal128> & x) const { return toString(x); }
// String FieldVisitorToJSONString::operator() (const DecimalField<Decimal256> & x) const { return toString(x); }
// String FieldVisitorToJSONString::operator() (const Int128 & x) const { return toString(x); }
// String FieldVisitorToJSONString::operator() (const UInt128 & x) const { return toString(x); }
// String FieldVisitorToJSONString::operator() (const UInt256 & x) const { return toString(x); }
// String FieldVisitorToJSONString::operator() (const Int256 & x) const { return toString(x); }
// String FieldVisitorToJSONString::operator() (const UUID & x) const { return toString(x); }
// String FieldVisitorToJSONString::operator() (const AggregateFunctionStateData & x) const { return formatDoubleQuoted(x.data); }

// String FieldVisitorToJSONString::operator() (const Array & x) const
// {
//     std::ostringstream wb;

//     wb << '[';
//     for (Array::const_iterator it = x.begin(); it != x.end(); ++it)
//     {
//         if (it != x.begin())
//             wb.write(", ", 2);
//         wb << applyVisitor(*this, *it);
//     }
//     wb << ']';

//     return wb.str();
// }

// String FieldVisitorToJSONString::operator() (const Tuple & x) const
// {
//     std::ostringstream wb;

//     wb << '[';
//     for (auto it = x.begin(); it != x.end(); ++it)
//     {
//         if (it != x.begin())
//             wb << ", ";
//         wb << applyVisitor(*this, *it);
//     }
//     wb << ']';

//     return wb.str();
// }

// String FieldVisitorToJSONString::operator() (const Map & x) const
// {
//     std::ostringstream wb;

//     wb << '[';
//     for (auto it = x.begin(); it != x.end(); ++it)
//     {
//         if (it != x.begin())
//             wb << ", ";
//         wb << applyVisitor(*this, *it);
//     }
//     wb << ']';

//     return wb.str();
// }


// void FieldVisitorWriteBinary::operator() (const Null &, WriteBuffer &) const { }
// void FieldVisitorWriteBinary::operator() (const NegativeInfinity &, WriteBuffer &) const { }
// void FieldVisitorWriteBinary::operator() (const PositiveInfinity &, WriteBuffer &) const { }
// void FieldVisitorWriteBinary::operator() (const UInt64 & x, WriteBuffer & buf) const { throw Exception("not implement");}//{ DB::writeVarUInt(x, buf); }
// void FieldVisitorWriteBinary::operator() (const Int64 & x, WriteBuffer & buf) const  { throw Exception("not implement");}//{ DB::writeVarInt(x, buf); }
// void FieldVisitorWriteBinary::operator() (const Float64 & x, WriteBuffer & buf) const { throw Exception("not implement");}//{ DB::writeFloatBinary(x, buf); }
// void FieldVisitorWriteBinary::operator() (const String & x, WriteBuffer & buf) const { throw Exception("not implement");}//{ DB::writeStringBinary(x, buf); }
// void FieldVisitorWriteBinary::operator() (const UInt128 & x, WriteBuffer & buf) const { DB::writeBinary(x, buf); }
// void FieldVisitorWriteBinary::operator() (const Int128 & x, WriteBuffer & buf) const { DB::writeVarInt(x, buf); }
// void FieldVisitorWriteBinary::operator() (const UInt256 & x, WriteBuffer & buf) const { DB::writeBinary(x, buf); }
// void FieldVisitorWriteBinary::operator() (const Int256 & x, WriteBuffer & buf) const { DB::writeBinary(x, buf); }
// void FieldVisitorWriteBinary::operator() (const UUID & x, WriteBuffer & buf) const { DB::writeBinary(x, buf); }
// void FieldVisitorWriteBinary::operator() (const DecimalField<Decimal32> & x, WriteBuffer & buf) const { DB::writeBinary(x.getValue(), buf); }
// void FieldVisitorWriteBinary::operator() (const DecimalField<Decimal64> & x, WriteBuffer & buf) const { DB::writeBinary(x.getValue(), buf); }
// void FieldVisitorWriteBinary::operator() (const DecimalField<Decimal128> & x, WriteBuffer & buf) const { DB::writeBinary(x.getValue(), buf); }
// void FieldVisitorWriteBinary::operator() (const DecimalField<Decimal256> & x, WriteBuffer & buf) const { DB::writeBinary(x.getValue(), buf); }
// void FieldVisitorWriteBinary::operator() (const AggregateFunctionStateData & x, WriteBuffer & buf) const
// {
//     throw Exception("not implement");
//     //DB::writeStringBinary(x.name, buf);
//     //DB::writeStringBinary(x.data, buf);
// }

// void FieldVisitorWriteBinary::operator() (const Array & x, WriteBuffer & buf) const
// {
//     const size_t size = x.size();
//     DB::writeBinary(size, buf);

//     for (size_t i = 0; i < size; ++i)
//     {
//         const UInt8 type = x[i].getType();
//         DB::writeBinary(type, buf);
//         Field::dispatch([&buf] (const auto & value) { DB::FieldVisitorWriteBinary()(value, buf); }, x[i]);
//     }
// }

// void FieldVisitorWriteBinary::operator() (const Tuple & x, WriteBuffer & buf) const
// {
//     const size_t size = x.size();
//     DB::writeBinary(size, buf);

//     for (size_t i = 0; i < size; ++i)
//     {
//         const UInt8 type = x[i].getType();
//         DB::writeBinary(type, buf);
//         Field::dispatch([&buf] (const auto & value) { DB::FieldVisitorWriteBinary()(value, buf); }, x[i]);
//     }
// }


// void FieldVisitorWriteBinary::operator() (const Map & x, WriteBuffer & buf) const
// {
//     const size_t size = x.size();
//     DB::writeBinary(size, buf);

//     for (size_t i = 0; i < size; ++i)
//     {
//         const UInt8 type = x[i].getType();
//         writeBinary(type, buf);
//         Field::dispatch([&buf] (const auto & value) { DB::FieldVisitorWriteBinary()(value, buf); }, x[i]);
//     }
// }

FieldVisitorHash::FieldVisitorHash(SipHash & hash_) : hash(hash_) {}

// void FieldVisitorHash::operator() (const Null &) const
// {
//     UInt8 type = Field::Types::Null;
//     hash.update(type);
// }

// void FieldVisitorHash::operator() (const NegativeInfinity &) const
// {
//     UInt8 type = Field::Types::NegativeInfinity;
//     hash.update(type);
// }

// void FieldVisitorHash::operator() (const PositiveInfinity &) const
// {
//     UInt8 type = Field::Types::PositiveInfinity;
//     hash.update(type);
// }

void FieldVisitorHash::operator() (const UInt64 & x) const
{
    UInt8 type = Field::Types::UInt64;
    hash.update(type);
    hash.update(x);
}

// void FieldVisitorHash::operator() (const UInt128 & x) const
// {
//     UInt8 type = Field::Types::UInt128;
//     hash.update(type);
//     hash.update(x);
// }

void FieldVisitorHash::operator() (const Int64 & x) const
{
    UInt8 type = Field::Types::Int64;
    hash.update(type);
    hash.update(x);
}

// void FieldVisitorHash::operator() (const Int128 & x) const
// {
//     UInt8 type = Field::Types::Int128;
//     hash.update(type);
//     hash.update(x);
// }

// void FieldVisitorHash::operator() (const UUID & x) const
// {
//     UInt8 type = Field::Types::UUID;
//     hash.update(type);
//     hash.update(x);
// }

void FieldVisitorHash::operator() (const Float64 & x) const
{
    UInt8 type = Field::Types::Float64;
    hash.update(type);
    hash.update(x);
}

void FieldVisitorHash::operator() (const String & x) const
{
    UInt8 type = Field::Types::String;
    hash.update(type);
    hash.update(x.size());
    hash.update(x.data(), x.size());
}

void FieldVisitorHash::operator() (const Tuple & x) const
{
    UInt8 type = Field::Types::Tuple;
    hash.update(type);
    hash.update(x.size());

    for (const auto & elem : x)
        applyVisitor(*this, elem);
}

void FieldVisitorHash::operator() (const Map & x) const
{
    UInt8 type = Field::Types::Map;
    hash.update(type);
    hash.update(x.size());

    for (const auto & elem : x)
        applyVisitor(*this, elem);
}

void FieldVisitorHash::operator() (const Array & x) const
{
    UInt8 type = Field::Types::Array;
    hash.update(type);
    hash.update(x.size());

    for (const auto & elem : x)
        applyVisitor(*this, elem);
}

// void FieldVisitorHash::operator() (const DecimalField<Decimal32> & x) const
// {
//     UInt8 type = Field::Types::Decimal32;
//     hash.update(type);
//     hash.update(x.getValue().value);
// }

// void FieldVisitorHash::operator() (const DecimalField<Decimal64> & x) const
// {
//     UInt8 type = Field::Types::Decimal64;
//     hash.update(type);
//     hash.update(x.getValue().value);
// }

// void FieldVisitorHash::operator() (const DecimalField<Decimal128> & x) const
// {
//     UInt8 type = Field::Types::Decimal128;
//     hash.update(type);
//     hash.update(x.getValue().value);
// }

// void FieldVisitorHash::operator() (const DecimalField<Decimal256> & x) const
// {
//     UInt8 type = Field::Types::Decimal256;
//     hash.update(type);
//     hash.update(x.getValue().value);
// }

void FieldVisitorHash::operator() (const AggregateFunctionStateData & x) const
{
    UInt8 type = Field::Types::AggregateFunctionState;
    hash.update(type);
    hash.update(x.name.size());
    hash.update(x.name.data(), x.name.size());
    hash.update(x.data.size());
    hash.update(x.data.data(), x.data.size());
}

// void FieldVisitorHash::operator() (const UInt256 & x) const
// {
//     UInt8 type = Field::Types::UInt256;
//     hash.update(type);
//     hash.update(x);
// }

// void FieldVisitorHash::operator() (const Int256 & x) const
// {
//     UInt8 type = Field::Types::Int256;
//     hash.update(type);
//     hash.update(x);
// }

}
