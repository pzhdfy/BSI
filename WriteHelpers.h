#pragma once

#include <cstring>
#include <cstdio>
#include <limits>
#include <algorithm>
#include <iterator>
#include <sstream>

#include "find_symbols.h"
#include "extended_types.h"


/** Writes a C-string without creating a temporary object. If the string is a literal, then `strlen` is executed at the compilation stage.
  * Use when the string is a literal.
  */
#define writeCString(s, buf) \
  (buf).write((s), strlen(s))

inline void writeString(const char * data, size_t size, std::ostringstream & buf)
{
    buf.write(data, size);
}

inline void writeString(const std::string & ref, std::ostringstream & buf)
{
    writeString(ref.data(), ref.size(), buf);
}

inline void writeChar(char x, std::ostringstream & buf)
{
    buf.write(&x, 1);
}

/** Will escape quote_character and a list of special characters('\b', '\f', '\n', '\r', '\t', '\0', '\\').
 *   - when escape_quote_with_quote is true, use backslash to escape list of special characters,
 *      and use quote_character to escape quote_character. such as: 'hello''world'
 *   - otherwise use backslash to escape list of special characters and quote_character
 */
template <char quote_character, bool escape_quote_with_quote = false>
void writeAnyEscapedString(const char * begin, const char * end, std::ostringstream & buf)
{
    const char * pos = begin;
    while (true)
    {
        /// On purpose we will escape more characters than minimally necessary.
        const char * next_pos = find_first_symbols<'\b', '\f', '\n', '\r', '\t', '\0', '\\', quote_character>(pos, end);

        if (next_pos == end)
        {
            buf.write(pos, next_pos - pos);
            break;
        }
        else
        {
            buf.write(pos, next_pos - pos);
            pos = next_pos;
            switch (*pos)
            {
                case '\b':
                    writeChar('\\', buf);
                    writeChar('b', buf);
                    break;
                case '\f':
                    writeChar('\\', buf);
                    writeChar('f', buf);
                    break;
                case '\n':
                    writeChar('\\', buf);
                    writeChar('n', buf);
                    break;
                case '\r':
                    writeChar('\\', buf);
                    writeChar('r', buf);
                    break;
                case '\t':
                    writeChar('\\', buf);
                    writeChar('t', buf);
                    break;
                case '\0':
                    writeChar('\\', buf);
                    writeChar('0', buf);
                    break;
                case '\\':
                    writeChar('\\', buf);
                    writeChar('\\', buf);
                    break;
                case quote_character:
                {
                    if constexpr (escape_quote_with_quote)
                        writeChar(quote_character, buf);
                    else
                        writeChar('\\', buf);
                    writeChar(quote_character, buf);
                    break;
                }
                default:
                    writeChar(*pos, buf);
            }
            ++pos;
        }
    }
}

template <char quote_character>
void writeAnyQuotedString(const char * begin, const char * end, std::ostringstream & buf)
{
    writeChar(quote_character, buf);
    writeAnyEscapedString<quote_character>(begin, end, buf);
    writeChar(quote_character, buf);
}

template <char quote_character>
void writeAnyQuotedString(const std::string & ref,  std::ostringstream & buf)
{
    writeAnyQuotedString<quote_character>(ref.data(), ref.data() + ref.size(), buf);
}

inline void writeBackQuotedString(const std::string & s, std::ostringstream & buf)
{
    writeAnyQuotedString<'`'>(s, buf);
}




/// Write quoted if the string doesn't look like and identifier.
void writeProbablyBackQuotedString(const std::string & s, std::ostringstream & buf);
void writeProbablyDoubleQuotedString(const std::string & s, std::ostringstream & buf);
void writeProbablyBackQuotedStringMySQL(const std::string & s, std::ostringstream & buf);

inline void writeDoubleQuotedString(const std::string & s, std::ostringstream & buf)
{
    writeAnyQuotedString<'"'>(s, buf);
}

/// Outputs a string in backquotes for MySQL.
inline void writeBackQuotedStringMySQL(const std::string & s, std::ostringstream & buf)
{
    writeChar('`', buf);
    writeAnyEscapedString<'`', true>(s.data(), s.data() + s.size(), buf);
    writeChar('`', buf);
}

void writePointerHex(const void * ptr, std::ostringstream & buf);


/// Methods for outputting the value in text form for a tab-separated format.
template <typename T>
inline std::enable_if_t<is_integer_v<T>, void>
writeText(const T & x, std::ostringstream & buf) { buf<<x; }

template <typename T>
inline std::enable_if_t<std::is_floating_point_v<T>, void>
writeText(const T & x, std::ostringstream & buf) { buf<<x; }

inline void writeText(const String & x, std::ostringstream & buf) { writeString(x.c_str(), x.size(), buf); }

inline void writeBoolText(bool x, std::ostringstream & buf)
{
    writeChar(x ? '1' : '0', buf);
}

/// Implemented as template specialization (not function overload) to avoid preference over templates on arithmetic types above.
template <> inline void writeText<bool>(const bool & x, std::ostringstream & buf) { writeBoolText(x, buf); }

/// unlike the method for std::string
/// assumes here that `x` is a null-terminated string.
inline void writeText(const char * x, std::ostringstream & buf) { writeCString(x, buf); }
inline void writeText(const char * x, size_t size, std::ostringstream & buf) { writeString(x, size, buf); }


/// String, date, datetime are in single quotes with C-style escaping. Numbers - without.
template <typename T>
inline std::enable_if_t<is_arithmetic_v<T>, void>
writeQuoted(const T & x, std::ostringstream & buf) { writeText(x, buf); }

inline void writeQuotedString(const String & s, std::ostringstream & buf)
{
    writeAnyQuotedString<'\''>(s, buf);
}

inline void writeQuoted(const String & x, std::ostringstream & buf) { writeQuotedString(x, buf); }



/// String, date, datetime are in double quotes with C-style escaping. Numbers - without.
template <typename T>
inline std::enable_if_t<is_arithmetic_v<T>, void>
writeDoubleQuoted(const T & x, std::ostringstream & buf) { writeText(x, buf); }

inline void writeDoubleQuoted(const String & x, std::ostringstream & buf) { writeDoubleQuotedString(x, buf); }

//inline void writeDoubleQuoted(const std::string_view & x, std::ostringstream & buf) { writeDoubleQuotedString(x, buf); }

/// An easy-to-use method for converting something to a string in text form.
template <typename T>
inline String toString(const T & x)
{
    std::ostringstream buf;
    writeText(x, buf);
    return buf.str();
}