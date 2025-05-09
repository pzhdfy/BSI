#include <WriteHelpers.h>
#include "StringUtils.h"
#include "hex.h"


/// The same, but quotes apply only if there are characters that do not match the identifier without quotes
template <typename F>
static inline void writeProbablyQuotedStringImpl(const std::string & s, std::ostringstream & buf, F && write_quoted_string)
{
    if (isValidIdentifier(std::string_view{s}))
        writeString(s, buf);
    else
        write_quoted_string(s, buf);
}


void writeProbablyBackQuotedString(const std::string & s, std::ostringstream & buf)
{
    writeProbablyQuotedStringImpl(s, buf, [](const std::string & s_, std::ostringstream & buf_) { return writeBackQuotedString(s_, buf_); });
}


void writeProbablyDoubleQuotedString(const std::string & s, std::ostringstream & buf)
{
    writeProbablyQuotedStringImpl(s, buf, [](const std::string & s_, std::ostringstream & buf_) { return writeDoubleQuotedString(s_, buf_); });
}

void writeProbablyBackQuotedStringMySQL(const std::string & s, std::ostringstream & buf)
{
    writeProbablyQuotedStringImpl(s, buf, [](const std::string & s_, std::ostringstream & buf_) { return writeBackQuotedStringMySQL(s_, buf_); });
}

void writePointerHex(const void * ptr, std::ostringstream & buf)
{
    writeString("0x", buf);
    char hex_str[2 * sizeof(ptr)];
    writeHexUIntLowercase(reinterpret_cast<uintptr_t>(ptr), hex_str);
    buf.write(hex_str, 2 * sizeof(ptr));
}