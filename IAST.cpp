//#include <IO/WriteBufferFromString.h>
#include <WriteHelpers.h>
//#include <IO/Operators.h>
#include <SipHash.h>
#include <IAST.h>
#include <string>


namespace DB
{

const char * IAST::hilite_keyword      = "\033[1m";
const char * IAST::hilite_identifier   = "\033[0;36m";
const char * IAST::hilite_function     = "\033[0;33m";
const char * IAST::hilite_operator     = "\033[1;33m";
const char * IAST::hilite_alias        = "\033[0;32m";
const char * IAST::hilite_substitution = "\033[1;36m";
const char * IAST::hilite_none         = "\033[0m";


size_t IAST::size() const
{
    size_t res = 1;
    for (const auto & child : children)
        res += child->size();

    return res;
}

size_t IAST::checkSize(size_t max_size) const
{
    size_t res = 1;
    for (const auto & child : children)
        res += child->checkSize(max_size);

    if (res > max_size)
        throw Exception("AST is too big. Maximum: " + std::to_string(max_size));

    return res;
}


IAST::Hash IAST::getTreeHash() const
{
    SipHash hash_state;
    updateTreeHash(hash_state);
    IAST::Hash res;
    hash_state.get128(res);
    return res;
}


void IAST::updateTreeHash(SipHash & hash_state) const
{
    updateTreeHashImpl(hash_state);
    hash_state.update(children.size());
    for (const auto & child : children)
        child->updateTreeHash(hash_state);
}


void IAST::updateTreeHashImpl(SipHash & hash_state) const
{
    auto id = getID();
    hash_state.update(id.data(), id.size());
}


size_t IAST::checkDepthImpl(size_t max_depth, size_t level) const
{
    size_t res = level + 1;
    for (const auto & child : children)
    {
        if (level >= max_depth)
            throw Exception("AST is too deep. Maximum: " + std::to_string(max_depth));
        res = std::max(res, child->checkDepthImpl(max_depth, level + 1));
    }

    return res;
}

std::string IAST::formatForErrorMessage() const
{
    std::ostringstream buf;
    format(FormatSettings(buf, true /* one line */));
    return buf.str();
}

void IAST::cloneChildren()
{
    for (auto & child : children)
        child = child->clone();
}


String IAST::getColumnName() const
{
    std::ostringstream write_buffer;
    appendColumnName(write_buffer);
    return write_buffer.str();
}


String IAST::getColumnNameWithoutAlias() const
{
    std::ostringstream write_buffer;
    appendColumnNameWithoutAlias(write_buffer);
    return write_buffer.str();
}


void IAST::FormatSettings::writeIdentifier(const String & name) const
{
    switch (identifier_quoting_style)
    {
        case IdentifierQuotingStyle::None:
        {
            if (always_quote_identifiers)
                throw Exception("Incompatible arguments: always_quote_identifiers = true && identifier_quoting_style == IdentifierQuotingStyle::None");
            writeString(name, ostr);
            break;
        }
        case IdentifierQuotingStyle::Backticks:
        {
            if (always_quote_identifiers)
                writeBackQuotedString(name, ostr);
            else
                writeProbablyBackQuotedString(name, ostr);
            break;
        }
        case IdentifierQuotingStyle::DoubleQuotes:
        {
            if (always_quote_identifiers)
                writeDoubleQuotedString(name, ostr);
            else
                writeProbablyDoubleQuotedString(name, ostr);
            break;
        }
        case IdentifierQuotingStyle::BackticksMySQL:
        {
            if (always_quote_identifiers)
                writeBackQuotedStringMySQL(name, ostr);
            else
                writeProbablyBackQuotedStringMySQL(name, ostr);
            break;
        }
    }
}

void IAST::dumpTree(std::ostringstream & ostr, size_t indent) const
{
    String indent_str(indent, '-');
    ostr << indent_str << getID() << ", ";
    writePointerHex(this, ostr);
    writeChar('\n', ostr);
    for (const auto & child : children)
    {
        if (!child) throw Exception("Can't dump nullptr child");
        child->dumpTree(ostr, indent + 1);
    }
}

std::string IAST::dumpTree(size_t indent) const
{
    std::ostringstream wb;
    dumpTree(wb, indent);
    return wb.str();
}

}
