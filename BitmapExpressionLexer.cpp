#include <BitmapExpressionLexer.h>
//#include <Poco/Logger.h>
#include <types.h>
//#include <Common/StringUtils/StringUtils.h>
#include <find_symbols.h>
#include "StringUtils.h"
//#include <common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

Token BitmapExpressionLexer::nextToken()
{
    Token res = nextTokenImpl();
    if (res.type != TokenType::EndOfStream && max_query_size && res.end > begin + max_query_size)
        res.type = TokenType::ErrorMaxQuerySizeExceeded;
    if (res.isSignificant())
        prev_significant_token_type = res.type;
    return res;
}

Token BitmapExpressionLexer::nextTokenImpl()
{
    if (pos >= end)
        return Token(TokenType::EndOfStream, end, end);

    const char * const token_begin = pos;

    // String tmp_msg(token_begin, end);
    // LOG_DEBUG(&Poco::Logger::get("BitmapExpressionLexer"), "freedomdr cur {}", tmp_msg);
    switch (*pos)
    {
        case ' ':
            [[fallthrough]];
        case '\t':
            [[fallthrough]];
        case '\n':
            [[fallthrough]];
        case '\r':
            [[fallthrough]];
        case '\f':
            [[fallthrough]];
        case '\v': {
            ++pos;
            while (pos < end && isWhitespaceASCII(*pos))
                ++pos;
            return Token(TokenType::Whitespace, token_begin, pos);
        }

        case '(':
            return Token(TokenType::OpeningRoundBracket, token_begin, ++pos);
        case ')':
            return Token(TokenType::ClosingRoundBracket, token_begin, ++pos);
        case '=':
            ++pos;
            return Token(TokenType::Equals, token_begin, pos);
        case '&':
            ++pos;
            return Token(TokenType::BitmapBlockAnd, token_begin, pos);
        case '|':
            ++pos;
            return Token(TokenType::BitmapBlockOr, token_begin, pos);
        case '!':
            ++pos;
            return Token(TokenType::BitmapBlockNot, token_begin, pos);

        default:
            if (prev_significant_token_type == TokenType::Equals)
            {
                while (pos < end)
                {
                    // LOG_DEBUG(&Poco::Logger::get("BitmapExpressionLexer"), "freedomdr pos {}", *pos);
                    if (!isItemEnd())
                        ++pos;
                    else if (isEscapeCharacter())
                        pos += 2;
                    else
                        break;
                }
                if (pos > end)
                {
                    std::string msg = "bitmap expression failed: ";
                    msg.append(token_begin);
                    throw Exception(msg);
                }
                String temp_s(token_begin, pos);
                return Token(TokenType::BareWord, token_begin, pos);
            }
            if (isWordCharASCII(*pos))
            {
                ++pos;
                while (pos < end && isWordCharASCII(*pos))
                    ++pos;
                return Token(TokenType::BareWord, token_begin, pos);
            }
            else
            {
                /// We will also skip unicode whitespaces in UTF-8 to support for queries copy-pasted from MS Word and similar.
                pos = skipWhitespacesUTF8(pos, end);
                if (pos > token_begin)
                    return Token(TokenType::Whitespace, token_begin, pos);
                else
                    return Token(TokenType::Error, token_begin, ++pos);
            }
    }
}

bool BitmapExpressionLexer::isEscapeCharacter()
{
    return *pos == '\\';
}

bool BitmapExpressionLexer::isItemEnd()
{
    switch (*pos)
    {
        case '(':
            [[fallthrough]];
        case ')':
            [[fallthrough]];
        case '=':
            [[fallthrough]];
        case '&':
            [[fallthrough]];
        case '|':
            [[fallthrough]];
        case '!':
            [[fallthrough]];
        case ' ':
            [[fallthrough]];
        case '\t':
            [[fallthrough]];
        case '\n':
            [[fallthrough]];
        case '\r':
            [[fallthrough]];
        case '\f':
            [[fallthrough]];
        case '\\':
            [[fallthrough]];
        case '\v':
            return true;
        default:
            return false;
    }
}

}
