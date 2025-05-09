#pragma once

#include <stddef.h>
#include "Lexer.h"


namespace DB
{

class BitmapExpressionLexer
{
public:
    BitmapExpressionLexer(const char * begin_, const char * end_, size_t max_query_size_ = 0)
        : begin(begin_), pos(begin_), end(end_), max_query_size(max_query_size_)
    {
    }
    Token nextToken();

private:
    const char * const begin;
    const char * pos;
    const char * const end;

    const size_t max_query_size;

    Token nextTokenImpl();

    /// This is needed to disambiguate tuple access operator from floating point number (.1).
    TokenType prev_significant_token_type = TokenType::Whitespace; /// No previous token.
    bool isItemEnd();
    bool isEscapeCharacter();
};

}
