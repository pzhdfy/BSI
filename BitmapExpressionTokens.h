#pragma once

#include <vector>
#include "BitmapExpressionLexer.h"
//#include <Common/typeid_cast.h>


namespace DB
{

class BitmapExpressionTokens
{
private:
    std::vector<Token> data;
    BitmapExpressionLexer lexer;

public:

    BitmapExpressionTokens(const char * begin, const char * end, size_t max_query_size = 0) : lexer(begin, end, max_query_size) { }

    const Token & operator[](size_t index)
    {
        while (true)
        {
            if (index < data.size())
                return data[index];

            if (!data.empty() && data.back().isEnd())
                return data.back();

            Token token = lexer.nextToken();

            if (token.isSignificant())
                data.emplace_back(token);
        }
    }

    const Token & max()
    {
        if (data.empty())
            return (*this)[0];
        return data.back();
    }
};

}
