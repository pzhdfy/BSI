#include <ParserBitmapExpression.h>
#include <ExpressionElementParsers.h>
#include "ASTLiteral.h"
#include "ASTFunction.h"
#include "ASTExpressionList.h"
#include "typeid_cast.h"
#include "ASTIdentifier.h"
//#include <Poco/Logger.h>
//#include <common/logger_useful.h>

namespace DB
{

bool ParserBitmapExpression::parseImpl(Pos & /*pos*/, ASTPtr & /*node*/, Expected & /*expected*/)
{
    return false;
}

bool ParserBitmapExpression::parseBitmapBlockColumn(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserIdentifier identifier;
    ParserLiteral literal;
    ASTPtr tag_name_node;
    if (!(identifier.parse(pos, tag_name_node, expected) || literal.parse(pos, tag_name_node, expected)))
        return false;

    if (pos->type != TokenType::Equals)
        return false;

    ++pos;
    ASTPtr tag_value_node;
    if (!(identifier.parse(pos, tag_value_node, expected) || literal.parse(pos, tag_value_node, expected)))
        return false;

    String tag_name_string_value, tag_value_string_value;
    if (!(extractString(tag_name_node, tag_name_string_value) && extractString(tag_value_node, tag_value_string_value)))
    {
        return false;
    }

    auto tag_name_value = std::make_shared<ASTLiteral>(tag_name_string_value + "+" + tag_value_string_value);
    auto function_node = std::make_shared<ASTFunction>();
    function_node->name = "bitmapBlockGet";
    auto args = std::make_shared<ASTExpressionList>();
    args->children.push_back(tag_name_value);
    function_node->arguments = args;
    node = function_node;

    return true;
}

bool ParserBitmapExpression::parseCoBitmapBlockColumn(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserIdentifier identifier;
    ParserLiteral literal;
    ASTPtr tag_node;
    if (!(identifier.parse(pos, tag_node, expected) || literal.parse(pos, tag_node, expected)))
        return false;

    String tag;
    if (!(extractString(tag_node, tag)))
    {
        return false;
    }

    auto tag_arg = std::make_shared<ASTLiteral>(tag);
    auto function_node = std::make_shared<ASTFunction>();
    function_node->name = "bitmapBlockGet";
    auto args = std::make_shared<ASTExpressionList>();
    args->children.push_back(tag_arg);
    function_node->arguments = args;
    node = function_node;

    return true;
}

bool ParserBitmapExpression::extractString(ASTPtr & node, String & string_value)
{
    auto delEscapecharactor = [](const String & s) -> String {
        // LOG_DEBUG(&Poco::Logger::get("ParserBitmapExpression"), "freedomdr s {}", s);
        String new_s = "";
        for (size_t i = 0; i < s.size(); i++)
        {
            if (s[i] == '\\' && i + 1 < s.size())
                new_s.push_back(s[++i]);
            else
                new_s.push_back(s[i]);
        }
        // LOG_DEBUG(&Poco::Logger::get("ParserBitmapExpression"), "freedomdr new_s {}", new_s);
        return new_s;
    };
    if (auto * identifier = typeid_cast<ASTIdentifier *>(node.get()))
    {
        string_value = delEscapecharactor(identifier->name());
        return true;
    }
    else if (auto * literal = typeid_cast<ASTLiteral *>(node.get()))
    {
        if (literal->value.tryGet(string_value))
            ;
        else if (UInt64 u64; literal->value.tryGet(u64))
            string_value = std::to_string(u64);
        else if (Int64 i64; literal->value.tryGet(i64))
            string_value = std::to_string(i64);
        else
            return false;
        return true;
    }
    else
    {
        return false;
    }
}

bool ParserBitmapExpression::getBitmapExpression(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto function_node = std::make_shared<ASTFunction>();
    auto args = std::make_shared<ASTExpressionList>();
    function_node->arguments = args;
    function_node->children.push_back(args);
    ASTPtr result_expression;
    while (pos->type != TokenType::EndOfStream)
    {
        switch (pos->type)
        {
            case TokenType::ClosingRoundBracket: {
                node = function_node;
                return true;
            }

            case TokenType::OpeningRoundBracket:
                if (!getBracketBitmapExpression(pos, result_expression, expected))
                {
                    return false;
                }
                function_node->arguments->children.push_back(result_expression);
                break;

            case TokenType::BitmapBlockNot:
                //LOG_DEBUG(&Poco::Logger::get("ParserBitmapExpression"), "freedomdr parse !");
                if (!getBitmapBlockNotExpression(pos, result_expression, expected))
                {
                    return false;
                }
                if (function_node->name.empty())
                {
                    function_node->name = "bitmapBlockNot";
                }
                //LOG_DEBUG(&Poco::Logger::get("ParserBitmapExpression"), "freedomdr function name {}", function_node->name);
                function_node->arguments->children.push_back(result_expression);
                break;

            case TokenType::BitmapBlockAnd:
                if (function_node->name.empty())
                {
                    function_node->name = "bitmapBlockAnd";
                }
                else if (function_node->name == "bitmapBlockOr")
                {
                    auto or_node = std::make_shared<ASTFunction>();
                    auto or_args = std::make_shared<ASTExpressionList>();
                    or_node->name = "bitmapBlockAnd";
                    or_node->arguments = or_args;
                    or_node->children.push_back(or_args);
                    or_node->arguments->children.push_back(function_node);
                    function_node = or_node;
                }
                ++pos;
                break;
            case TokenType::BitmapBlockOr:
                if (function_node->name.empty())
                {
                    function_node->name = "bitmapBlockOr";
                }
                else if (function_node->name == "bitmapBlockAnd")
                {
                    auto and_node = std::make_shared<ASTFunction>();
                    auto and_args = std::make_shared<ASTExpressionList>();
                    and_node->name = "bitmapBlockOr";
                    and_node->arguments = and_args;
                    and_node->children.push_back(and_args);
                    and_node->arguments->children.push_back(function_node);
                    function_node = and_node;
                }
                ++pos;
                break;
            default:
                if (!parseBitmapBlockColumn(pos, result_expression, expected))
                {
                    return false;
                }
                function_node->arguments->children.push_back(result_expression);
                break;
        }
    }
    node = function_node;
    return true;
}

bool ParserBitmapExpression::getBracketBitmapExpression(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (pos->type == TokenType::OpeningRoundBracket)
    {
        ++pos;
        if (!getBitmapExpression(pos, node, expected))
        {
            return false;
        }
        if (pos->type != TokenType::ClosingRoundBracket)
        {
            return false;
        }
        ++pos;
        return true;
    }
    else
    {
        return false;
    }
}

bool ParserBitmapExpression::getBitmapBlockNotExpression(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (pos->type == TokenType::BitmapBlockNot)
    {
        ++pos;
        if (!getBitmapExpression(pos, node, expected))
        {
            return false;
        }
        return true;
    }
    else
    {
        return false;
    }
}

bool ParserBitmapExpression::getCoBitmapExpression(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto function_node = std::make_shared<ASTFunction>();
    auto args = std::make_shared<ASTExpressionList>();
    function_node->arguments = args;
    function_node->children.push_back(args);
    ASTPtr result_expression;
    while (pos->type != TokenType::EndOfStream)
    {
        switch (pos->type)
        {
            case TokenType::ClosingRoundBracket: {
                node = function_node;
                return true;
            }

            case TokenType::OpeningRoundBracket:
                if (!getBracketCoBitmapExpression(pos, result_expression, expected))
                {
                    return false;
                }
                function_node->arguments->children.push_back(result_expression);
                break;

            case TokenType::BitmapBlockNot:
                //LOG_DEBUG(&Poco::Logger::get("ParserBitmapExpression"), "freedomdr parse !");
                if (!getCoBitmapBlockNotExpression(pos, result_expression, expected))
                {
                    return false;
                }
                if (function_node->name.empty())
                {
                    function_node->name = "bitmapBlockNot";
                }
                //LOG_DEBUG(&Poco::Logger::get("ParserBitmapExpression"), "freedomdr function name {}", function_node->name);
                function_node->arguments->children.push_back(result_expression);
                break;

            case TokenType::BitmapBlockAnd:
                if (function_node->name.empty())
                {
                    function_node->name = "bitmapBlockAnd";
                }
                else if (function_node->name == "bitmapBlockOr")
                {
                    auto or_node = std::make_shared<ASTFunction>();
                    auto or_args = std::make_shared<ASTExpressionList>();
                    or_node->name = "bitmapBlockAnd";
                    or_node->arguments = or_args;
                    or_node->children.push_back(or_args);
                    or_node->arguments->children.push_back(function_node);
                    function_node = or_node;
                }
                ++pos;
                break;
            case TokenType::BitmapBlockOr:
                if (function_node->name.empty())
                {
                    function_node->name = "bitmapBlockOr";
                }
                else if (function_node->name == "bitmapBlockAnd")
                {
                    auto and_node = std::make_shared<ASTFunction>();
                    auto and_args = std::make_shared<ASTExpressionList>();
                    and_node->name = "bitmapBlockOr";
                    and_node->arguments = and_args;
                    and_node->children.push_back(and_args);
                    and_node->arguments->children.push_back(function_node);
                    function_node = and_node;
                }
                ++pos;
                break;
            default:
                if (!parseCoBitmapBlockColumn(pos, result_expression, expected))
                {
                    return false;
                }
                function_node->arguments->children.push_back(result_expression);
                break;
        }
    }
    node = function_node;
    return true;
}

bool ParserBitmapExpression::getBracketCoBitmapExpression(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (pos->type == TokenType::OpeningRoundBracket)
    {
        ++pos;
        if (!getCoBitmapExpression(pos, node, expected))
        {
            return false;
        }
        if (pos->type != TokenType::ClosingRoundBracket)
        {
            return false;
        }
        ++pos;
        return true;
    }
    else
    {
        return false;
    }
}

bool ParserBitmapExpression::getCoBitmapBlockNotExpression(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (pos->type == TokenType::BitmapBlockNot)
    {
        ++pos;
        if (!getCoBitmapExpression(pos, node, expected))
        {
            return false;
        }
        return true;
    }
    else
    {
        return false;
    }
}

bool ParserBitmapExpression::getFilterFromBitmapExpression(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto tuple_function = std::make_shared<ASTFunction>();
    tuple_function->name = "tuple";
    auto tags_list = std::make_shared<ASTExpressionList>();
    tuple_function->arguments = tags_list;
    tuple_function->children.push_back(tuple_function->arguments);
    ASTPtr result_expression;
    while (pos->type != TokenType::EndOfStream)
    {
        switch (pos->type)
        {
            case TokenType::ClosingRoundBracket:
                [[fallthrough]];
            case TokenType::OpeningRoundBracket:
                [[fallthrough]];
            case TokenType::BitmapBlockNot:
                [[fallthrough]];
            case TokenType::BitmapBlockAnd:
                [[fallthrough]];
            case TokenType::BitmapBlockOr:
                ++pos;
                break;
            default:
                if (!getTagFilter(pos, result_expression, expected))
                    return false;
                tags_list->children.push_back(result_expression);
                break;
        }
    }
    std::sort(tags_list->children.begin(), tags_list->children.end(), [](const DB::ASTPtr & lhs, const DB::ASTPtr & rhs)
    {
        const auto * val_lhs = lhs->as<ASTLiteral>();
        const auto * val_rhs = rhs->as<ASTLiteral>();
        return val_lhs->value.safeGet<String>() < val_rhs->value.safeGet<String>();
    });
    node = tuple_function;
    return true;
}

bool ParserBitmapExpression::getTagFilter(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserIdentifier identifier;
    ParserLiteral literal;
    ASTPtr tag_name_node;
    if (!(identifier.parse(pos, tag_name_node, expected) || literal.parse(pos, tag_name_node, expected)))
        return false;

    if (pos->type != TokenType::Equals)
        return false;

    ++pos;
    ASTPtr tag_value_node;
    if (!(identifier.parse(pos, tag_value_node, expected) || literal.parse(pos, tag_value_node, expected)))
        return false;

    String tag_name_string_value, tag_value_string_value;
    if (!(extractString(tag_name_node, tag_name_string_value) && extractString(tag_value_node, tag_value_string_value)))
    {
        return false;
    }
    node = std::make_shared<ASTLiteral>(tag_name_string_value + "+" + tag_value_string_value);
    return true;
}

}
