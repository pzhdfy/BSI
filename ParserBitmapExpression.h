#pragma once

#include <string>
#include <IParserBase.h>


namespace DB
{

class ParserBitmapExpression : public IParserBase
{
public:
    bool getBitmapExpression(Pos & pos, ASTPtr & node, Expected & expected);
    bool getCoBitmapExpression(Pos & pos, ASTPtr & node, Expected & expected);
    bool getFilterFromBitmapExpression(Pos & pos, ASTPtr & node, Expected & expected);

protected:
    const char * getName() const override { return "bitmap expression"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

private:
    bool parseBitmapBlockColumn(Pos & pos, ASTPtr & node, Expected & expected);
    bool parseCoBitmapBlockColumn(Pos & pos, ASTPtr & node, Expected & expected);
    bool extractRequiredTag(
        ASTPtr & tag_name_node, String & tag_name_column, ASTPtr & tag_value_node, String & tag_value_column, ASTPtr & required_tag);
    bool extractColumnValue(ASTPtr & node, String & column_name, ASTPtr & column_expression);
    bool extractString(ASTPtr & node, String & string_value);

    bool getBracketBitmapExpression(Pos & pos, ASTPtr & node, Expected & expected);
    bool getBracketCoBitmapExpression(Pos & pos, ASTPtr & node, Expected & expected);
    //bool getBitmapColumnExpression(Pos & pos, ASTPtr & node, Expected & expected);
    bool getBitmapBlockNotExpression(Pos & pos, ASTPtr & node, Expected & expected);
    bool getCoBitmapBlockNotExpression(Pos & pos, ASTPtr & node, Expected & expected);
    bool getTagFilter(Pos & pos, ASTPtr & node, Expected & expected);
};

}
