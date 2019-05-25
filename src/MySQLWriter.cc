#include <vector>
#include <string>
#include <errno.h>
#include <stdlib.h>
#include <regex>
#include "bro-config.h"
#include "MySQLWriter.h"
#include "mysql.bif.h"
#include <iostream>
#include <mysql.h>

using namespace logging;
using namespace writer;
using threading::Field;
using threading::Value;

MySQL::MySQL(WriterFrontend *frontend) : WriterBackend(frontend)
{
    io = unique_ptr<threading::formatter::Ascii>(new threading::formatter::Ascii(this, threading::formatter::Ascii::SeparatorInfo()));
    bytea_instead_text = false;
}

MySQL::~MySQL()
{
    if (&mysql != 0)
    {
        mysql_close(&mysql);
    }
}
// 初始化MYSQL连接
bool MySQL::DoInit(const WriterInfo &info, int num_fields, const threading::Field *const *fields)
{
    hostname = LookupParam(info, "hostname");
    dbname = LookupParam(info, "dbname");
    port = LookupParam(info, "port");
    user = LookupParam(info, "user");
    password = LookupParam(info, "password");

    if (mysql_init(&mysql) == NULL)
    {
        return false;
    }
    if (mysql_real_connect(&mysql, hostname.c_str(), user.c_str(),
                           password.c_str(), dbname.c_str(), atoi(port.c_str()), NULL, 0) == NULL)
    {
        Error(Fmt("Could not connect to mysql %s %s %s %s %s", hostname.c_str(), user.c_str(), password.c_str(), dbname.c_str(), port.c_str()));
        return false;
    }
    if (mysql_set_character_set(&mysql, "utf8") != 0)
    {
        Error(Fmt("Set character %s faild", "utf8"));
        return false;
    }
    table = EscapeIdentifier(info.path);
    if (table.empty())
        return false;
    if (CreateTable(num_fields, fields) && mysql_query(&mysql, createTable.c_str()) != 0)
    {
        Error(Fmt("Create table %s faild.", table.c_str()));
        return false;
    }
    return true;
}
bool MySQL::DoWrite(int num_fields, const threading::Field *const *fields, threading::Value **vals)
{
    string query = "";
    string names = "INSERT INTO `" + table + "` ( ";
    string values("VALUES (");
    for (int i = 0; i < num_fields; ++i)
    {
        string fieldname = EscapeIdentifier(fields[i]->name);
        if (fieldname.empty())
            return false;
        if (i != 0)
        {
            values += ", ";
            names += ", ";
        }

        names += "`" + fieldname + "`";
        values += std::get<1>(CreateParams(vals[i]));
    }
    query = names + ") " + values + ") ;";
    const char *query_ = query.c_str();
    mysql_real_query(&mysql, query_, strlen(query_));
    return true;
}
bool MySQL::DoSetBuf(bool enabled) { return true; }
bool MySQL::DoRotate(const char *rotated_path, double open, double close, bool terminating)
{
    FinishedRotation();
    return true;
}
bool MySQL::DoFlush(double network_time) { return true; }
bool MySQL::DoFinish(double network_time) { return true; }
bool MySQL::DoHeartbeat(double network_time, double current_time) { return true; }

// note - EscapeIdentifier is replicated in reader
string MySQL::EscapeIdentifier(const char *identifier)
{
    char *tStr = new char[strlen(identifier) * 2 + 1];
    mysql_real_escape_string(&mysql, tStr, identifier, strlen(identifier));
    string retStr(tStr);
    delete[] tStr;
    return identifier;
}

std::tuple<bool, string, int> MySQL::CreateParams(const Value *val)
{
    static std::regex curly_re("\\\\|\"");
    string retval;
    if (!val->present)
    {
        switch (val->type)
        {
        case TYPE_BOOL:
            retval = "true";
            break;
        case TYPE_INT:
        case TYPE_COUNT:
        case TYPE_COUNTER:
        case TYPE_PORT:
            retval = "0";
            break;
        case TYPE_SUBNET:
        case TYPE_ADDR:
        case TYPE_TIME:
        case TYPE_INTERVAL:
        case TYPE_DOUBLE:
        case TYPE_ENUM:
        case TYPE_STRING:
        case TYPE_FILE:
        case TYPE_FUNC:
            retval = "''";
            break;
        case TYPE_TABLE:
        case TYPE_VECTOR:
            retval = "''";
            break;
        default:
            retval = "''";
        }
        return std::make_tuple(false, retval, 0);
    }
    int retlength = 0;
    switch (val->type)
    {
    case TYPE_BOOL:
        retval = val->val.int_val ? "true" : "false";
        break;
    case TYPE_INT:
        retval = std::to_string(val->val.int_val);
        break;
    case TYPE_COUNT:
    case TYPE_COUNTER:
        retval = std::to_string(val->val.uint_val);
        break;
    case TYPE_PORT:
        retval = std::to_string(val->val.port_val.port);
        break;
    case TYPE_SUBNET:
        retval = "'" + io->Render(val->val.subnet_val) + "'";
        break;
    case TYPE_ADDR:
        retval = "INET_ATON('" + io->Render(val->val.addr_val) + "')";
        break;
    case TYPE_TIME:
    case TYPE_INTERVAL:
    case TYPE_DOUBLE:
        retval = std::to_string(val->val.double_val);
        break;
    case TYPE_ENUM:
    case TYPE_STRING:
    case TYPE_FILE:
    case TYPE_FUNC:
        retval = "'" + string(val->val.string_val.data, val->val.string_val.length) + "'";
        break;
    case TYPE_TABLE:
    case TYPE_VECTOR:
    {
        bro_int_t size;
        Value **vals;
        string out("{");
        retlength = 1;
        if (val->type == TYPE_TABLE)
        {
            size = val->val.set_val.size;
            vals = val->val.set_val.vals;
        }
        else
        {
            size = val->val.vector_val.size;
            vals = val->val.vector_val.vals;
        }
        if (!size)
            return std::make_tuple(false, string("''"), 0);

        for (int i = 0; i < size; ++i)
        {
            if (i != 0)
                out += ", ";
            auto res = CreateParams(vals[i]);
            if (std::get<0>(res) == false)
            {
                out += "NULL";
                continue;
            }
            string resstr = std::get<1>(res);
            TypeTag type = vals[i]->type;
            // for all numeric types, we do not need escaping
            if (type == TYPE_BOOL || type == TYPE_INT || type == TYPE_COUNT ||
                type == TYPE_COUNTER || type == TYPE_PORT || type == TYPE_TIME ||
                type == TYPE_INTERVAL || type == TYPE_DOUBLE)
                out += resstr;
            else
            {
                //string escaped = std::regex_replace(resstr, curly_re, std::string("\\$&");
                //Error(Fmt("%s-%s",resstr.c_str(), escaped.c_str()));
                resstr = "\"" + resstr.substr(1, resstr.length() - 2) + "\"";
                out += resstr;
                retlength += resstr.length();
            }
        }
        out += "}";
        retlength += 1;
        retval = "'" + out + "'";
        break;
    }

    default:
        Error(Fmt("unsupported field format %d", val->type));
        return std::make_tuple(false, string(), 0);
    }

    if (retlength == 0)
        retlength = retval.length();

    return std::make_tuple(true, retval, retlength);
}

string MySQL::GetTableType(int arg_type, int arg_subtype)
{
    string type;

    switch (arg_type)
    {
    case TYPE_BOOL:
        type = "boolean";
        break;

    case TYPE_INT:
    case TYPE_COUNT:
    case TYPE_COUNTER:
    case TYPE_PORT:
        type = "bigint";
        break;

        /*
        case TYPE_PORT:
            type = "VARCHAR(10)";
            break; */

    case TYPE_SUBNET:
        type = "varchar(20)";
        break;
    case TYPE_ADDR:
        type = "int(10) unsigned";
        break;

    case TYPE_TIME:
    case TYPE_INTERVAL:
    case TYPE_DOUBLE:
        type = "double precision";
        break;

    case TYPE_ENUM:
        type = "TEXT";
        break;

    case TYPE_STRING:
    case TYPE_FILE:
    case TYPE_FUNC:
        type = "TEXT";
        if (bytea_instead_text)
            type = "BYTEA";
        break;

    case TYPE_TABLE:
    case TYPE_VECTOR:
        //type = GetTableType(arg_subtype, 0) + "[]";
        type = "TEXT";
        break;

    default:
        Error(Fmt("unsupported field format %d ", arg_type));
        return string();
    }

    return type;
}

// 读取参数
string MySQL::LookupParam(const WriterInfo &info, const string name) const
{
    map<const char *, const char *>::const_iterator it = info.config.find(name.c_str());
    if (it == info.config.end())
        return string();
    else
        return it->second;
}

// 创建数据表
bool MySQL::CreateTable(int num_fields, const threading::Field *const *fields)
{
    createTable = "CREATE TABLE IF NOT EXISTS " + table + " (\n"
                                                          "id SERIAL UNIQUE NOT NULL";
    for (int i = 0; i < num_fields; ++i)
    {
        const Field *field = fields[i];
        createTable += ",\n";
        string escaped = EscapeIdentifier(field->name);
        if (escaped.empty())
            return false;
        createTable += "`" + escaped + "`";
        createTable += " " + GetTableType(field->type, field->subtype);
    }
    createTable += "\n);";
    return true;
}

