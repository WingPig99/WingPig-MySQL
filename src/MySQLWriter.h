#ifndef LOGGING_WRITER_MYSQL_H
#define LOGGING_WRITER_MYSQL_H

#include "logging/WriterBackend.h"
#include "threading/formatters/Ascii.h"
#include <mysql.h>

namespace logging
{
namespace writer
{

// extend WriterBackend
class MySQL : public WriterBackend
{
public:
    MySQL(WriterFrontend *frontend);
    ~MySQL();

    static WriterBackend *Instantiate(WriterFrontend *frontend)
    {
        return new MySQL(frontend);
    }

protected:
    bool DoInit(const WriterInfo &info, int num_fields, const threading::Field *const *fields) override;
    bool DoWrite(int num_fields, const threading::Field *const *fields, threading::Value **vals) override;
    bool DoSetBuf(bool enabled) override;
    bool DoRotate(const char *rotated_path, double open, double close, bool terminating) override;
    bool DoFlush(double network_time) override;
    bool DoFinish(double network_time) override;
    bool DoHeartbeat(double network_time, double current_time) override;

private:
    bool CreateTable(int num_fields, const threading::Field *const *fields);
    string EscapeIdentifier(const char *identifier);
    string GetTableType(int arg_type, int arg_subtype);
    std::tuple<bool, string, int> CreateParams(const threading::Value *val);
    string LookupParam(const WriterInfo &info, const string name) const;
    MYSQL mysql;
    string table;
    string createTable;
    string insert;
    bool bytea_instead_text;
    std::unique_ptr<threading::formatter::Ascii> io;
    
    // mysql configure
    string hostname;
    string dbname;
    string port;
    string user;
    string password;
};

} // namespace writer
} // namespace logging
#endif

