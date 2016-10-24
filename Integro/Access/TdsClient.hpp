#pragma once

#include <sstream>
#include <vector>
#include <map>
#include <functional>

#include <sybfront.h>
#include <sybdb.h>

#include "Mave/Mave.hpp"

namespace Integro
{
	namespace Access
	{
		extern "C" const char *freetds_conf_path;

		using std::string;
		using std::stringstream;
		using std::vector;
		using std::map;
		using std::exception;
		using std::function;

		class TdsClient
		{
			class Infin
			{
			public:
				bool IsInitialized;
				string ConfigPath;
				string ApplicationName;
				function<void(const string&)> OnError;
				function<void(const string&)> OnEvent;

				Infin(
					const string &configPath
					, const string &applicationName
					, function<void(const string&)> OnError
					, function<void(const string&)> OnEvent)
					: ConfigPath(configPath)
					, ApplicationName(applicationName)
					, OnError(OnError)
					, OnEvent(OnEvent)
				{
					auto status = dbinit();

					if (status == FAIL)
					{
						IsInitialized = false;
					}
					else
					{
						freetds_conf_path = ConfigPath == "" ? NULL : ConfigPath.c_str();
						dberrhandle(HandleError);
						dbmsghandle(HandleMessage);
						IsInitialized = true;
					}
				}

				~Infin()
				{
					if (IsInitialized)
					{
						dbexit();
					}
				}
			};

			static Infin infin;

			static
				int
				HandleError(
					DBPROCESS* dbproc
					, int severity
					, int dberr
					, int oserr
					, char* dberrstr
					, char* oserrstr)
			{
				stringstream s;

				if (DBDEAD(dbproc))
				{
					s << "[dbproc is dead] ";
				}

				if (oserr && oserrstr)
				{
					s << "[os error] " << "code: '" << oserr << "', message: '" << oserrstr << "'; ";
				}

				if (dberrstr)
				{
					s << "[db-lib error] ";

					if (dberr)
					{
						s << "code: '" << dberr << "', severity: '" << severity << "', ";
					}

					s << "message: '" << dberrstr << "'";
				}

				infin.OnError(s.str());

				return INT_CANCEL;
			}

			static
				int
				HandleMessage(
					DBPROCESS* dbproc
					, DBINT msgno
					, int msgstate
					, int severity
					, char* msgtext
					, char* srvname
					, char* procname
					, int line)
			{
				enum { changed_database = 5701, changed_language = 5703 };

				if (msgno != changed_database
					&& msgno != changed_language)
				{
					stringstream s;

					s << "message number: '" << msgno << "', severity: '" << severity << "', message state: '" << msgstate << "'";

					if (srvname)
					{
						s << ", server: '" << srvname << "'";
					}

					if (procname)
					{
						s << ", procedure: '" << procname << "'";
					}

					if (line > 0)
					{
						s << ", line: '" << line << "'";
					}

					if (msgtext)
					{
						s << ", message text: '" << msgtext << "'";
					}

					infin.OnEvent(s.str());
				}

				return 0;
			}

			static
				string
				ToTrimmedString(
					char *begin
					, char *end)
			{
				for (; begin < end && *begin == ' '; ++begin);
				for (; begin < end && *(end - 1) == ' '; --end);
				return string(begin, end);
			}

			DBPROCESS *dbproc = NULL;

		public:
			static
				void
				ExecuteCommand(
					const string &host
					, const string &user
					, const string &password
					, const string &database
					, const string &sql)
			{
				TdsClient client(host, user, password);
				client.ExecuteCommand(database, sql);
			}

			static
				void
				ExecuteQuery(
					const string &host
					, const string &user
					, const string &password
					, const string &database
					, const string &sql
					, function<void(Mave::Mave&)> OnRow)
			{
				TdsClient client(host, user, password);
				client.ExecuteCommand(database, sql);
				client.FetchResults(OnRow);
			}

			TdsClient(const TdsClient&) = delete;
			TdsClient& operator=(const TdsClient&) = delete;

			TdsClient(
				const string &host
				, const string &user
				, const string &password)
			{
				if (!infin.IsInitialized)
				{
					throw exception("TdsClient::TdsClient(): failed to initialize the driver");
				}

				auto *login = dblogin();

				if (login == NULL)
				{
					throw exception("TdsClient::TdsClient(): failed to allocate login structure");
				}

				DBSETLUSER(login, user.c_str());
				DBSETLPWD(login, password.c_str());
				DBSETLAPP(login, infin.ApplicationName.c_str());

				for (auto version = DBVERSION_74; dbproc == NULL && version >= DBTDS_UNKNOWN; --version)
				{
					DBSETLVERSION(login, version);
					dbproc = dbopen(login, host.c_str());
				}

				dbloginfree(login);

				if (dbproc == NULL)
				{
					throw exception(("TdsClient::TdsClient(): failed to connect to '" + host + "'").c_str());
				}
			}

			void
				FetchResults(
					function<void(Mave::Mave&)> OnRow)
			{
				vector<string> columns;

				while (true)
				{
					auto status = dbresults(dbproc);

					if (status == NO_MORE_RESULTS)
					{
						break;
					}

					if (status == FAIL)
					{
						throw exception("TdsClient::FetchResults(): failed to fetch a result");
					}

					auto columnCount = dbnumcols(dbproc);

					if (columnCount == 0)
					{
						continue;
					}

					for (auto i = 0; i < columnCount; ++i)
					{
						auto name = dbcolname(dbproc, i + 1);
						columns.emplace_back(name);
					}

					while (true)
					{
						auto rowCode = dbnextrow(dbproc);

						if (rowCode == NO_MORE_ROWS)
						{
							break;
						}

						map<string, Mave::Mave> row;

						switch (rowCode)
						{
							case REG_ROW:
								for (auto i = 0; i < columnCount; ++i)
								{
									auto data = dbdata(dbproc, i + 1);

									if (data == NULL)
									{
										row.insert({ columns[i], nullptr });
									}
									else
									{
										auto type = dbcoltype(dbproc, i + 1);
										auto length = dbdatlen(dbproc, i + 1);
										vector<BYTE> buffer(max(32, 2 * length) + 2, 0);
										auto count = dbconvert(dbproc, type, data, length, SYBCHAR, &buffer[0], buffer.size() - 1);

										if (count == -1)
										{
											throw exception("TdsClient::FetchResults(): failed to fetch column data, insufficient buffer space");
										}

										row.insert({ columns[i], ToTrimmedString((char*)&buffer[0], (char*)&buffer[count]) });
									}
								}
								OnRow(Mave::Mave(row));
								break;
							case BUF_FULL:
								throw exception("TdsClient::FetchResults(): failed to fetch a row, the buffer is full");
							case FAIL:
								throw exception("TdsClient::FetchResults(): failed to fetch a row");
							default:
								// ingnore a computeid row
								break;
						}
					}
				}
			}

			void
				ExecuteCommand(
					const string &database
					, const string &sql)
			{
				dbfreebuf(dbproc);

				auto status = dbuse(dbproc, database.c_str());

				if (status == FAIL)
				{
					throw exception(("TdsClient::ExecuteCommand(): failed to use a database '" + database + "'").c_str());
				}

				status = dbcmd(dbproc, sql.c_str());

				if (status == FAIL)
				{
					throw exception(("TdsClient::ExecuteCommand(): failed to process a query '" + sql + "'").c_str());
				}

				status = dbsqlexec(dbproc);

				if (status == FAIL)
				{
					throw exception(("TdsClient::ExecuteCommand(): failed to execute a query '" + sql + "'").c_str());
				}
			}

			~TdsClient()
			{
				if (dbproc != NULL)
				{
					dbclose(dbproc);
				}
			}
		};
	}
}