#pragma once

#include <vector>
#include <sstream>
#include <functional>
#include <memory>
#include <algorithm>

#include <sybfront.h>
#include <sybdb.h>

#include "LDAPAsynConnection.h"
#include "LDAPSearchResult.h"
#include "LDAPResult.h"

#include <mongocxx/client.hpp>
#include <mongocxx/options/create_collection.hpp>

#include "client_http.hpp"

#include "build_compability_includes/real/win32compability.h"
#include "lmdb.h"

#include "Milliseconds.hpp"
#include "Mave/Mave.hpp"
#include "Mave/Ldap.hpp"
#include "Mave/Json.hpp"
#include "Mave/Bson.hpp"

namespace Integro
{
	using namespace std;
	using namespace chrono;
	using namespace json11;

	extern "C" const char *freetds_conf_path;

	class ClientTds
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
			ClientTds client(host, user, password);
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
			ClientTds client(host, user, password);
			client.ExecuteCommand(database, sql);
			client.FetchResults(OnRow);
		}

		ClientTds(const ClientTds&) = delete;
		ClientTds& operator=(const ClientTds&) = delete;

		ClientTds(
			const string &host
			, const string &user
			, const string &password)
		{
			if (!infin.IsInitialized)
			{
				throw exception("ClientTds::ClientTds(): failed to initialize the driver");
			}

			auto *login = dblogin();

			if (login == NULL)
			{
				throw exception("ClientTds::ClientTds(): failed to allocate login structure");
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
				throw exception(("ClientTds::ClientTds(): failed to connect to '" + host + "'").c_str());
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
					throw exception("ClientTds::FetchResults(): failed to fetch a result");
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
										throw exception("ClientTds::FetchResults(): failed to fetch column data, insufficient buffer space");
									}

									row.insert({ columns[i], ToTrimmedString((char*)&buffer[0], (char*)&buffer[count]) });
								}
							}
							OnRow(Mave::Mave(row));
							break;
						case BUF_FULL:
							throw exception("ClientTds::FetchResults(): failed to fetch a row, the buffer is full");
						case FAIL:
							throw exception("ClientTds::FetchResults(): failed to fetch a row");
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
				throw exception(("ClientTds::ExecuteCommand(): failed to use a database '" + database + "'").c_str());
			}

			status = dbcmd(dbproc, sql.c_str());

			if (status == FAIL)
			{
				throw exception(("ClientTds::ExecuteCommand(): failed to process a query '" + sql + "'").c_str());
			}

			status = dbsqlexec(dbproc);

			if (status == FAIL)
			{
				throw exception(("ClientTds::ExecuteCommand(): failed to execute a query '" + sql + "'").c_str());
			}
		}

		~ClientTds()
		{
			if (dbproc != NULL)
			{
				dbclose(dbproc);
			}
		}
	};

	class ClientLdap
	{
		static
			int
			SearchSome(
				const string &host
				, const int port
				, const string &user
				, const string &password
				, const string &node
				, const string &filter
				, function<void(Mave::Mave&)> OnEntry)
		{
			auto result = LDAPResult::SUCCESS;
			LDAPAsynConnection connection(host, port);
			auto queue = unique_ptr<LDAPMessageQueue>(connection.bind(user, password));

			if (queue.get() == nullptr)
			{
				throw exception("ClientLdap::SearchSome(): bind has failed");
			}

			auto message = unique_ptr<LDAPMsg>(queue->getNext());

			if (message.get() == nullptr)
			{
				throw exception("ClientLdap::SearchSome(): bind has failed");
			}

			queue = unique_ptr<LDAPMessageQueue>(connection.search(
				node
				//, LDAPAsynConnection::SEARCH_BASE
				//, LDAPAsynConnection::SEARCH_ONE
				, LDAPAsynConnection::SEARCH_SUB
				, filter));

			if (queue.get() == nullptr)
			{
				throw exception("ClientLdap::SearchSome(): search has failed");
			}

			for (auto cont = true; cont;)
			{
				message = unique_ptr<LDAPMsg>(queue->getNext());

				if (message.get() == nullptr)
				{
					throw exception("ClientLdap::SearchSome(): search has failed");
				}

				auto type = message->getMessageType();
				const LDAPEntry *entry = nullptr;

				switch (type)
				{
					case LDAPMsg::SEARCH_ENTRY:
						entry = ((LDAPSearchResult*)message.get())->getEntry();
						if (entry == nullptr)
						{
							throw exception("ClientLdap::SearchSome(): search has failed");
						}
						OnEntry(Mave::FromLdap(*entry));
						break;
					case LDAPMsg::SEARCH_REFERENCE:
						break;
					default:
						result = ((LDAPResult*)message.get())->getResultCode();
						cont = false;
						break;
				}
			}

			return result;
		}

	public:
		static
			void
			Search(
				const string &host
				, const int port
				, const string &user
				, const string &password
				, const string &node
				, const string &filter
				, const string &idAttribute
				, const string &timeAttribute
				, const milliseconds lowerBound
				, const milliseconds upperBound
				, function<void(Mave::Mave&)> OnEntry
				, function<void(const string&)> OnError
				, function<void(const string&)> OnEvent)
		{
			vector<Mave::Mave> entries;
			stack<pair<milliseconds, milliseconds>> intervals; intervals.push({ lowerBound, upperBound });
			stringstream s;

			while (intervals.size() > 0)
			{
				auto i = intervals.top(); intervals.pop();

				if (i.second > milliseconds::zero() && i.first > i.second)
				{
					throw exception(("ClientLdap::Search(): bad interval ["
						+ to_string(i.first.count()) + ", " + to_string(i.second.count()) + "]").c_str());
				}

				s.str("");
				s << "(&";
				s << filter;
				s << "(" << idAttribute << "=*)";
				s << "(" << timeAttribute << "=*)";
				if (i.first > milliseconds::zero()) s << "(" << timeAttribute << ">=" << MillisecondsToLdapTime(i.first) << ")";
				if (i.second > milliseconds::zero()) s << "(" << timeAttribute << "<=" << MillisecondsToLdapTime(i.second) << ")";
				s << ")";
				auto newFilter = s.str();

				s.str("");
				auto utcLower = i.first > milliseconds::zero() ? MillisecondsToUtc(i.first) : "";
				auto utcUpper = i.second > milliseconds::zero() ? MillisecondsToUtc(i.second) : "";
				s << "ClientLdap::Search(): searching [" << utcLower << ", " << utcUpper << "]";
				OnEvent(s.str());

				entries.clear();
				auto result = SearchSome(host, port, user, password, node, newFilter, [&](Mave::Mave &entry)
				{
					entries.push_back(entry);
					auto &value = entries.back()[timeAttribute];
					value = LdapTimeToMilliseconds(value.AsString());
				});

				sort(entries.begin(), entries.end(), [&](Mave::Mave &left, Mave::Mave &right)
				{
					return left[timeAttribute].AsMilliseconds() < right[timeAttribute].AsMilliseconds();
				});

				if (result == LDAPResult::SUCCESS)
				{
					for (auto &entry : entries)
					{
						entry[timeAttribute] = MillisecondsToLdapTime(entry[timeAttribute].AsMilliseconds());
						OnEntry(entry);
					}
				}
				else if (entries.size() > 0
					&& (result == LDAPResult::SIZE_LIMIT_EXCEEDED
						|| result == LDAPResult::TIME_LIMIT_EXCEEDED))
				{
					auto lastTime = entries.back()[timeAttribute].AsMilliseconds();

					for (auto e = entries.rbegin() + entries.size() / 2; e != entries.rend(); ++e)
					{
						auto time = (*e)[timeAttribute].AsMilliseconds();

						if (time != lastTime)
						{
							intervals.push({ time, i.second });
							intervals.push({ i.first, time });
							result = LDAPResult::SUCCESS;
							break;
						}
					}
				}

				if (result != LDAPResult::SUCCESS)
				{
					s.str("");
					auto utcLower = i.first > milliseconds::zero() ? MillisecondsToUtc(i.first) : "";
					auto utcUpper = i.second > milliseconds::zero() ? MillisecondsToUtc(i.second) : "";
					s << "ClientLdap::Search(): failed at [" << utcLower << ", " << utcUpper << "]";
					OnError(s.str());
				}
			}
		}
	};

	class ClientMongo
	{
	public:
		static
			void
			QueryCapped(
				function<void(Mave::Mave&)> OnObject
				, const string &url
				, const string &database
				, const string &collection
				, const bsoncxx::oid &lowerBound)
		{
			bsoncxx::builder::stream::document filter;
			filter
				<< "_id"
				<< bsoncxx::builder::stream::open_document
				<< "$gte"
				<< lowerBound
				<< bsoncxx::builder::stream::close_document;

			mongocxx::options::find options;
			options.sort(
				bsoncxx::builder::stream::document{}
				<< "$natural"
				<< -1
				<< bsoncxx::builder::stream::finalize);

			mongocxx::client client{ mongocxx::uri{ url } };
			auto cursor = client[database][collection].find(filter.view(), options);
			vector<Mave::Mave> maves;
			for (auto d : cursor)
			{
				maves.push_back(Mave::FromBson(d));
			}
			for (auto i = maves.rbegin(); i != maves.rend(); ++i)
			{
				OnObject(*i);
			}
		}

		static
			void
			Query(
				function<void(Mave::Mave&)> OnObject
				, const string &url
				, const string &database
				, const string &collection
				, const string &timeAttribute
				, const milliseconds lowerBound
				, const milliseconds upperBound)
		{
			if (timeAttribute == "")
			{
				throw exception("ClientMongo::Query(): time attribute must be provided");
			}
			if (upperBound > milliseconds::zero() && lowerBound > upperBound)
			{
				throw exception(("ClientMongo::Query(): bad interval ["
					+ to_string(lowerBound.count()) + ", " + to_string(upperBound.count()) + "]").c_str());
			}

			bsoncxx::builder::stream::document filter;
			if (lowerBound > milliseconds::zero())
			{
				filter
					<< timeAttribute
					<< bsoncxx::builder::stream::open_document
					<< "$gte"
					<< bsoncxx::types::b_date(lowerBound.count())
					<< bsoncxx::builder::stream::close_document;
			}
			if (upperBound > milliseconds::zero())
			{
				filter
					<< timeAttribute
					<< bsoncxx::builder::stream::open_document
					<< "$lte"
					<< bsoncxx::types::b_date(upperBound.count())
					<< bsoncxx::builder::stream::close_document;
			}

			mongocxx::options::find options;
			options.sort(
				bsoncxx::builder::stream::document{}
				<< timeAttribute
				<< 1
				<< bsoncxx::builder::stream::finalize);

			mongocxx::client client{ mongocxx::uri{ url } };
			auto cursor = client[database][collection].find(filter.extract(), options);
			for (auto d : cursor)
			{
				OnObject(Mave::FromBson(d));
			}
		}

		static
			void
			Query(
				function<void(Mave::Mave&)> OnObject
				, const string &url
				, const string &database
				, const string &collection
				, const string &attribute
				, vector<int> &values)
		{
			if (attribute == "")
			{
				throw exception("ClientMongo::Query(): attribute must be provided");
			}

			bsoncxx::builder::stream::array filterIds;
			for (auto v : values)
			{
				filterIds << v;
			}

			bsoncxx::builder::stream::document filter;
			filter
				<< attribute
				<< bsoncxx::builder::stream::open_document
				<< "$in"
				<< bsoncxx::types::b_array{ filterIds.view() }
			<< bsoncxx::builder::stream::close_document;

			mongocxx::options::find options;
			options.sort(
				bsoncxx::builder::stream::document{}
				<< attribute
				<< 1
				<< bsoncxx::builder::stream::finalize);

			mongocxx::client client{ mongocxx::uri{ url } };
			auto cursor = client[database][collection].find(filter.view(), options);
			for (auto d : cursor)
			{
				OnObject(Mave::FromBson(d));
			}
		}

		static
			void
			Query(
				function<void(Mave::Mave&)> OnObject
				, const string &url
				, const string &database
				, const string &collection)
		{
			mongocxx::client client{ mongocxx::uri{ url } };
			auto cursor = client[database][collection].find({});
			for (auto d : cursor)
			{
				OnObject(Mave::FromBson(d));
			}
		}

		static
			bool
			Contains(
				const bsoncxx::oid &id
				, const string &url
				, const string &database
				, const string &collection)
		{
			mongocxx::client client{ mongocxx::uri{ url } };
			return 0 < client[database][collection].count(bsoncxx::builder::stream::document{} << "_id" << id << bsoncxx::builder::stream::finalize);
		}

		static
			unsigned long long
			Count(
				const string &url
				, const string &database
				, const string &collection)
		{
			mongocxx::client client{ mongocxx::uri{ url } };
			return client[database][collection].count(bsoncxx::document::view());
		}

		static
			void
			Delete(
				const vector<Mave::Mave> &objects
				, const string &url
				, const string &database
				, const string &collection)
		{
			if (!objects.empty())
			{
				bsoncxx::builder::stream::array filterIds;
				if (objects[0]["_id"].AsCustom().first == Mave::BSON_OID)
				{
					for (auto &o : objects)
					{
						filterIds << bsoncxx::oid(o["_id"].AsCustom().second);
					}
				}
				else
				{
					for (auto &o : objects)
					{
						filterIds << o["_id"].AsString();
					}
				}

				bsoncxx::builder::stream::document filter;
				filter
					<< "_id"
					<< bsoncxx::builder::stream::open_document
					<< "$in"
					<< bsoncxx::types::b_array{ filterIds.view() }
				<< bsoncxx::builder::stream::close_document;

				mongocxx::client client{ mongocxx::uri{ url } };
				auto result = client[database][collection].delete_many(filter.view());
			}
		}

		static
			void
			Insert(
				const Mave::Mave &object
				, const string &url
				, const string &database
				, const string &collection)
		{
			mongocxx::client client{ mongocxx::uri{ url } };
			auto result = client[database][collection].insert_one(ToBsonDocument(object));
		}

		static
			void
			Insert(
				const vector<Mave::Mave> &objects
				, const string &url
				, const string &database
				, const string &collection)
		{
			if (!objects.empty())
			{
				vector<bsoncxx::document::value> documents;
				for (auto &o : objects)
				{
					documents.push_back(ToBsonDocument(o));
				}

				mongocxx::client client{ mongocxx::uri{ url } };
				auto result = client[database][collection].insert_many(documents);
			}
		}

		static
			void
			Upsert(
				const vector<Mave::Mave> &objects
				, const string &url
				, const string &database
				, const string &collection)
		{
			if (!objects.empty())
			{
				Delete(objects, url, database, collection);
				Insert(objects, url, database, collection);
			}
		}

		static
			void
			CreateIndex(
				const string &attribute
				, const string &url
				, const string &database
				, const string &collection)
		{
			mongocxx::client client{ mongocxx::uri{ url } };
			client[database][collection].create_index(bsoncxx::builder::stream::document{} << attribute << 1 << bsoncxx::builder::stream::finalize);
		}

		static
			void
			CreateCollection(
				const string &url
				, const string &database
				, const string &collection)
		{
			mongocxx::client client{ mongocxx::uri{ url } };
			client[database].create_collection(collection);
		}

		static
			void
			CreateCappedCollection(
				const string &url
				, const string &database
				, const string &collection
				, const int maxDocumentsCount = 5000
				, const long long maxCollectionSize = 5242880)
		{
			mongocxx::client client{ mongocxx::uri{ url } };
			mongocxx::options::create_collection options;
			options.capped(true);
#pragma push_macro("max")
#undef max
			options.max(maxDocumentsCount);
#pragma pop_macro("max")
			options.size(maxCollectionSize);
			client[database].create_collection(collection, options);
		}

		static
			void
			DropCollection(
				const string &url
				, const string &database
				, const string &collection)
		{
			mongocxx::client client{ mongocxx::uri{ url } };
			client[database][collection].drop();
		}

		static
			void
			DropDatabase(
				const string &url
				, const string &database)
		{
			mongocxx::client client{ mongocxx::uri{ url } };
			client[database].drop();
		}
	};

	class ClientElastic
	{
		typedef SimpleWeb::Client<SimpleWeb::HTTP> HttpClient;

		static
			Json
			MakeRequest(
				const string &url
				, const string &request
				, const string &path
				, stringstream &content)
		{
			HttpClient client(url);
			auto httpResponse = client.request(request, path, content);
			stringstream s; s << httpResponse->content.rdbuf();
			string error;
			auto response = Json::parse(s.str(), error);

			if (error != "")
			{
				throw exception(error.c_str());
			}

			if (!response["error"].is_null())
			{
				throw exception(response.dump().c_str());
			}

			if (response["errors"].is_bool() && response["errors"].bool_value())
			{
				auto &items = const_cast<Json::array&>(response["items"].array_items());
				auto end = remove_if(items.begin(), items.end(), [&](const Json &item)
				{
					return item["index"]["error"].is_null();
				});
				items.resize(end - items.begin());

				throw exception(response.dump().c_str());
			}

			return response;
		}

	public:
		static
			void
			Index(
				const vector<Mave::Mave> &objects
				, const string &url
				, const string &index
				, const string &type
				, const int maxBatchCount = 10000
				, const int maxBatchSize = 100000000)
		{
			string request = "PUT";
			string path = (index == "" ? "" : "/" + index + (type == "" ? "" : "/" + type)) + "/_bulk";
			stringstream content;
			int n = 0;

			for (auto o = objects.begin(); o != objects.end(); ++o)
			{
				auto json = ToJson(*o);
				content << "{\"index\":{\"_id\":\"" << json["_id"].string_value() << "\"}}" << endl;
				content << json.dump() << endl;

				if (++n == maxBatchCount || 1 + o == objects.end() || content.tellp() >= maxBatchSize)
				{
					auto response = MakeRequest(url, request, path, content);
					content.str("");
					n = 0;
				}
			}
		}

		static
			void
			Delete(
				const vector<string> &ids
				, const string &url
				, const string &index
				, const string &type)
		{
			string request = "DELETE";
			string path = index == "" ? "" : "/" + index + (type == "" ? "" : "/" + type);
			stringstream content;

			if (ids.size() > 0)
			{
				content << "{\"ids\":[";

				for (auto i = ids.begin();;)
				{
					content << "\"" << *i << "\"";

					if (++i == ids.end())
					{
						break;
					}

					content << ",";
				}

				content << "]}";
			}

			auto response = MakeRequest(url, request, path, content);
		}

		static
			void
			CreateIndex(
				const string &url
				, const string &index)
		{
			if (index == "")
			{
				throw exception("ClientElastic::CreateIndex(): index name is required");
			}

			string request = "PUT";
			string path = "/" + index;
			stringstream content;
			auto response = MakeRequest(url, request, path, content);
		}

		static
			void
			DeleteIndex(
				const string &url
				, const string &index)
		{
			if (index == "")
			{
				throw exception("ClientElastic::DeleteIndex(): index name is required");
			}

			string request = "DELETE";
			string path = "/" + index;
			stringstream content;
			auto response = MakeRequest(url, request, path, content);
		}

		static
			void
			DeleteType(
				const string &url
				, const string &index
				, const string &type)
		{
			if (index == "" || type == "")
			{
				throw exception("ClientElastic::DeleteType(): index and type names are required");
			}

			string request = "DELETE";
			string path = "/" + index + "/" + type;
			stringstream content;
			auto response = MakeRequest(url, request, path, content);
		}

		static
			int
			Count(
				const string &url
				, const string &index
				, const string &type)
		{
			string request = "GET";
			string path = (index == "" ? "" : "/" + index + (type == "" ? "" : "/" + type)) + "/_search?from=0&size=0";
			stringstream content;
			auto response = MakeRequest(url, request, path, content);
			return response["hits"]["total"].int_value();
		}

		static
			Mave::Mave
			Get(
				const string &url
				, const string &index
				, const string &type
				, const string &id)
		{
			string request = "GET";
			string path = (index == "" ? "" : "/" + index + (type == "" ? "" : "/" + type)) + id;
			stringstream content;
			auto response = MakeRequest(url, request, path, content);
			return Mave::FromJson(response);
		}

		static
			void
			Get(
				function<void(Mave::Mave&)> OnObject
				, const vector<string> &ids
				, const string &url
				, const string &index
				, const string &type)
		{
			if (ids.size() > 0)
			{
				string request = "GET";
				string path = (index == "" ? "" : "/" + index + (type == "" ? "" : "/" + type)) + "/_mget";
				stringstream content;

				content << "{\"ids\":[";

				for (auto i = ids.begin();;)
				{
					content << "\"" << *i << "\"";

					if (++i == ids.end())
					{
						break;
					}

					content << ",";
				}

				content << "]}";

				auto response = MakeRequest(url, request, path, content);

				for (auto &o : response["docs"].array_items())
				{
					if (!o["_source"].is_null())
					{
						OnObject(Mave::FromJson(o["_source"]));
					}
				}
			}
		}

		static
			void
			Search(
				function<void(Mave::Mave&)> OnObject
				, const string &url
				, const string &index
				, const string &type
				, const string &attribute
				, vector<string> &values)
		{
			stringstream query;
			query << "{\"query\":{\"constant_score\":{\"filter\":{\"terms\":{\"" << attribute << "\":[";

			for (auto i = values.begin();;)
			{
				query << "\"" << *i << "\"";

				if (++i == values.end())
				{
					break;
				}

				query << ",";
			}

			query << "]}}}}}";

			ClientElastic::Search(OnObject, url, index, type, query.str());
		}

		static
			void
			Search(
				function<void(Mave::Mave&)> OnObject
				, const string &url
				, const string &index
				, const string &type
				, const string &query = ""
				, const long long from = 0
				, const long long count = -1
				, const long long batchCount = 10000)
		{
			string request = "GET";
			stringstream content;

			for (long long i = from, n = count < 0 ? batchCount : min(count, batchCount); n != 0; i += n)
			{
				content.str(""); if (query != "") content << query << endl;
				auto path = (index == "" ? "" : "/" + index + (type == "" ? "" : "/" + type)) + "/_search?from=" + to_string(i) + "&size=" + to_string(n);
				auto response = MakeRequest(url, request, path, content);

				for (auto &o : response["hits"]["hits"].array_items())
				{
					OnObject(Mave::FromJson(o["_source"]));
				}

				auto m = response["hits"]["total"].int_value();
				n = min(batchCount, (count < 0 ? m : min(from + count, m)) - i);
			}
		}
	};

	class ClientLmdb
	{
	public:
		static
			string
			Get(
				const string &path
				, const string &key)
		{
			string value;

			if (!TryGet(path, key, value))
			{
				throw exception("ClientLmdb::Get(): failed to get the value of a key");
			}

			return value;
		}

		static
			string
			GetOrDefault(
				const string &path
				, const string &key)
		{
			string value;
			TryGet(path, key, value);
			return value;
		}

		static
			bool
			TryGet(
				const string &path
				, const string &key
				, string &value)
		{
			value = "";

			int rc = 0;
			MDB_env *env = NULL;
			MDB_dbi dbi = 0;
			MDB_txn *txn = NULL;
			MDB_val key_p, data_p;
			key_p.mv_data = NULL;
			key_p.mv_size = 0;
			data_p.mv_data = NULL;
			data_p.mv_size = 0;

			rc = mdb_env_create(&env);

			if (rc != 0)
			{
				mdb_env_close(env);
				throw exception("ClientLmdb::TryGet(): failed to create an environment");
			}

			rc = mdb_env_open(env, path.c_str(), 0, 0664);

			if (rc != 0)
			{
				mdb_env_close(env);
				throw exception("ClientLmdb::TryGet(): failed to open an environment");
			}

			rc = mdb_txn_begin(env, NULL, 0, &txn);

			if (rc != 0)
			{
				mdb_txn_abort(txn);
				mdb_env_close(env);
				throw exception("ClientLmdb::TryGet(): failed to begin a transaction");
			}

			rc = mdb_open(txn, NULL, 0, &dbi);

			if (rc != 0)
			{
				mdb_txn_abort(txn);
				mdb_close(env, dbi);
				mdb_env_close(env);
				throw exception("ClientLmdb::TryGet(): failed to open a database");
			}

			key_p.mv_data = (void*)key.data();
			key_p.mv_size = strlen((const char*)key_p.mv_data);

			rc = mdb_get(txn, dbi, &key_p, &data_p);

			if (rc == 0)
			{
				value.assign((const char*)data_p.mv_data, data_p.mv_size);
			}

			mdb_txn_abort(txn);
			mdb_close(env, dbi);
			mdb_env_close(env);

			return (rc == 0);
		}

		static
			void
			Set(
				const string &path
				, const string &key
				, const string &value)
		{
			int rc = 0;
			MDB_env *env = NULL;
			MDB_dbi dbi = 0;
			MDB_txn *txn = NULL;
			MDB_val key_p, data_p;
			key_p.mv_data = NULL;
			key_p.mv_size = 0;
			data_p.mv_data = NULL;
			data_p.mv_size = 0;

			rc = mdb_env_create(&env);

			if (rc != 0)
			{
				mdb_env_close(env);
				throw exception("ClientLmdb::Set(): failed to create an environment");
			}

			rc = mdb_env_open(env, path.c_str(), 0, 0664);

			if (rc != 0)
			{
				mdb_env_close(env);
				throw exception("ClientLmdb::Set(): failed to open an environment");
			}

			rc = mdb_txn_begin(env, NULL, 0, &txn);

			if (rc != 0)
			{
				mdb_txn_abort(txn);
				mdb_env_close(env);
				throw exception("ClientLmdb::Set(): failed to begin a transaction");
			}

			rc = mdb_open(txn, NULL, 0, &dbi);

			if (rc != 0)
			{
				mdb_txn_abort(txn);
				mdb_close(env, dbi);
				mdb_env_close(env);
				throw exception("ClientLmdb::Set(): failed to open a database");
			}

			key_p.mv_data = (void*)key.data();
			key_p.mv_size = strlen((const char*)key_p.mv_data);
			data_p.mv_data = (void*)value.data();
			data_p.mv_size = strlen((const char*)data_p.mv_data);

			rc = mdb_put(txn, dbi, &key_p, &data_p, 0);

			if (rc != 0)
			{
				mdb_txn_abort(txn);
				mdb_close(env, dbi);
				mdb_env_close(env);
				throw exception("ClientLmdb::Set(): failed to set the value of a key");
			}

			rc = mdb_txn_commit(txn);

			if (rc != 0)
			{
				mdb_txn_abort(txn);
				mdb_close(env, dbi);
				mdb_env_close(env);
				throw exception("ClientLmdb::Set(): failed to commit a transaction");
			}

			mdb_close(env, dbi);
			mdb_env_close(env);
		}

		static
			void
			Remove(
				const string &path
				, const string &key)
		{
			int rc = 0;
			MDB_env *env = NULL;
			MDB_dbi dbi = 0;
			MDB_txn *txn = NULL;
			MDB_val key_p;
			key_p.mv_data = NULL;
			key_p.mv_size = 0;

			rc = mdb_env_create(&env);

			if (rc != 0)
			{
				mdb_env_close(env);
				throw exception("ClientLmdb::Remove(): failed to create an environment");
			}

			rc = mdb_env_open(env, path.c_str(), 0, 0664);

			if (rc != 0)
			{
				mdb_env_close(env);
				throw exception("ClientLmdb::Remove(): failed to open an environment");
			}

			rc = mdb_txn_begin(env, NULL, 0, &txn);

			if (rc != 0)
			{
				mdb_txn_abort(txn);
				mdb_env_close(env);
				throw exception("ClientLmdb::Remove(): failed to begin a transaction");
			}

			rc = mdb_open(txn, NULL, 0, &dbi);

			if (rc != 0)
			{
				mdb_txn_abort(txn);
				mdb_close(env, dbi);
				mdb_env_close(env);
				throw exception("ClientLmdb::Remove(): failed to open a database");
			}

			key_p.mv_data = (void*)key.data();
			key_p.mv_size = strlen((const char*)key_p.mv_data);

			rc = mdb_del(txn, dbi, &key_p, NULL);

			if (rc != 0)
			{
				mdb_txn_abort(txn);
				mdb_close(env, dbi);
				mdb_env_close(env);
				throw exception("ClientLmdb::Remove(): failed to remove a key");
			}

			rc = mdb_txn_commit(txn);

			if (rc != 0)
			{
				mdb_txn_abort(txn);
				mdb_close(env, dbi);
				mdb_env_close(env);
				throw exception("ClientLmdb::Remove(): failed to commit a transaction");
			}

			mdb_close(env, dbi);
			mdb_env_close(env);
		}

		static
			void
			Query(
				const string &path
				, function<void(const string &key, const string &value)> OnKyeValue)
		{
			int rc = 0;
			MDB_env *env = NULL;
			MDB_dbi dbi = 0;
			MDB_txn *txn = NULL;
			MDB_val key_p, data_p;
			key_p.mv_data = NULL;
			key_p.mv_size = 0;
			data_p.mv_data = NULL;
			data_p.mv_size = 0;
			MDB_cursor *cursor = NULL;

			rc = mdb_env_create(&env);

			if (rc != 0)
			{
				mdb_env_close(env);
				throw exception("ClientLmdb::Query(): failed to create an environment");
			}

			rc = mdb_env_open(env, path.c_str(), 0, 0664);

			if (rc != 0)
			{
				mdb_env_close(env);
				throw exception("ClientLmdb::Query(): failed to open an environment");
			}

			rc = mdb_txn_begin(env, NULL, 0, &txn);

			if (rc != 0)
			{
				mdb_txn_abort(txn);
				mdb_env_close(env);
				throw exception("ClientLmdb::Query(): failed to begin a transaction");
			}

			rc = mdb_open(txn, NULL, 0, &dbi);

			if (rc != 0)
			{
				mdb_txn_abort(txn);
				mdb_close(env, dbi);
				mdb_env_close(env);
				throw exception("ClientLmdb::Query(): failed to open a database");
			}

			rc = mdb_cursor_open(txn, dbi, &cursor);

			if (rc != 0)
			{
				mdb_cursor_close(cursor);
				mdb_txn_abort(txn);
				mdb_close(env, dbi);
				mdb_env_close(env);
				throw exception("ClientLmdb::Query(): failed to open a cursor");
			}

			while (0 == (rc = mdb_cursor_get(cursor, &key_p, &data_p, MDB_NEXT)))
			{
				OnKyeValue(string((const char*)key_p.mv_data, key_p.mv_size), string((const char*)data_p.mv_data, data_p.mv_size));
			}

			mdb_cursor_close(cursor);
			mdb_txn_abort(txn);
			mdb_close(env, dbi);
			mdb_env_close(env);
		}
	};
}