#pragma once

#include <sstream>
#include <memory>

#include "mongo/client/dbclient.h"
#include "ConversionMave.hpp"

namespace Integro
{
	using namespace std;

	class ClientMongo
	{
		typedef mongo::DBClientBase DBClientBase;

		static DBClientBase* Create(const string &url)
		{
			string errmsg;
			auto cs = mongo::ConnectionString::parse(url, errmsg);

			if (!cs.isValid())
			{
				throw exception(errmsg.c_str());
			}

			auto *client = cs.connect(errmsg);

			if (client == nullptr)
			{
				throw exception(errmsg.c_str());
			}

			return client;
		}

	public:
		static unsigned long long Count(const string &url
			, const string &database
			, const string &collection)
		{
			unique_ptr<DBClientBase> client(Create(url));
			return client->count(database + "." + collection);
		}

		static void QueryCapped(const string &url
			, const string &database
			, const string &collection
			, const mongo::OID &lowerBound
			, function<void(const Mave&)> OnObject)
		{
			unique_ptr<DBClientBase> client(Create(url));
			auto query = MONGO_QUERY("_id" << mongo::GTE << lowerBound).sort("$natural");
			auto cursor = client->query(database + "." + collection, query);

			if (!cursor.get())
			{
				throw exception("ClientMongo::QueryCapped(): failed to obtain a cursor");
			}

			while (cursor->more())
			{
				OnObject(ConversionMave::FromBson(cursor->nextSafe()));
			}
		}

		static void Query(const string &url
			, const string &database
			, const string &collection
			, const string &timeAttribute
			, const milliseconds lowerBound
			, const milliseconds upperBound
			, function<void(const Mave&)> OnObject)
		{
			assert(upperBound <= milliseconds::zero()
				|| lowerBound <= upperBound);

			unique_ptr<DBClientBase> client(Create(url));

			mongo::BSONObjBuilder b;

			if (lowerBound > milliseconds::zero())
			{
				b << timeAttribute << mongo::GTE << mongo::Date_t(lowerBound.count());
			}

			if (upperBound > milliseconds::zero())
			{
				b << timeAttribute << mongo::LTE << mongo::Date_t(upperBound.count());
			}

			auto query = mongo::Query(b.obj()); if (timeAttribute != "") query = query.sort(timeAttribute);
			auto cursor = client->query(database + "." + collection, query);

			if (!cursor.get())
			{
				throw exception("ClientMongo::Query(): failed to obtain a cursor");
			}

			while (cursor->more())
			{
				OnObject(ConversionMave::FromBson(cursor->nextSafe()));
			}
		}

		static bool Contains(const string &url
			, const string &database
			, const string &collection
			, const mongo::OID &id)
		{
			unique_ptr<DBClientBase> client(Create(url));
			auto query = MONGO_QUERY("_id" << mongo::GTE << id << "_id" << mongo::LTE << id).sort("_id");
			auto result = client->findOne(database + "." + collection, query);
			return !result.isEmpty();
		}

		static void Insert(const string &url
			, const string &database
			, const string &collection
			, const Mave &object)
		{
			unique_ptr<DBClientBase> client(Create(url));
			client->insert(database + "." + collection, ConversionMave::ToBson(object));
		}

		static void Upsert(const string &url
			, const string &database
			, const string &collection
			, const vector<Mave> &objects)
		{
			if (!objects.empty())
			{
				unique_ptr<DBClientBase> client(Create(url));
				auto builder = client->initializeUnorderedBulkOp(database + "." + collection);

				for (auto &o : objects)
				{
					if (o["_id"].IsBsonOid())
					{
						auto value = o["_id"].AsBsonOid();
						builder.find(BSON("_id" << value)).upsert().replaceOne(ConversionMave::ToBson(o));
					}
					else
					{
						auto value = o["_id"].AsString();
						builder.find(BSON("_id" << value)).upsert().replaceOne(ConversionMave::ToBson(o));
					}
				}

				mongo::WriteResult result;
				builder.execute(0, &result);
			}
		}

		static void CreateCollection(const string &url
			, const string &database
			, const string &collection
			, const bool isCapped = true
			, const int maxDocumentsCount = 5000
			, const long long maxCollectionSize = 5242880)
		{
			unique_ptr<DBClientBase> client(Create(url));

			if (!client->createCollection(database + "." + collection, maxCollectionSize, isCapped, maxDocumentsCount))
			{
				throw exception("ClientMongo::CreateCollection(): failed");
			}
		}

		static void DropCollection(const string &url
			, const string &database
			, const string &collection)
		{
			unique_ptr<DBClientBase> client(Create(url));

			if (!client->dropCollection(database + "." + collection))
			{
				throw exception("ClientMongo::DropCollection(): failed");
			}
		}

		static void DropDatabase(const string &url
			, const string &database)
		{
			unique_ptr<DBClientBase> client(Create(url));

			if (!client->dropDatabase(database))
			{
				throw exception("ClientMongo::DropDatabase(): failed");
			}
		}
	};
}