#pragma once

#include <string>
#include <vector>
#include <functional>

#include <mongocxx/client.hpp>
#include <mongocxx/options/create_collection.hpp>

#include "Milliseconds.hpp"
#include "Mave/Mave.hpp"
#include "Mave/Bson.hpp"

namespace Integro
{
	namespace Access
	{
		using std::string;
		using std::to_string;
		using std::vector;
		using std::chrono::milliseconds;
		using std::exception;
		using std::function;

		class MongoClient
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
					throw exception("MongoClient::Query(): time attribute must be provided");
				}
				if (upperBound > milliseconds::zero() && lowerBound > upperBound)
				{
					throw exception(("MongoClient::Query(): bad interval ["
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
					throw exception("MongoClient::Query(): attribute must be provided");
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
					if (objects[0]["_id"].IsCustom())
					{
						for (auto &o : objects)
						{
							if (!o["_id"].IsCustom() || o["_id"].AsCustom().first != Mave::BSON_OID)
							{
								throw exception("MongoClient::Delete(): unexpected id type encountered");
							}
							filterIds << bsoncxx::oid(o["_id"].AsCustom().second);
						}
					}
					else
					{
						for (auto &o : objects)
						{
							if (!o["_id"].IsString())
							{
								throw exception("MongoClient::Delete(): unexpected id type encountered");
							}
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
					, const int maxCollectionSize = 5242880)
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
	}
}