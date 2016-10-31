#pragma once

#include <sstream>
#include <string>
#include <chrono>
#include <regex>
#include <vector>
#include <set>
#include <atomic>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

#include "Access/TdsClient.hpp"
#include "Access/LdapClient.hpp"
#include "Access/MongoClient.hpp"
#include "Access/ElasticClient.hpp"
#include "Access/LmdbClient.hpp"
#include "Synchronized.hpp"
#include "Milliseconds.hpp"

namespace Integro
{
	using namespace std;
	using namespace chrono;
	using std::string;
	using std::regex;
	using std::regex_replace;

	class Copy
	{
	public:

		// CopyData

		template <
			typename Datum
			, typename Time>
			static
			void
			CopyDataInBulk(
				function<void(Time, function<void(Datum&)>)> LoadData
				, function<void(vector<Datum>&)> SaveData
				, function<Time()> LoadStartTime
				, function<void(Time)> SaveStartTime
				, function<Time(Datum&)> GetTime)
		{
			auto startTime = LoadStartTime();
			vector<Datum> data;

			LoadData(startTime, [&](Datum &datum)
			{
				auto time = GetTime(datum);

				if (startTime < time)
				{
					startTime = time;
				}

				data.push_back(datum);
			});

			if (data.size() > 0)
			{
				SaveData(data);
				SaveStartTime(startTime);
			}
		}

		template <
			typename Datum
			, typename Time>
			static
			void
			CopyDataInChunks(
				function<void(Time, function<void(Datum&)>)> LoadData
				, function<void(vector<Datum>&)> SaveData
				, function<Time()> LoadStartTime
				, function<void(Time)> SaveStartTime
				, function<Time(Datum&)> GetTime)
		{
			auto startTime = LoadStartTime();
			SynchronizedBuffer<Datum> buffer;
			auto hasFailed = false;
			atomic_flag lock = ATOMIC_FLAG_INIT;
			string error;

			auto TryThrow = [&]()
			{
				if (hasFailed)
				{
					throw exception("Copy::CopyDataInChunks(): abortion requested due to errors");
				}
			};

			enum ActionName { LoadDataAN, SaveDataAN };
			bool hasActionFinished[] = { false, false };
			function<void()> actions[] =
			{
				[&]() // LoadData
				{
					LoadData(startTime, [&](Datum &datum)
					{
						while (buffer.Size() > 10000)
						{
							TryThrow();
							this_thread::sleep_for(chrono::milliseconds(1));
						}

						TryThrow();
						buffer.AddOne(datum);
					});
				},

				[&]() // SaveData
				{
					while (!hasActionFinished[LoadDataAN] || !buffer.IsEmpty())
					{
						if (buffer.IsEmpty())
						{
							this_thread::sleep_for(chrono::milliseconds(1));
						}
						else
						{
							auto data = buffer.GetAll();

							for (auto &datum : data)
							{
								auto time = GetTime(datum);

								if (startTime > time)
								{
									throw exception("Copy::CopyDataInChunks(): invariant violation, the current record's time must be greater than or equal to the previous record's time");
								}

								startTime = time;
							}

							SaveData(data);
							SaveStartTime(startTime);
						}
					}
				}
			};

			auto OnError = [&](ActionName actionName)
			{
				try
				{
					actions[actionName]();
				}
				catch (const exception &ex)
				{
					if (!lock.test_and_set())
					{
						hasFailed = true;
						error = ex.what();
					}
				}
				catch (...)
				{
					if (!lock.test_and_set())
					{
						hasFailed = true;
						error = "ellipsis exception";
					}
				}

				atomic_thread_fence(memory_order_seq_cst);
				hasActionFinished[actionName] = true;
			};

			thread SaveDataThread(OnError, SaveDataAN);
			OnError(LoadDataAN);

			SaveDataThread.join();

			if (hasFailed)
			{
				throw exception(error.c_str());
			}
		}

		template <
			typename Datum
			, typename Time
			, typename Id>
			static
			void
			CopyCappedDataInChunks(
				function<void(Id&, function<void(Datum&)>)> LoadCappedData
				, function<void(Time, function<void(Datum&)>)> LoadData
				, function<void(vector<Datum>&)> SaveData
				, function<Time()> LoadStartTime
				, function<Id()> LoadStartId
				, function<void(Time)> SaveStartTime
				, function<void(Id&)> SaveStartId
				, function<Time(Datum&)> GetTime
				, function<Id(Datum&)> GetId)
		{
			auto cappedStartId = LoadStartId();
			auto cappedStartTime = LoadStartTime();
			auto storeStartId = cappedStartId;
			auto storeStartTime = cappedStartTime;
			SynchronizedBuffer<Datum> cappedBuffer;
			SynchronizedBuffer<Datum> storeBuffer;
			auto hasLoadingStoreDataBeenRequested = false;
			auto hasLoadingStoreDataBeenDisabled = false;
			auto hasFailed = false;
			atomic_flag lock = ATOMIC_FLAG_INIT;
			exception failure;

			auto TryThrow = [&](bool doThrow, bool isFinal)
			{
				if (doThrow)
				{
					throw isFinal ? failure : exception("Copy::CopyCappedDataInChunks(): abortion requested due to errors");
				}
			};

			enum ActionName { LoadCappedDataAN, LoadStoreDataAN, SaveCappedDataAN, SaveStoreDataAN };
			bool hasActionFinished[] = { false, false, false, false };
			function<void()> actions[] =
			{
				[&]() // LoadCappedData
				{
					LoadCappedData(cappedStartId, [&](Datum &datum)
					{
						TryThrow(hasFailed, false);
						cappedBuffer.AddOne(datum);
					});
				},
					[&]() // LoadStoreData
				{
					while (!hasLoadingStoreDataBeenDisabled
						&& !hasActionFinished[SaveCappedDataAN])
					{
						if (!hasLoadingStoreDataBeenRequested)
						{
							this_thread::sleep_for(chrono::milliseconds(1));
						}
						else
						{
							hasLoadingStoreDataBeenDisabled = true;

							LoadData(storeStartTime, [&](Datum &datum)
							{
								while (storeBuffer.Size() > 10000)
								{
									TryThrow(hasActionFinished[SaveStoreDataAN], false);
									this_thread::sleep_for(chrono::milliseconds(1));
								}

								TryThrow(hasActionFinished[SaveStoreDataAN], false);
								storeBuffer.AddOne(datum);
							});
						}
					}
				},
					[&]() // SaveCappedData
				{
					auto hasSavedMetadata = false;

					while (!hasActionFinished[LoadCappedDataAN] || !cappedBuffer.IsEmpty())
					{
						if (cappedBuffer.IsEmpty())
						{
							this_thread::sleep_for(chrono::milliseconds(1));
						}
						else
						{
							auto data = cappedBuffer.GetAll();

							for (auto &datum : data)
							{
								auto id = GetId(datum);

								if (!hasLoadingStoreDataBeenDisabled)
								{
									if (id == cappedStartId)
									{
										hasLoadingStoreDataBeenDisabled = true;
									}
									else
									{
										hasLoadingStoreDataBeenRequested = true;

										while (!hasLoadingStoreDataBeenDisabled
											&& !hasActionFinished[LoadStoreDataAN])
										{
											this_thread::sleep_for(chrono::milliseconds(1));
										}
									}
								}

								cappedStartId = id;

								auto time = GetTime(datum);

								if (cappedStartTime > time)
								{
									throw exception("Copy::CopyCappedDataInChunks(): invariant violation, the current record's time must be greater than or equal to the previous record's time");
								}

								cappedStartTime = time;
							}

							SaveData(data);

							if (hasActionFinished[SaveStoreDataAN] && !hasFailed)
							{
								SaveStartId(cappedStartId);
								SaveStartTime(cappedStartTime);
								hasSavedMetadata = true;
							}
						}
					}

					if (!hasLoadingStoreDataBeenDisabled)
					{
						hasSavedMetadata = true;
						hasLoadingStoreDataBeenRequested = true;
					}

					if (hasLoadingStoreDataBeenRequested)
					{
						while (!hasActionFinished[SaveStoreDataAN])
						{
							this_thread::sleep_for(chrono::milliseconds(1));
						}
					}

					if (!hasSavedMetadata && !hasFailed)
					{
						SaveStartId(cappedStartId);
						SaveStartTime(cappedStartTime);
					}
				},
					[&]() // SaveStoreData
				{
					while (!hasActionFinished[LoadStoreDataAN] || !storeBuffer.IsEmpty())
					{
						if (storeBuffer.IsEmpty())
						{
							this_thread::sleep_for(chrono::milliseconds(1));
						}
						else
						{
							auto data = storeBuffer.GetAll();

							for (auto &datum : data)
							{
								auto id = GetId(datum);
								storeStartId = id;

								auto time = GetTime(datum);

								if (storeStartTime > time)
								{
									throw exception("Copy::CopyCappedDataInChunks(): invariant violation, the current record's time must be greater than or equal to the previous record's time");
								}

								storeStartTime = time;
							}

							SaveData(data);
							SaveStartId(storeStartId);
							SaveStartTime(storeStartTime);
						}
					}
				}
			};

			auto OnError = [&](ActionName actionName)
			{
				try
				{
					actions[actionName]();
				}
				catch (const exception &ex)
				{
					if (!lock.test_and_set())
					{
						hasFailed = true;
						failure = exception(ex.what());
					}
				}
				catch (...)
				{
					if (!lock.test_and_set())
					{
						hasFailed = true;
						failure = exception("ellipsis exception");
					}
				}

				atomic_thread_fence(memory_order_seq_cst);
				hasActionFinished[actionName] = true;
			};

			thread SaveStoreDataThread(OnError, SaveStoreDataAN);
			thread SaveCappedDataThread(OnError, SaveCappedDataAN);
			thread LoadStoreDataThread(OnError, LoadStoreDataAN);
			OnError(LoadCappedDataAN);

			SaveStoreDataThread.join();
			SaveCappedDataThread.join();
			LoadStoreDataThread.join();

			TryThrow(hasFailed, true);
		}

		// LoadData

		static
			auto
			LoadDataTds(
				const string &host
				, const string &user
				, const string &password
				, const string &database
				, const string &query)
		{
			return [=](milliseconds startTime, function<void(Mave::Mave&)> OnDatum) mutable
			{
				// TEMPORARY SOLUTION NOTICE:
				// subtract 1 second from startTime to compensate for addition of 1 second in a query
				// must be removed when queries are fixed
				auto q = regex_replace(query
					, regex("\\$\\(LAST_EXEC_TIME\\)")
					, startTime <= milliseconds(1000)
					? "CONVERT(datetime, '1970-01-01')"
					: "convert(datetime, '" + Milliseconds::ToUtc(startTime - milliseconds(1000), true) + "')");

				Access::TdsClient::ExecuteQuery(host, user, password, database, q, OnDatum);
			};
		}

		static
			auto
			LoadDataLdap(
				const string &host
				, const int port
				, const string &user
				, const string &password
				, const string &node
				, const string &filter
				, const string &idAttribute
				, const string &timeAttribute
				, function<void(const string&)> OnError
				, function<void(const string&)> OnEvent)
		{
			return [=](milliseconds startTime, function<void(Mave::Mave&)> OnDatum) mutable
			{
				Access::LdapClient::Search(host, port, user, password, node, filter, idAttribute, timeAttribute, startTime, milliseconds::zero(), OnDatum, OnError, OnEvent);
			};
		}

		static
			auto
			LoadDataMongo(
				const string &url
				, const string &database
				, const string &collection
				, const string &timeAttribute)
		{
			Access::MongoClient::CreateIndex(timeAttribute, url, database, collection);

			return [=](milliseconds startTime, function<void(Mave::Mave&)> OnDatum) mutable
			{
				Access::MongoClient::Query(OnDatum, url, database, collection, timeAttribute, startTime, milliseconds::zero());
			};
		}

		static
			auto
			LoadCappedDataMongo(
				const string &url
				, const string &database
				, const string &collection)
		{
			return [=](bsoncxx::oid &startId, function<void(Mave::Mave&)> OnDatum) mutable
			{
				Access::MongoClient::QueryCapped(OnDatum, url, database, collection, startId);
			};
		}

		// SaveData

		static
			auto
			SaveDataMongo(
				const string &url
				, const string &database
				, const string &collection)
		{
			return [=](vector<Mave::Mave> &data) mutable
			{
				Access::MongoClient::Upsert(data, url, database, collection);
			};
		}

		static
			auto
			SaveDataElastic(
				const string &url
				, const string &index
				, const string &type)
		{
			return [=](vector<Mave::Mave> &data) mutable
			{
				Access::ElasticClient::Index(data, url, index, type);
			};
		}

		// ProcessData

		static
			auto
			ProcessDataTds(
				const string &channelName
				, const string &modelName
				, const string &model
				, const string &action
				, const vector<string> &targetStores)
		{
			return [=](vector<Mave::Mave> &data) mutable
			{
				for (auto &datum : data)
				{
					datum = map<string, Mave::Mave>(
					{
						{ "_id", boost::uuids::to_string(boost::uuids::random_generator()()) }
						,{ "_uid", datum.AsMap().count("_uid") == 0 ? "" : datum["_uid"].AsString() }
						, { "action", action }
						, { "channel", channelName }
						, { "modelName", modelName }
						, { "processed", 0 }
						, { "start_time", Milliseconds::FromUtc(datum["start_time"].AsString()) }
						, { "source", datum }
					});

					auto &d = datum.AsMap();
					auto &s = d["source"].AsMap();

					if (s.count("forType") > 0)
					{
						d["modelName"] = s["forType"].AsString();
					}

					if (targetStores.size() > 0)
					{
						d["targetStores"] = targetStores;
					}
				}
			};
		}

		static
			auto
			ProcessDataLdap(
				const string &idAttribute
				, const string &channelName
				, const string &modelName
				, const string &model
				, const string &action)
		{
			return [=](vector<Mave::Mave> &data) mutable
			{
				for (auto &datum : data)
				{
					datum = map<string, Mave::Mave>(
					{
						{ "_id", datum[idAttribute].AsString() }
						, { "_uid", datum[idAttribute].AsString() }
						, { "action", action }
						, { "channel", channelName }
						, { "modelName", modelName }
						, { "processed", 0 }
						, { "start_time", duration_cast<milliseconds>(chrono::system_clock::now().time_since_epoch()) }
						, { "source", datum }
					});
				}
			};
		}

		static
			auto
			ProcessDataLdapElastic()
		{
			return [=](vector<Mave::Mave> &data) mutable
			{
				for (auto &datum : data)
				{
					// Binary
					{
						for (auto a :
						{
							"msExchMailboxGuid"
							, "msExchMailboxSecurityDescriptor"
							, "objectGUID"
							, "objectSid"
							, "userParameters"
							, "userCertificate"
							, "msExchArchiveGUID"
							, "msExchBlockedSendersHash"
							, "msExchSafeSendersHash"
							, "securityProtocol"
							, "terminalServer"
							, "mSMQDigests"
							, "mSMQSignCertificates"
							, "msExchSafeRecipientsHash"
							, "msExchDisabledArchiveGUID"
							, "sIDHistory"
							, "replicationSignature"
							, "msExchMasterAccountSid"
							, "logonHours"
							, "thumbnailPhoto"
						})
						{
							auto &s = datum["source"].AsMap();

							if (s.count(a) > 0)
							{
								auto &o = s[a];

								if (!o.IsVector())
								{
									o = "";
								}
								else
								{
									for (auto &i : o.AsVector())
									{
										i = "";
									}
								}
							}
						}
					}

					// Variant
					{
						for (auto a :
						{
							"extensionAttribute1"
							, "extensionAttribute2"
							, "extensionAttribute3"
							, "extensionAttribute4"
							, "extensionAttribute5"
							, "extensionAttribute6"
							, "extensionAttribute7"
							, "extensionAttribute8"
							, "extensionAttribute9"
							, "extensionAttribute10"
							, "extensionAttribute11"
							, "extensionAttribute12"
							, "extensionAttribute13"
							, "extensionAttribute14"
							, "extensionAttribute15"
						})
						{
							auto &s = datum["source"].AsMap();

							if (s.count(a) > 0)
							{
								auto &o = s[a];

								if (!o.IsVector())
								{
									o = "[string] " + o.AsString();
								}
								else
								{
									for (auto &i : o.AsVector())
									{
										i = "[string] " + i.AsString();
									}
								}
							}
						}
					}
				}
			};
		}

		static
			auto
			ProcessDataMongo(
				const string &channelName
				, const string &modelName
				, const string &model
				, const string &action)
		{
			return [=](vector<Mave::Mave> &data) mutable
			{
				for (auto &datum : data)
				{
					datum = map<string, Mave::Mave>(
					{
						{ "_id", boost::uuids::to_string(boost::uuids::random_generator()()) }
						, { "_uid", datum.AsMap().count("_uid") == 0 ? "" : datum["_uid"].AsString() }
						, { "action", action }
						, { "channel", channelName }
						, { "modelName", modelName }
						, { "processed", 0 }
						, { "start_time", datum["start_time"].AsString() }
						, { "source", datum }
					});
				}
			};
		}

		// Metadata

		static
			auto
			LoadStartTimeLmdb(
				const string &path
				, const string &key)
		{
			return [=]() mutable
			{
				return milliseconds(stoull("0" + Access::LmdbClient::GetOrDefault(path, key)));
			};
		}

		static
			auto
			LoadStartIdLmdb(
				const string &path
				, const string &key)
		{
			return [=]() mutable
			{
				auto value = Access::LmdbClient::GetOrDefault(path, key);
				return value == "" ? bsoncxx::types::b_oid().value : bsoncxx::oid(value);
			};
		}

		static
			auto
			SaveStartTimeLmdb(
				const string &path
				, const string &key)
		{
			return [=](milliseconds time) mutable
			{
				Access::LmdbClient::Set(path, key, to_string(time.count()));
			};
		}

		static
			auto
			SaveStartIdLmdb(
				const string &path
				, const string &key)
		{
			return [=](bsoncxx::oid &id) mutable
			{
				Access::LmdbClient::Set(path, key, id.to_string());
			};
		}

		static
			auto
			GetTimeTds(
				const string &timeAttribute)
		{
			return [=](Mave::Mave &datum) mutable
			{
				return Milliseconds::FromUtc(datum[timeAttribute].AsString());
			};
		}

		static
			auto
			GetTimeLdap(
				const string &timeAttribute)
		{
			return [=](Mave::Mave &datum) mutable
			{
				//return ConversionMilliseconds::FromLdapTime(datum[timeAttribute].AsString());
				//return max(milliseconds::zero(), ConversionMilliseconds::FromLdapTime(datum[timeAttribute].AsString()) - milliseconds(25 * 60 * 60 * 1000));
				return milliseconds::zero();
			};
		}

		static
			auto
			GetTimeMongo(
				const string &timeAttribute)
		{
			return [=](Mave::Mave &datum) mutable
			{
				return datum[timeAttribute].AsMilliseconds();
			};
		}

		static
			auto
			GetIdMongo(
				const string &idAttribute)
		{
			return [=](Mave::Mave &datum) mutable
			{
				return bsoncxx::oid(datum[idAttribute].AsCustom().second);
			};
		}

		// Duplicates

		static
			auto
			RemoveDuplicates(
				const string &descriptorAttribute
				, const string &sourceAttribute
				, function<void(const string&, vector<int>&, function<void(Mave::Mave&)>)> LoadData)
		{
			return [=](vector<Mave::Mave> &data) mutable
			{
				if (data.size() == 0)
				{
					return;
				}

				vector<int> descriptors;

				for (auto &datum : data)
				{
					auto descriptor = Hash(datum[sourceAttribute]);
					datum.AsMap().insert({ descriptorAttribute, descriptor });
					descriptors.push_back(descriptor);
				}

				set<int> storedDescriptors;
				set<string> storedSources;

				LoadData(descriptorAttribute, descriptors, [&](Mave::Mave &datum)
				{
					storedSources.insert(ToString(datum[sourceAttribute]));
					storedDescriptors.insert(datum[descriptorAttribute].AsInt());
				});

				if (storedDescriptors.size() == 0)
				{
					return;
				}

				vector<Mave::Mave> noDuplicatesData;

				for (auto &datum : data)
				{
					if (storedDescriptors.count(datum[descriptorAttribute].AsInt()) == 0
						|| storedSources.count(ToString(datum[sourceAttribute])) == 0)
					{
						noDuplicatesData.push_back(datum);
					}
				}

				data = move(noDuplicatesData);
			};
		}

		static
			auto
			LoadDuplicateDataMongo(
				const string &url
				, const string &database
				, const string &collection
				, const string &descriptorAttribute)
		{
			Access::MongoClient::CreateIndex(descriptorAttribute, url, database, collection);

			return [=](const string &attribute, vector<int> &values, function<void(Mave::Mave&)> OnDatum) mutable
			{
				Access::MongoClient::Query(OnDatum, url, database, collection, attribute, values);
			};
		}

		static
			auto
			LoadDuplicateDataElastic(
				const string &url
				, const string &index
				, const string &type)
		{
			return [=](const string &attribute, vector<int> &values, function<void(Mave::Mave&)> OnDatum) mutable
			{
				vector<string> vv; for (auto v : values) vv.push_back(to_string(v));
				Access::ElasticClient::Search(OnDatum, url, index, type, attribute, vv);
			};
		}
	};
}