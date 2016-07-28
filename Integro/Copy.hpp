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

#include "Access.hpp"
#include "Synchronized.hpp"
#include "Milliseconds.hpp"
#include "Mave.hpp"

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
			auto hasFinishedLoadingData = false;
			auto hasFinishedSavingData = false;
			auto hasFailed = false;
			atomic_flag lock = ATOMIC_FLAG_INIT;
			string error;

			auto TryAbort = [&]()
			{
				if (hasFailed)
				{
					throw exception("Copy::CopyDataInChunks(): abortion requested due to errors");
				}
			};

			auto OnError = [&](function<void()> action, bool &hasFinished)
			{
				try
				{
					action();
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
				hasFinished = true;
			};

			auto LoadDataA = [&]()
			{
				LoadData(startTime, [&](Datum &datum)
				{
					while (buffer.Size() > 10000)
					{
						TryAbort();
						this_thread::sleep_for(chrono::milliseconds(1));
					}

					TryAbort();
					buffer.AddOne(datum);
				});
			};

			auto SaveDataA = [&]()
			{
				while (!hasFinishedLoadingData || !buffer.IsEmpty())
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
			};

			thread SaveDataThread(OnError, SaveDataA, hasFinishedSavingData);
			OnError(LoadDataA, hasFinishedLoadingData);

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
			function<void(milliseconds, function<void(Mave&)>)>
			LoadDataTds(
			const string &host
			, const string &user
			, const string &password
			, const string &database
			, const string &query)
		{
			return [=](milliseconds startTime, function<void(Mave&)> OnDatum) mutable
			{
				// TEMPORARY SOLUTION NOTICE:
				// subtract 1 second from startTime to compensate for addition of 1 second in a query
				// must be removed when queries are fixed
				auto q = regex_replace(query
					, regex("\\$\\(LAST_EXEC_TIME\\)")
					, startTime <= milliseconds(1000)
					? "CONVERT(datetime, '1970-01-01')"
					: "convert(datetime, '" + MillisecondsToUtc(startTime - milliseconds(1000), true) + "')");

				ClientTds::ExecuteQuery(host, user, password, database, q, OnDatum);
			};
		}

		static
			function<void(milliseconds, function<void(Mave&)>)>
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
			return [=](milliseconds startTime, function<void(Mave&)> OnDatum) mutable
			{
				ClientLdap::Search(host, port, user, password, node, filter, idAttribute, timeAttribute, startTime, milliseconds::zero(), OnDatum, OnError, OnEvent);
			};
		}

		static
			function<void(milliseconds, function<void(Mave&)>)>
			LoadDataMongo(
			const string &url
			, const string &database
			, const string &collection
			, const string &timeAttribute)
		{
			mongo::IndexSpec index; index.addKey(timeAttribute);
			ClientMongo::CreateIndex(url, database, collection, index);

			return [=](milliseconds startTime, function<void(Mave&)> OnDatum) mutable
			{
				ClientMongo::Query(url, database, collection, timeAttribute, startTime, milliseconds::zero(), OnDatum);
			};
		}

		static
			function<void(OID&, function<void(Mave&)>)>
			LoadCappedDataMongo(
			const string &url
			, const string &database
			, const string &collection)
		{
			return [=](OID &startId, function<void(Mave&)> OnDatum) mutable
			{
				ClientMongo::QueryCapped(url, database, collection, startId, OnDatum);
			};
		}

		// SaveData

		static
			function<void(vector<Mave>&)>
			SaveDataMongo(
			const string &url
			, const string &database
			, const string &collection)
		{
			return [=](vector<Mave> &data) mutable
			{
				ClientMongo::Upsert(url, database, collection, data);
			};
		}

		static
			function<void(vector<Mave>&)>
			SaveDataElastic(
			const string &url
			, const string &index
			, const string &type)
		{
			return [=](vector<Mave> &data) mutable
			{
				ClientElastic::Index(data, url, index, type);
			};
		}

		// ProcessData

		static
			function<void(vector<Mave>&)>
			ProcessDataTds(
			const string &channelName
			, const string &modelName
			, const string &model
			, const string &action
			, const vector<string> &targetStores)
		{
			return [=](vector<Mave> &data) mutable
			{
				for (auto & datum : data)
				{
					auto &m = datum.AsMap();

					//m["_uid"] = ;	should be there, although not unique
					m["action"] = action;
					m["channel"] = channelName;
					m["model"] = model;
					m["modelName"] = modelName;
					m["processed"] = 0;
					m["start_time"] = UtcToMilliseconds(m["start_time"].AsString());

					if (m.count("forType") > 0)
					{
						m["modelName"] = m["forType"].AsString();
						m["model"] = regex_replace(m["forType"].AsString(), regex("[[:punct:]|[:space:]]"), "");
					}

					if (targetStores.size() > 0)
					{
						m["targetStores"] = targetStores;
					}

					m["_id"] = boost::uuids::to_string(boost::uuids::random_generator()());
				}
			};
		}

		static
			function<void(vector<Mave>&)>
			ProcessDataLdap(
			const string &idAttribute
			, const string &channelName
			, const string &modelName
			, const string &model
			, const string &action)
		{
			return [=](vector<Mave> &data) mutable
			{
				for (auto & datum : data)
				{
					// DateTime
					{
						for (auto a :
						{
							"msExchWhenMailboxCreated"
							, "msTSExpireDate"
							, "whenChanged"
							, "whenCreated"
							, "dSCorePropagationData"
						})
						{
							if (datum.AsMap().count(a) > 0)
							{
								auto &o = datum[a];

								if (!o.IsVector())
								{
									o = max(milliseconds::zero(), LdapTimeToMilliseconds(o.AsString()));
								}
								else
								{
									for (auto &i : o.AsVector())
									{
										i = max(milliseconds::zero(), LdapTimeToMilliseconds(i.AsString()));
									}
								}
							}
						}
					}

					auto &m = datum.AsMap();

					m["_uid"] = m[idAttribute].AsString();
					m["action"] = action;
					m["channel"] = channelName;
					m["model"] = model;
					m["modelName"] = modelName;
					m["processed"] = 0;
					m["start_time"] = duration_cast<milliseconds>(chrono::system_clock::now().time_since_epoch());
					m["_id"] = m[idAttribute].AsString();
				}
			};
		}

		static
			function<void(vector<Mave>&)>
			ProcessDataLdapElastic()
		{
			return [=](vector<Mave> &data) mutable
			{
				for (auto & datum : data)
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
							if (datum.AsMap().count(a) > 0)
							{
								auto &o = datum[a];

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
							if (datum.AsMap().count(a) > 0)
							{
								auto &o = datum[a];

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
			function<void(vector<Mave>&)>
			ProcessDataMongo(
			const string &channelName
			, const string &modelName
			, const string &model
			, const string &action)
		{
			return [=](vector<Mave> &data) mutable
			{
				for (auto & datum : data)
				{
					auto &m = datum.AsMap();

					m["_uid"] = m["_id"];
					m["action"] = action;
					m["channel"] = channelName;
					m["model"] = model;
					m["modelName"] = modelName;
					m["processed"] = 0;
					//m["start_time"] = ;	should be there
					m["_id"] = boost::uuids::to_string(boost::uuids::random_generator()());
				}
			};
		}

		// Metadata

		static
			function<milliseconds()>
			LoadStartTimeLmdb(
			const string &path
			, const string &key)
		{
			return [=]() mutable
			{
				return milliseconds(stoull("0" + ClientLmdb::GetOrDefault(path, key)));
			};
		}

		static
			function<OID()>
			LoadStartIdLmdb(
			const string &path
			, const string &key)
		{
			return [=]() mutable
			{
				auto value = ClientLmdb::GetOrDefault(path, key);
				return value == "" ? OID() : OID(value);
			};
		}

		static
			function<void(milliseconds)>
			SaveStartTimeLmdb(
			const string &path
			, const string &key)
		{
			return [=](milliseconds time) mutable
			{
				ClientLmdb::Set(path, key, to_string(time.count()));
			};
		}

		static
			function<void(OID&)>
			SaveStartIdLmdb(
			const string &path
			, const string &key)
		{
			return [=](OID &id) mutable
			{
				ClientLmdb::Set(path, key, id.toString());
			};
		}

		static
			function<milliseconds(Mave&)>
			GetTimeTds(
			const string &timeAttribute)
		{
			return [=](Mave &datum) mutable
			{
				return UtcToMilliseconds(datum[timeAttribute].AsString());
			};
		}

		static
			function<milliseconds(Mave&)>
			GetTimeLdap(
			const string &timeAttribute)
		{
			return [=](Mave &datum) mutable
			{
				//return ConversionMilliseconds::FromLdapTime(datum[timeAttribute].AsString());
				//return max(milliseconds::zero(), ConversionMilliseconds::FromLdapTime(datum[timeAttribute].AsString()) - milliseconds(25 * 60 * 60 * 1000));
				return milliseconds::zero();
			};
		}

		static
			function<milliseconds(Mave&)>
			GetTimeMongo(
			const string &timeAttribute)
		{
			return [=](Mave &datum) mutable
			{
				return datum[timeAttribute].AsMilliseconds();
			};
		}

		static
			function<OID(Mave&)>
			GetIdMongo(
			const string &idAttribute)
		{
			return [=](Mave &datum) mutable
			{
				return datum[idAttribute].AsBsonOid();
			};
		}

		// Duplicates

		static
			function<void(vector<Mave>&)>
			RemoveDuplicates(
			const string &idAttribute
			, const string &descriptorAttribute
			, function<void(const string&, vector<int>&, function<void(Mave&)>)> LoadData)
		{
			return [=](vector<Mave> &data) mutable
			{
				if (data.size() == 0)
				{
					return;
				}

				vector<int> descriptorValues;

				for (auto &datum : data)
				{
					auto &m = datum.AsMap();
					auto id = m[idAttribute];
					m.erase(idAttribute);
					m.erase(descriptorAttribute);
					auto hash = Hash(datum);
					m[descriptorAttribute] = hash;
					m[idAttribute] = id;
					descriptorValues.push_back(hash);
				}

				set<int> storedDescriptors;
				set<string> storedStrings;

				LoadData(descriptorAttribute, descriptorValues, [&](Mave &datum)
				{
					auto &m = datum.AsMap();
					auto id = m[idAttribute];
					auto hash = m[descriptorAttribute];
					m.erase(idAttribute);
					m.erase(descriptorAttribute);
					storedStrings.insert(ToString(datum));
					m[idAttribute] = id;
					m[descriptorAttribute] = hash;
					storedDescriptors.insert(hash.AsInt());
				});

				if (storedDescriptors.size() == 0)
				{
					return;
				}

				vector<Mave> noDuplicatesData;

				for (auto &datum : data)
				{
					if (storedDescriptors.count(datum[descriptorAttribute].AsInt()) == 0)
					{
						noDuplicatesData.push_back(datum);
					}
					else
					{
						auto &m = datum.AsMap();
						auto id = m[idAttribute];
						auto hash = m[descriptorAttribute];
						m.erase(idAttribute);
						m.erase(descriptorAttribute);
						auto n = storedStrings.count(ToString(datum));
						m[idAttribute] = id;
						m[descriptorAttribute] = hash;

						if (n == 0)
						{
							noDuplicatesData.push_back(datum);
						}
					}
				}

				data = move(noDuplicatesData);
			};
		}

		static
			function<void(const string&, vector<int>&, function<void(Mave&)>)>
			LoadDuplicateDataMongo(
			const string &url
			, const string &database
			, const string &collection
			, const string &descriptorAttribute)
		{
			mongo::IndexSpec index; index.addKey(descriptorAttribute);
			ClientMongo::CreateIndex(url, database, collection, index);

			return [=](const string &attribute, vector<int> &values, function<void(Mave&)> OnDatum) mutable
			{
				mongo::BSONArrayBuilder ba; for (auto value : values) ba.append(value);
				auto query = MONGO_QUERY(attribute << BSON("$in" << ba.arr())).sort(attribute);
				ClientMongo::Query(url, database, collection, query, OnDatum);
			};
		}

		static
			function<void(const string&, vector<int>&, function<void(Mave&)>)>
			LoadDuplicateDataElastic(
			const string &url
			, const string &index
			, const string &type)
		{
			return [=](const string &attribute, vector<int> &values, function<void(Mave&)> OnDatum) mutable
			{
				vector<string> vv; for (auto v : values) vv.push_back(to_string(v));
				ClientElastic::Search(OnDatum, url, index, type, attribute, vv);
			};
		}
	};
}