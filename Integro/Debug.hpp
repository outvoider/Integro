#pragma once

#include <sstream>
#include <fstream>

#include "Integro.hpp"

namespace Integro
{
	class Debug
	{
		minstd_rand rand;
		atomic_flag lock;

		string environment;
		string metadataPath;
		string startIdKey;
		string startTimeKey;

		string idAttribute;
		string descriptorAttribute;
		string sourceAttribute;
		string timeAttribute;

		string channelName;
		string topicName;
		string modelName;
		string model;
		vector<string> targetStores;

		string tdsHost;
		string tdsUser;
		string tdsPassword;
		string tdsDatabase;
		string tdsQuery;
		vector<string> tdsCollections;

		string mongoUrl;
		string mongoDatabase;
		string mongoCollection;
		string mongoCapped;

		string elasticUrl;
		string elasticIndex;
		string elasticType;

		string ldapHost;
		int ldapPort;
		string ldapUser;
		string ldapPassword;
		string ldapNode;
		string ldapFilter;
		string ldapIdAttribute;
		string ldapTimeAttribute;
		vector<string> ldapCollections;

		string Concatenate(const Json &stringArray)
		{
			stringstream s;

			for (auto &i : stringArray.array_items())
			{
				s << i.string_value();
			}

			return s.str();
		}

		string Escape(string input)
		{
			string output;
			json11::dump(input, output);
			return output;
		}

		void Print(const string &s)
		{
			while (lock.test_and_set());
			cout << s << endl;
			lock.clear();
		}

		void Print(const string &s, const int n)
		{
			while (lock.test_and_set());
			cout << s << n << endl;
			lock.clear();
		}

		void Print(const string &s, const int n0, const int n1)
		{
			while (lock.test_and_set());
			cout << s << n0 << " " << n1 << endl;
			lock.clear();
		}

		void Print(const string &s, const int n0, const int n1, const int n2)
		{
			while (lock.test_and_set());
			cout << s << n0 << " " << n1 << " " << n2 << endl;
			lock.clear();
		}

		void Print(const string &s, const int n0, const int n1, const int n2, const int n3)
		{
			while (lock.test_and_set());
			cout << s << n0 << " " << n1 << " " << n2 << " " << n3 << endl;
			lock.clear();
		}

		int Rand()
		{
			int r;
			while (lock.test_and_set());
			r = rand();
			lock.clear();
			return r;
		}

		void TryThrow(const int n, const string &s)
		{
			if (Rand() % (100 * n) == 0)
			{
				throw exception(s.c_str());
			}
		}

		void Retry(function<void()> action)
		{
			for (int n = 0;;)
			{
				try
				{
					action();
					break;
				}
				catch (const exception &ex)
				{
					Print(string(ex.what()) + "\t", ++n);
				}
				catch (...)
				{
					Print("", ++n);
				}
			}
		}

		void TdsQuery()
		{
			ClientTds::ExecuteQuery(tdsHost, tdsUser, tdsPassword, tdsDatabase, tdsQuery, [&](const Mave &datum)
			{
				cout << Escape(ToString(datum)) << endl;
			});
		}

		void LdapQuery()
		{
			auto OnData = [&](const Mave &datum)
			{
				cout << Escape(ToString(datum)) << endl;
			};

			auto OnMessage = [&](const string &message)
			{
				cerr << message << endl;
			};

			ClientLdap::Search(ldapHost, ldapPort, ldapUser, ldapPassword, ldapNode, ldapFilter, ldapIdAttribute, ldapTimeAttribute, milliseconds::zero(), milliseconds::zero(), OnData, OnMessage, OnMessage);
		}

		void MongoCreateCapped()
		{
			ClientMongo::CreateCollection(mongoUrl, mongoDatabase, mongoCapped, true, 1000);
		}

		void MongoDropDatabase()
		{
			ClientMongo::DropDatabase(mongoUrl, mongoDatabase);
		}

		void MongoDropCollection()
		{
			ClientMongo::DropCollection(mongoUrl, mongoDatabase, mongoCollection);
		}

		void MongoCount()
		{
			auto n = ClientMongo::Count(mongoUrl, mongoDatabase, mongoCollection);
			cout << n << endl;
		}

		void MongoQuery()
		{
			ClientMongo::Query(mongoUrl, mongoDatabase, mongoCollection, mongo::Query(), [&](const Mave &mave)
			{
				cout << Escape(ToString(mave)) << endl;
			});
		}

		void MongoProduce()
		{
			for (int n = 0;;)
			{
				string s;
				cout << "Enter the number of objects to add or type 'exit':" << endl;
				cin >> s;

				if (s == "exit")
				{
					break;
				}

				int i = atoi(s.c_str());

				while (--i >= 0)
				{
					auto time = mongo::Date_t(duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count());
					auto object = ToMave(BSON(mongo::GENOID << "#" << ++n << "start_time" << time));
					ClientMongo::Insert(mongoUrl, mongoDatabase, mongoCollection, object);
					ClientMongo::Insert(mongoUrl, mongoDatabase, mongoCapped, object);
					this_thread::sleep_for(chrono::microseconds(1));
				}

				cout << "Done." << endl << endl;
			}
		}

		void MongoProduceUpsert()
		{
			for (int n = 0;;)
			{
				string s;
				cout << "Enter the number of objects to add or type 'exit':" << endl;
				cin >> s;

				if (s == "exit")
				{
					break;
				}

				int i = atoi(s.c_str());
				vector<Mave> objects;

				while (--i >= 0)
				{
					auto time = mongo::Date_t(duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count());
					auto object = ToMave(BSON(mongo::GENOID << "#" << ++n << "start_time" << time));
					objects.push_back(object);
				}

				ClientMongo::Upsert(mongoUrl, mongoDatabase, mongoCollection, objects);
				ClientMongo::Upsert(mongoUrl, mongoDatabase, mongoCapped, objects);

				cout << "Done." << endl << endl;
			}
		}

		void ElasticCreateIndex()
		{
			ClientElastic::CreateIndex(elasticUrl, elasticIndex);
		}

		void ElasticDeleteIndex()
		{
			ClientElastic::DeleteIndex(elasticUrl, elasticIndex);
		}

		void ElasticDeleteType()
		{
			ClientElastic::DeleteType(elasticUrl, elasticIndex, elasticType);
		}

		void ElasticCount()
		{

			auto n = ClientElastic::Count(elasticUrl, elasticIndex, elasticType);
			cout << n << endl;
		}

		void ElasticQuery()
		{
			ClientElastic::Search([&](const Mave &object)
			{
				cout << Escape(ToString(object)) << endl;
			}, elasticUrl, elasticIndex, elasticType);
		}

		void CopyTds()
		{
			auto LoadData = Copy::LoadDataTds(tdsHost, tdsUser, tdsPassword, tdsDatabase, tdsQuery);
			auto ProcessData = Copy::ProcessDataTds(channelName, modelName, model, topicName, targetStores);
			auto LoadDuplicateData = Copy::LoadDuplicateDataMongo(mongoUrl, mongoDatabase, mongoCollection, descriptorAttribute);
			auto RemoveDuplicates = Copy::RemoveDuplicates(descriptorAttribute, sourceAttribute, LoadDuplicateData);
			auto SaveDataMongo = Copy::SaveDataMongo(mongoUrl, mongoDatabase, mongoCollection);
			auto SaveDataElastic = Copy::SaveDataElastic(elasticUrl, elasticIndex, elasticType);
			auto SaveData = [=](vector<Mave> &data) mutable
			{
				ProcessData(data);
				RemoveDuplicates(data);
				SaveDataMongo(data);
				SaveDataElastic(data);
			};
			auto LoadStartTime = Copy::LoadStartTimeLmdb(metadataPath, startTimeKey);
			auto SaveStartTime = Copy::SaveStartTimeLmdb(metadataPath, startTimeKey);
			auto GetTime = Copy::GetTimeTds(timeAttribute);
			auto CopyData = [=]() mutable
			{
				Copy::CopyDataInBulk<Mave, milliseconds>(LoadData, SaveData, LoadStartTime, SaveStartTime, GetTime);
			};

			Retry(CopyData);
		}

		void CopyCorrectnessTest()
		{
			string dataAttribute = "data";
			bool hasMoreData = true;
			int idCount = 0;
			int startTime = 0;
			vector<Mave> source;
			map<int, Mave> dest;
			multimap<int, Mave> index;

			for (int i = 0, t = 0; i < 100000; ++i)
			{
				source.push_back(Mave(map<string, Mave>({ { sourceAttribute, map<string, Mave>({ { dataAttribute, i } }) }, { timeAttribute, (Rand() % 10 != 0) ? t : t++ } })));
			}

			auto LoadData = [&](int startTime, function<void(Mave&)> OnDatum)
			{
				auto i = lower_bound(source.begin(), source.end(), Mave(map<string, Mave>({ { timeAttribute, startTime } })), [&](const Mave &a, const Mave &b)
				{
					return a[timeAttribute].AsInt() < b[timeAttribute].AsInt();
				});

				if (i == source.end())
				{
					throw exception("failed to find start time");
				}

				for (; i != source.end(); ++i)
				{
					TryThrow(500, "failed to load data");
					//Print("loaded datum: " + ToString(*i));
					OnDatum(*i);
				}

				hasMoreData = false;
			};

			auto RemoveDuplicates = Copy::RemoveDuplicates(descriptorAttribute, sourceAttribute
				, [&](const string &attribute, vector<int> &values, function<void(Mave&)> OnDatum)
			{
				TryThrow(1, "failed to remove duplicates");

				for (auto descriptor : values)
				{
					auto range = index.equal_range(descriptor);

					for (auto i = range.first; i != range.second; ++i)
					{
						OnDatum(i->second);
					}
				}
			});

			auto SaveData = [&](vector<Mave> &data)
			{
				for (auto &datum : data)
				{
					TryThrow(100, "failed to process data");
					datum.AsMap()[idAttribute] = idCount++;
					//Print("processed datum: " + ToString(datum));
				}

				RemoveDuplicates(data);

				for (auto &datum : data)
				{
					TryThrow(100, "failed to save data");
					dest.insert({ datum[idAttribute].AsInt(), datum });
					index.insert({ datum[descriptorAttribute].AsInt(), datum });
					//Print("saved datum: " + ToString(datum));
				}
			};

			auto LoadStartTime = [&]()
			{
				TryThrow(1, "failed to load start time");
				//Print("loaded start time: ", startTime);
				return startTime;
			};

			auto SaveStartTime = [&](int time)
			{
				TryThrow(1, "failed to save start time");
				startTime = time;
				//Print("saved start time: ", startTime);
			};

			auto GetTime = [&](Mave &datum)
			{
				TryThrow(100, "failed to get time");
				//Print("got time: ", datum[timeAttribute].AsInt());
				return datum[timeAttribute].AsInt();
			};

			auto CopyData = [&]()
			{
				Copy::CopyDataInChunks<Mave, int>(LoadData, SaveData, LoadStartTime, SaveStartTime, GetTime);
			};

			while (hasMoreData)
			{
				Retry(CopyData);
			}

			cout << "--------------------------------" << endl;

			auto result = source.size() == dest.size();
			map<int, Mave> sourceData;
			map<int, Mave> destData;

			for (auto d : source)
			{
				if (!sourceData.insert({ d[sourceAttribute][dataAttribute].AsInt(), d }).second)
				{
					result = false;
					cout << "duplicate in source data: " << ToString(d) << endl;
				}
			}

			for (auto d : dest)
			{
				if (!destData.insert({ d.second[sourceAttribute][dataAttribute].AsInt(), d.second }).second)
				{
					result = false;
					cout << "duplicate in destination data: " << ToString(d.second) << endl;
				}
			}

			for (auto d : sourceData)
			{
				if (destData.count(d.first) == 0)
				{
					result = false;
					cout << "not in destination data: " << ToString(d.second) << endl;
				}
			}

			for (auto d : destData)
			{
				if (sourceData.count(d.first) == 0)
				{
					result = false;
					cout << "not in source data: " << ToString(d.second) << endl;
				}
			}

			if (result)
			{
				cout << "CopyCorrectnessTest(): succeeded" << endl;
			}
			else
			{
				cout << "source size: " << source.size() << endl;
				cout << "destination size: " << dest.size() << endl;
				cout << "CopyCorrectnessTest(): failed" << endl;
			}
		}

		void CopyGeneralTest()
		{
			string countAttribute = "count";
			bool hasMoreData = true;
			vector<Mave> source;

			{
				auto time = milliseconds::zero();
				vector<string> text;

				for (int i = 0; i < 26; ++i)
				{
					text.push_back(string(1000 + i, i + 'a'));
				}

				for (int i = 0; i < 26; ++i)
				{
					text.push_back(string(1000 + 26 + i, i + 'A'));
				}

				for (int i = 0; i < 10000; ++i)
				{
					map<string, Mave> m;
					m[countAttribute] = i;
					m[timeAttribute] = MillisecondsToUtc(time, true);
					auto &s = (m[sourceAttribute] = map<string, Mave>()).AsMap();

					for (int i = 0; i < text.size(); ++i)
					{
						s[to_string(i)] = text[i];
					}

					source.push_back(m);
					if (Rand() % 10 == 0) time += milliseconds(1000);
				}
			}

			auto LoadData = [&](milliseconds startTime, function<void(Mave&)> OnDatum)
			{
				auto i = lower_bound(source.begin(), source.end(), Mave(startTime), [&](const Mave &a, const Mave &b)
				{
					return a.IsMilliseconds()
						? a.AsMilliseconds() < UtcToMilliseconds(b[timeAttribute].AsString())
						: UtcToMilliseconds(a[timeAttribute].AsString()) < b.AsMilliseconds();
				});

				if (i == source.end())
				{
					throw exception("failed to find start time");
				}

				for (; i != source.end(); ++i)
				{
					TryThrow(50, "failed to load data");
					OnDatum(Copy(*i));
				}

				hasMoreData = false;
			};

			auto ProcessData = Copy::ProcessDataTds(channelName, modelName, model, topicName, targetStores);
			auto LoadDuplicateData = Copy::LoadDuplicateDataMongo(mongoUrl, mongoDatabase, mongoCollection, descriptorAttribute);
			auto RemoveDuplicates = Copy::RemoveDuplicates(descriptorAttribute, sourceAttribute, LoadDuplicateData);
			auto SaveDataMongo = Copy::SaveDataMongo(mongoUrl, mongoDatabase, mongoCollection);
			auto SaveDataElastic = Copy::SaveDataElastic(elasticUrl, elasticIndex, elasticType);
			auto SaveData = [=](vector<Mave> &data) mutable
			{
				TryThrow(1, "failed to save data");
				ProcessData(data);
				RemoveDuplicates(data);
				SaveDataMongo(data);
				//SaveDataElastic(data);
			};
			auto LoadStartTimeA = Copy::LoadStartTimeLmdb(metadataPath, startTimeKey);
			auto LoadStartTime = [=]() mutable
			{
				TryThrow(1, "failed to load start time");
				return LoadStartTimeA();
			};
			auto SaveStartTimeA = Copy::SaveStartTimeLmdb(metadataPath, startTimeKey);
			auto SaveStartTime = [=](milliseconds time) mutable
			{
				TryThrow(1, "failed to save start time");
				SaveStartTimeA(time);
			};
			auto GetTimeA = Copy::GetTimeTds(timeAttribute);
			auto GetTime = [=](Mave &datum) mutable
			{
				TryThrow(10, "failed to get time");
				return GetTimeA(datum);
			};
			auto CopyData = [=]() mutable
			{
				Copy::CopyDataInChunks<Mave, milliseconds>(LoadData, SaveData, LoadStartTime, SaveStartTime, GetTime);
			};

			while (hasMoreData)
			{
				Retry(CopyData);
			}
		}

		void CopyPerformanceTest()
		{
			string countAttribute = "count";
			bool hasMoreData = true;
			int number = 0;
			int count = 0;
			vector<string> sv;
			vector<Mave> maves;
			milliseconds start;

			{
				for (int i = 0; i < 26; ++i)
				{
					sv.push_back(string(1000 + i, i + 'a'));
				}

				for (int i = 0; i < 26; ++i)
				{
					sv.push_back(string(1000 + 26 + i, i + 'A'));
				}

				start = duration_cast<milliseconds>(chrono::system_clock::now().time_since_epoch());
			}

			auto LoadData = [&](milliseconds startTime, function<void(Mave&)> OnDatum)
			{
				auto time = startTime;

				for (auto &m : maves)
				{
					OnDatum(Copy(m));
				}

				while (true)
				{
					auto diff = duration_cast<milliseconds>(chrono::system_clock::now().time_since_epoch()) - start;

					if (diff > milliseconds(1 * 1 * 60000))
					{
						break;
					}

					if (diff >= number * milliseconds(60000))
					{
						++number;
						cout << "loaded records count: " << count << endl;
					}

					if (Rand() % 10 == 0)
					{
						time += milliseconds(1000);
						maves.clear();
					}

					map<string, Mave> m;
					m[timeAttribute] = MillisecondsToUtc(time, true);
					m[countAttribute] = ++count;
					auto &s = (m[sourceAttribute] = map<string, Mave>()).AsMap();

					for (int i = 0; i < sv.size(); ++i)
					{
						s[to_string(i)] = sv[i];
					}

					maves.push_back(m);
					OnDatum(Copy(m));

					if (Rand() % 1000 == 0)
					{
						//cout << "modeled interrupt" << endl;
						return;
					}
				}

				hasMoreData = false;
				cout << "loaded records count: " << count << endl;
				cout << "finished loading data, current time: " << MillisecondsToUtc(duration_cast<milliseconds>(chrono::system_clock::now().time_since_epoch())) << endl;
			};

			auto ProcessData = Copy::ProcessDataTds(channelName, modelName, model, topicName, targetStores);
			auto LoadDuplicateData = Copy::LoadDuplicateDataMongo(mongoUrl, mongoDatabase, mongoCollection, descriptorAttribute);
			auto RemoveDuplicates = Copy::RemoveDuplicates(descriptorAttribute, sourceAttribute, LoadDuplicateData);
			auto SaveDataMongo = Copy::SaveDataMongo(mongoUrl, mongoDatabase, mongoCollection);
			auto SaveDataElastic = Copy::SaveDataElastic(elasticUrl, elasticIndex, elasticType);
			auto SaveData = [=](vector<Mave> &data) mutable
			{
				ProcessData(data);
				RemoveDuplicates(data);
				SaveDataMongo(data);
				//SaveDataElastic(data);
			};
			auto LoadStartTime = Copy::LoadStartTimeLmdb(metadataPath, startTimeKey);
			auto SaveStartTime = Copy::SaveStartTimeLmdb(metadataPath, startTimeKey);
			auto GetTime = Copy::GetTimeTds(timeAttribute);
			auto CopyData = [=]() mutable
			{
				Copy::CopyDataInChunks<Mave, milliseconds>(LoadData, SaveData, LoadStartTime, SaveStartTime, GetTime);
			};

			cout << "started processing data, current time: " << MillisecondsToUtc(duration_cast<milliseconds>(chrono::system_clock::now().time_since_epoch())) << endl;

			while (hasMoreData)
			{
				Retry(CopyData);
			}

			cout << "finished saving data, current time: " << MillisecondsToUtc(duration_cast<milliseconds>(chrono::system_clock::now().time_since_epoch())) << endl;
		}

		void CopyCappedCorrectnessTest()
		{
			struct TestDatum
			{
				int id;
				int time;
			};

			atomic_flag lock = ATOMIC_FLAG_INIT;
			int startId = 0;
			int startTime = 0;
			int currentIndex = 0;
			vector<TestDatum> source;
			map<int, TestDatum> dest;

			for (int i = 0, t = 0; i < 100000; ++i)
			{
				TestDatum d;

				d.id = i;
				d.time = (Rand() % 10 != 0) ? t : t++;

				source.push_back(d);
			}

			auto LoadCappedData = [&](int &_startId, function<void(TestDatum&)> OnDatum)
			{
				assert(_startId <= currentIndex);

				for (; currentIndex < source.size(); ++currentIndex)
				{
					if (Rand() % 50000 == 0)
					{
						//Print("capped collection is empty");
						break;
					}

					if (Rand() % 50000 == 0)
					{
						currentIndex = min(currentIndex + Rand() % 10, source.size());
						throw exception("capped collection overflow");
					}

					auto &datum = source[currentIndex];
					TryThrow(500, "failed to load capped datum");
					//Print("loaded capped datum, id: ", datum.id);
					OnDatum(datum);
				}
			};

			auto LoadData = [&](int _startTime, function<void(TestDatum&)> OnDatum)
			{
				auto index = min(currentIndex, source.size() - 1);
				int i = _startTime;

				assert(i <= index);

				for (; i <= index; ++i)
				{
					if (source[i].time == _startTime)
					{
						break;
					}
				}

				for (; i <= index; ++i)
				{
					auto &datum = source[i];
					TryThrow(500, "failed to load datum");
					//Print("loaded datum, id: ", datum.id);
					OnDatum(datum);
				}
			};

			auto SaveData = [&](vector<TestDatum> &data)
			{
				for (auto &datum : data)
				{
					TryThrow(100, "failed to process datum");
					//Print("processed datum, id: ", datum.id);
				}

				for (auto &datum : data)
				{
					TryThrow(100, "failed to save datum");
					while (lock.test_and_set());
					dest[datum.id] = datum;
					lock.clear();
					//Print("saved datum, id: ", datum.id);
				}
			};

			auto LoadStartTime = [&]()
			{
				TryThrow(1, "failed to load start time");
				//Print("loaded start time: ", startTime);
				return startTime;
			};

			auto LoadStartId = [&]()
			{
				TryThrow(1, "failed to load start id");
				//Print("loaded start id: ", startId);
				return startId;
			};

			auto SaveStartTime = [&](int time)
			{
				TryThrow(1, "failed to save start time");
				while (lock.test_and_set());
				startTime = time;
				lock.clear();
				//Print("saved start time: ", startTime);
			};

			auto SaveStartId = [&](int id)
			{
				TryThrow(1, "failed to save start id");
				while (lock.test_and_set());
				startId = id;
				lock.clear();
				//Print("saved start id: ", startId);
			};

			auto GetTime = [&](TestDatum &datum)
			{
				TryThrow(500, "failed to get time");
				//Print("got time: ", datum.time);
				return datum.time;
			};

			auto GetId = [&](TestDatum &datum)
			{
				TryThrow(500, "failed to get id");
				//Print("got id: ", datum.id);
				return datum.id;
			};

			auto CopyData = [&]()
			{
				Copy::CopyCappedDataInChunks<TestDatum, int, int>(LoadCappedData, LoadData, SaveData, LoadStartTime, LoadStartId, SaveStartTime, SaveStartId, GetTime, GetId);
			};

			while (startId < source.size() - 1)
			{
				Retry(CopyData);
			}

			cout << "--------------------------------" << endl;
			cout << "source size: " << source.size() << endl;
			cout << "destination size: " << dest.size() << endl;

			auto result = source.size() == dest.size();

			if (!result)
			{
				cout << "bug" << endl;
			}

			for (auto d : source)
			{
				if (dest.count(d.id) == 0)
				{
					result = false;
					cout << "id: " << d.id << " is not in destination data" << endl;
				}
				else
				{
					dest.erase(d.id);
				}
			}

			for (auto d : dest)
			{
				cout << "id: " << d.second.id << " is not in source data" << endl;
			}

			if (result)
			{
				cout << "CopyCappedCorrectnessTest(): succeeded" << endl;
			}
			else
			{
				cout << "CopyCappedCorrectnessTest(): failed" << endl;
			}
		}

		void JsonBson()
		{
			map<string, Mave> o1 = { { "#", 1 }, { "d", vector<Mave>({ "Monday", "Tuesday" }) } };
			Mave m1 = o1;
			auto b1 = ToBson(m1);

			mongo::BSONObjBuilder bo;
			mongo::BSONArrayBuilder ba;
			ba.append("Monday");
			ba.append("Tuesday");
			bo.append("#", 1);
			bo.append("d", (mongo::BSONArray)((mongo::BSONObj)ba.arr()));
			auto b2 = bo.obj();

			cout << ToString(m1) << endl;
			cout << "-----------------------------------" << endl;
			cout << b1.jsonString() << endl;
			cout << "-----------------------------------" << endl;
			cout << b2.jsonString() << endl;

			//Mave l("aaa");
			//vector<Mave> a1 = { nullptr, true, l, "bbb" };
			//map<string, Mave> o1 = { { "#", 1 }, { "complex", false } };
			//vector<Mave> a2 = { 3, milliseconds(1), "ccc", a1, o1 };
			//map<string, Mave> o2 = { { "l", 2 }, { "complex", true }, { "object", o1 }, { "array", a1 }, { "array2", a2 } };

			//auto bo = ToBson(o2);
			//auto mo1 = ToMave(bo);
			//auto jo = ToJson(mo1);
			//auto mo2 = ToMave(jo);

			//cout << ToString(o2) << endl;
			//cout << "-----------------------------------" << endl;
			//cout << bo.jsonString() << endl;
			//cout << "-----------------------------------" << endl;
			//cout << ToString(mo1) << endl;
			//cout << "-----------------------------------" << endl;
			//cout << jo.dump() << endl;
			//cout << "-----------------------------------" << endl;
			//cout << ToString(mo2) << endl;
			//cout << "-----------------------------------" << endl;
		}

		void PrintCopyCounts()
		{
			for (auto &collection : tdsCollections)
			{
				cout << "collection name: '" << collection << "'" << endl;
				cout << "mongo count: " << ClientMongo::Count(mongoUrl, mongoDatabase, collection) << endl;
				cout << "elastic count: " << ClientElastic::Count(elasticUrl, elasticIndex, collection) << endl;
			}

			for (auto &collection : ldapCollections)
			{
				cout << "collection name: '" << collection << "'" << endl;
				cout << "mongo count: " << ClientMongo::Count(mongoUrl, mongoDatabase, collection) << endl;
				cout << "elastic count: " << ClientElastic::Count(elasticUrl, elasticIndex, collection) << endl;
			}
		}

		void LmdbTest()
		{
			int n = 8;
			string path = "test";
			string key = "test";
			string data = "test";

			auto g = [&](int i)
			{
				while (true)
				{
					try
					{
						string value = ClientLmdb::GetOrDefault(path, key);
						//cout << "g" << i << ": [" << key << "] = " << value << endl;
					}
					catch (const exception &ex)
					{
						stringstream a; a << "g" << i << ": " << ex.what();
						Print(a.str());
					}
				}
			};

			auto s = [&](int i)
			{
				while (true)
				{
					try
					{
						ClientLmdb::Set(path, key, data);
						//cout << "s" << i << ": [" << key << "] = " << data << endl;
					}
					catch (const exception &ex)
					{
						stringstream a; a << "s" << i << ": " << ex.what();
						Print(a.str());
					}
				}
			};

			vector<shared_ptr<thread>> gg;
			vector<shared_ptr<thread>> ss;

			for (int i = 0; i < n; ++i)
			{
				gg.push_back(shared_ptr<thread>(new thread(g, i)));
			}

			for (int i = 0; i < n; ++i)
			{
				ss.push_back(shared_ptr<thread>(new thread(s, i)));
			}

			g(n);
		}

	public:
		Debug()
			: rand(random_device()())
		{
			lock.clear();

			stringstream buffer;
			string error;
			ifstream input("configs\\config.json");
			buffer << input.rdbuf();
			auto config = Json::parse(buffer.str(), error);

			if (error != "")
			{
				throw exception("failed to parse config file");
			}

			environment = "staging";
			metadataPath = "metadata";
			startIdKey = "test_integro:id";
			startTimeKey = "test_integro:start_time";

			idAttribute = "_id";
			descriptorAttribute = "descriptor";
			sourceAttribute = "source";
			timeAttribute = "start_time";

			channelName = "test";
			topicName = "test";
			modelName = "test";
			model = "test";
			targetStores = { "dummy" };

			auto &tds = config["tds"];
			auto &tdsConnection = tds["connections"].object_items().begin()->second[environment];
			tdsHost = tdsConnection["host"].string_value();
			tdsUser = tdsConnection["user"].string_value();
			tdsPassword = tdsConnection["pass"].string_value();
			tdsDatabase = tdsConnection["database"].string_value();
			tdsQuery = Concatenate(tds["channels"][tds["connections"].object_items().begin()->first][0]["script"]);

			for (auto &channel : tds["channels"].object_items())
			{
				for (auto &topic : channel.second.array_items())
				{
					tdsCollections.push_back(topic["name"].string_value());
				}
			}

			auto &mongo = config["mongo"];
			auto &mongoConnection = mongo["connections"]["one"][environment];
			mongoUrl = "mongodb://" + mongoConnection["host"].string_value() + ":" + mongoConnection["port"].string_value();;
			mongoDatabase = mongoConnection["database"].string_value();
			mongoCollection = "destination";
			mongoCapped = "capped";

			auto &elastic = config["elastic"];
			auto &elasticConnection = elastic["connections"]["one"][environment];
			elasticUrl = elasticConnection["host"].string_value() + ":" + elasticConnection["port"].string_value();
			elasticIndex = elasticConnection["index"].string_value();
			elasticType = "destination";

			auto &ldap = config["ldap"];
			auto &ldapConnection = ldap["connections"].object_items().begin()->second[environment];
			auto &ldapTopic = ldap["channels"].object_items().begin()->second[0];
			ldapHost = ldapConnection["host"].string_value();
			ldapPort = stoi(ldapConnection["port"].string_value());
			ldapUser = ldapConnection["user"].string_value();
			ldapPassword = ldapConnection["pass"].string_value();
			ldapNode = ldapTopic["node"].string_value();
			ldapFilter = ldapTopic["filter"].string_value();
			ldapIdAttribute = ldapTopic["idAttribute"].string_value();
			ldapTimeAttribute = ldapTopic["timeAttribute"].string_value();

			for (auto &channel : ldap["channels"].object_items())
			{
				for (auto &topic : channel.second.array_items())
				{
					ldapCollections.push_back(topic["name"].string_value());
				}
			}

			//mongoCollection = elasticType = ldapCollections[0];
			//mongoCollection = elasticType = tdsCollections[1];
		}

		void Run()
		{
			//LmdbTest();
			//JsonBson();
			//PrintCopyCounts();

			//CopyTds();
			//CopyCorrectnessTest();
			//CopyGeneralTest();
			//CopyPerformanceTest();
			//CopyCappedCorrectnessTest();

			//TdsQuery();
			//LdapQuery();
			//MongoProduce();
			//MongoProduceUpsert();
			//MongoCreateCapped();
			//MongoDropCollection();
			//MongoDropDatabase();
			//MongoCount();
			//MongoQuery();
			//ElasticCreateIndex();
			//ElasticDeleteIndex();
			//ElasticDeleteType();
			//ElasticCount();
			//ElasticQuery();
		}
	};
}