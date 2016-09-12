#pragma once

#include <sstream>
#include <fstream>
#include <thread>

#include <boost/filesystem.hpp>

#include "json11.hxx"

#include "spdlog/spdlog.h"

#include "Copy.hpp"

namespace Integro
{
	using namespace std;
	using namespace json11;

	class Integro
	{
		bool isInitialized;
		static atomic_flag lock;

	public:
		static
			void
			OnError(
			const string &message)
		{
			while (lock.test_and_set());

			auto logger = spdlog::get("logger");

			if (logger == nullptr)
			{
				cerr << message << endl;
			}
			else
			{
				cerr << message << endl;
				logger->error() << message;
			}

			lock.clear();
		}

		static
			void
			OnEvent(
			const string &message)
		{
			while (lock.test_and_set());

			auto logger = spdlog::get("logger");

			if (logger == nullptr)
			{
				cerr << message << endl;
			}
			else
			{
				cerr << message << endl;
				logger->info() << message;
			}

			lock.clear();
		}

	private:
		Json config;
		string environment;
		string metadataPath;

		void
			Proceed(
			function<void()> action
			, function<void(const string&)> OnError)
		{
			try
			{
				action();
			}
			catch (const exception &ex)
			{
				stringstream s; s
					<< "an action has failed, error message is '"
					<< ex.what()
					<< "'";
				OnError(s.str());
			}
			catch (...)
			{
				OnError("an unspecified exception has been caught");
			}
		}

		void
			Retry(
			function<void()> action
			, function<void(const string&)> OnError
			, const int errorTolerance = 0
			, const int attemptsCount = 10
			, const milliseconds pauseBetweenAttempts = milliseconds(1000))
		{
			for (int attempt = 1;; ++attempt)
			{
				try
				{
					action();
				}
				catch (const exception &ex)
				{
					stringstream s;

					if (errorTolerance < 1 && attempt == attemptsCount)
					{
						s
							<< "an action has failed, error message is '"
							<< ex.what()
							<< "'";
						OnError(s.str());
						throw;
					}

					s
						<< "an action has failed, attempt "
						<< attempt
						<< " out of "
						<< (errorTolerance > 0 ? "infinity" : to_string(attemptsCount))
						<< ", next attempt in "
						<< pauseBetweenAttempts.count()
						<< " milliseconds, error message is '"
						<< ex.what()
						<< "'";
					OnError(s.str());
					this_thread::sleep_for(pauseBetweenAttempts);
				}
				catch (...)
				{
					stringstream s;

					if (errorTolerance < 2 && attempt == attemptsCount)
					{
						OnError("an action has failed");
						throw;
					}

					s
						<< "an action has failed, attempt "
						<< attempt
						<< " out of "
						<< (errorTolerance > 1 ? "infinity" : to_string(attemptsCount))
						<< ", next attempt in "
						<< pauseBetweenAttempts.count()
						<< " milliseconds";
					OnError(s.str());
					this_thread::sleep_for(pauseBetweenAttempts);
				}
			}
		}

		string
			Concatenate(
			const Json &stringArray)
		{
			stringstream s;

			for (auto &i : stringArray.array_items())
			{
				s << i.string_value();
			}

			return s.str();
		}

		vector<string>
			ToStringVector(
			const Json &stringArray)
		{
			vector<string> strings;

			for (auto &i : stringArray.array_items())
			{
				strings.push_back(i.string_value());
			}

			return strings;
		}

		vector<pair<string, function<void()>>>
			CreateTdsActions()
		{
			vector<pair<string, function<void()>>> actions;

			auto &mongo = config["mongo"];
			auto &elastic = config["elastic"];
			auto &tds = config["tds"];

			auto &mongoConnectionOne = mongo["connections"]["one"][environment];
			auto &elasticConnectionOne = elastic["connections"]["one"][environment];

			for (auto &channel : tds["channels"].object_items())
			{
				auto &proxy = tds["proxies"][channel.first][environment];
				auto &connection = tds["connections"][channel.first][environment];

				for (auto &topic : channel.second.array_items())
				{
					auto metadataKey = topic["name"].string_value();
					auto tdsHost = channel.first;
					auto tdsUser = connection["user"].string_value();
					auto tdsPassword = connection["pass"].string_value();
					auto tdsDatabase = connection["database"].string_value();
					auto tdsQuery = Concatenate(topic["script"]);
					auto elasticUrl = elasticConnectionOne["host"].string_value() + ":" + elasticConnectionOne["port"].string_value();
					auto elasticIndex = elasticConnectionOne["index"].string_value();
					auto elasticType = topic["name"].string_value();
					auto mongoUrl = "mongodb://" + mongoConnectionOne["host"].string_value() + ":" + mongoConnectionOne["port"].string_value();
					auto mongoDatabase = mongoConnectionOne["database"].string_value();
					auto mongoCollection = topic["name"].string_value();
					auto idAttribute = "_id";
					auto descriptorAttribute = "descriptor";
					auto sourceAttribute = "source";
					auto channelName = channel.first;
					auto modelName = topic["modelName"].string_value();
					auto model = topic["model"].string_value();
					auto action = topic["name"].string_value();
					auto targetStores = ToStringVector(topic["targetStores"]);
					auto timeAttribute = "start_time";

					auto LoadData = Copy::LoadDataTds(tdsHost, tdsUser, tdsPassword, tdsDatabase, tdsQuery);
					auto ProcessData = Copy::ProcessDataTds(channelName, modelName, model, action, targetStores);
					auto LoadDuplicateData = Copy::LoadDuplicateDataMongo(mongoUrl, mongoDatabase, mongoCollection, descriptorAttribute);
					auto RemoveDuplicates = Copy::RemoveDuplicates(descriptorAttribute, sourceAttribute, LoadDuplicateData);
					auto SaveDataMongo = Copy::SaveDataMongo(mongoUrl, mongoDatabase, mongoCollection);
					auto SaveDataElastic = Copy::SaveDataElastic(elasticUrl, elasticIndex, elasticType);
					auto SaveData = [=](vector<Mave> &data) mutable
					{
						ProcessData(data);
						RemoveDuplicates(data);
						SaveDataMongo(data);

						// TEMPORARY SOLUTION NOTICE:
						// disable saving to elasticsearch if it is not present in config.json
						if (elasticUrl != ":")
						{
							SaveDataElastic(data);
						}
					};
					auto LoadStartTime = Copy::LoadStartTimeLmdb(metadataPath, metadataKey);
					auto SaveStartTime = Copy::SaveStartTimeLmdb(metadataPath, metadataKey);
					auto GetTime = Copy::GetTimeTds(timeAttribute);
					auto CopyData = [=]() mutable
					{
						// TEMPORARY SOLUTION NOTICE:
						// Change to Copy::CopyDataInChunks when all tds queries provide sorted data

						//Copy::CopyDataInChunks<Mave, milliseconds>(LoadData, SaveData, LoadStartTime, SaveStartTime, GetTime);
						Copy::CopyDataInBulk<Mave, milliseconds>(LoadData, SaveData, LoadStartTime, SaveStartTime, GetTime);
					};

					actions.push_back(make_pair(action, CopyData));
				}
			}

			return actions;
		}

		vector<pair<string, function<void()>>>
			CreateLdapActions()
		{
			vector<pair<string, function<void()>>> actions;

			auto &mongo = config["mongo"];
			auto &elastic = config["elastic"];
			auto &ldap = config["ldap"];

			auto &mongoConnectionOne = mongo["connections"]["one"][environment];
			auto &elasticConnectionOne = elastic["connections"]["one"][environment];

			for (auto &channel : ldap["channels"].object_items())
			{
				auto &connection = ldap["connections"][channel.first][environment];

				for (auto &topic : channel.second.array_items())
				{
					auto timeAttribute = topic["timeAttribute"].string_value();
					auto metadataKey = topic["name"].string_value();
					auto ldapHost = connection["host"].string_value();
					auto ldapPort = stoi(connection["port"].string_value());
					auto ldapUser = connection["user"].string_value();
					auto ldapPassword = connection["pass"].string_value();
					auto ldapNode = topic["node"].string_value();
					auto ldapFilter = topic["filter"].string_value();
					auto ldapIdAttribute = topic["idAttribute"].string_value();
					auto elasticUrl = elasticConnectionOne["host"].string_value() + ":" + elasticConnectionOne["port"].string_value();
					auto elasticIndex = elasticConnectionOne["index"].string_value();
					auto elasticType = topic["name"].string_value();
					auto mongoUrl = "mongodb://" + mongoConnectionOne["host"].string_value() + ":" + mongoConnectionOne["port"].string_value();
					auto mongoDatabase = mongoConnectionOne["database"].string_value();
					auto mongoCollection = topic["name"].string_value();
					auto channelName = channel.first;
					auto modelName = "ldap";
					auto model = "ldap";
					auto action = topic["name"].string_value();

					auto LoadData = Copy::LoadDataLdap(ldapHost, ldapPort, ldapUser, ldapPassword, ldapNode, ldapFilter, ldapIdAttribute, timeAttribute, OnError, OnEvent);
					auto ProcessDataMongo = Copy::ProcessDataLdap(ldapIdAttribute, channelName, modelName, model, action);
					auto SaveDataMongo = Copy::SaveDataMongo(mongoUrl, mongoDatabase, mongoCollection);
					auto ProcessDataElastic = Copy::ProcessDataLdapElastic();
					auto SaveDataElastic = Copy::SaveDataElastic(elasticUrl, elasticIndex, elasticType);
					auto SaveData = [=](vector<Mave> &data) mutable
					{
						ProcessDataMongo(data);
						SaveDataMongo(data);

						// TEMPORARY SOLUTION NOTICE:
						// disable saving to elasticsearch if it is not present in config.json
						if (elasticUrl != ":")
						{
							ProcessDataElastic(data);
							SaveDataElastic(data);
						}
					};
					auto LoadStartTime = Copy::LoadStartTimeLmdb(metadataPath, metadataKey);
					auto SaveStartTime = Copy::SaveStartTimeLmdb(metadataPath, metadataKey);
					auto GetTime = Copy::GetTimeLdap(timeAttribute);
					auto CopyData = [=]() mutable
					{
						Copy::CopyDataInChunks<Mave, milliseconds>(LoadData, SaveData, LoadStartTime, SaveStartTime, GetTime);
					};

					actions.push_back(make_pair(action, CopyData));
				}
			}

			return actions;
		}

	public:
		Integro(
			int argc
			, wchar_t* argv[])
			: isInitialized(false)
		{
			using namespace boost::filesystem;

			if (!is_directory("./logs") && !create_directory("./logs"))
			{
				OnError("failed to create 'logs' directory");
				return;
			}

			spdlog::daily_logger_mt("logger", "logs/integro_log", 0, 0, true);

			metadataPath = "metadata";

			if (!is_directory(metadataPath) && !create_directory(metadataPath))
			{
				OnError("failed to create '" + metadataPath + "' directory");
				return;
			}

			if (argc != 3
				|| string((char*)argv[1]) != "--env"
				|| (string((char*)argv[2]) != "dev"
				&& string((char*)argv[2]) != "staging"
				&& string((char*)argv[2]) != "prod"))
			{
				OnError("[--env {dev, staging, prod}]");
				return;
			}

			environment = (char*)argv[2];

			stringstream configBuffer;
			ifstream configInput("configs\\config.json");
			configBuffer << configInput.rdbuf();
			string configError;
			config = Json::parse(configBuffer.str(), configError);

			if (configError != "")
			{
				OnError("failed to parse config file");
				return;
			}

			isInitialized = true;
		}

		void Run()
		{
			if (!isInitialized)
			{
				return;
			}

			auto ExecuteTdsAction = [&]()
			{
				auto period = milliseconds(config["tds"]["settings"]["program"]["sleep ms"].int_value());
				auto actions = CreateTdsActions();

				while (true)
				{
					int i = 0;
					for (auto action : actions)
					{
						OnEvent("tds action # " + to_string(++i) + " is starting, action name is '" + action.first + "'");
						Proceed(action.second, OnError);
					}

					OnEvent("tds actions are being paused for " + to_string(period.count()) + " milliseconds");
					this_thread::sleep_for(period);
				}
			};

			auto ExecuteLdapAction = [&]()
			{
				auto actions = CreateLdapActions();

				while (true)
				{
					int i = 0;
					for (auto action : actions)
					{
						OnEvent("ldap action # " + to_string(++i) + " is starting, action name is '" + action.first + "'");
						Proceed(action.second, OnError);
					}
				}
			};

			thread ExecuteLdapActionThread(ExecuteLdapAction);
			ExecuteTdsAction();
		}
	};

	atomic_flag Integro::lock = ATOMIC_FLAG_INIT;
	string configPath = "configs/client.conf";
	//string configPath = "configs/direct_client.conf";
	string applicationName = "Integro";
	ClientTds::Infin ClientTds::infin(configPath, applicationName, Integro::OnError, Integro::OnEvent);
}