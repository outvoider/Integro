#define STATIC_LIBMONGOCLIENT

#if defined(_WIN32) || defined(_WIN64)
#include <winsock2.h>
#include <windows.h>
#endif

#include <sstream>
#include <fstream>

#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>

#include "json11.hxx"

#include "spdlog/spdlog.h"

#include "Debug.hpp"
#include "HostAction.hpp"

namespace Integro
{
	class MessageHandler
	{
		static atomic_flag lock;

	public:
		static void OnError(const string &message)
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

		static void OnEvent(const string &message)
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
	};

	atomic_flag MessageHandler::lock = ATOMIC_FLAG_INIT;

	string configPath = "configs/client.conf";
	//string configPath = "configs/direct_client.conf";
	string applicationName = "Integro";
	ClientTds::Infin ClientTds::infin(configPath, applicationName, MessageHandler::OnError, MessageHandler::OnError);
}

int main(int argc, wchar_t* argv[])
{
	mongo::client::GlobalInstance mongoInstance;
	mongoInstance.assertInitialized();

	//Debug::Run();

	using namespace std;
	using namespace boost::filesystem;
	//using namespace boost::program_options;
	using namespace json11;

	if (!is_directory("./logs") && !create_directory("./logs"))
	{
		cout << "failed to create 'logs' directory" << endl;
		return 1;
	}

	spdlog::daily_logger_mt("logger", "logs/integro_log", 0, 0, true);

	//options_description description("Allowed options");
	//description.add_options()
	//	("help", "produce help message")
	//	("env", value<string>(), "dev, staging, prod");

	//variables_map vm;
	//store(parse_command_line(argc, argv, description), vm);
	//notify(vm);

	//if (vm.size() != 1 || vm.count("help") || !vm.count("env"))
	//{
	//	cout << description << endl;
	//	return 0;
	//}

	//string environment = vm["env"].as<string>();

	if (argc != 3
		|| string((char*)argv[1]) != "--env"
		|| (string((char*)argv[2]) != "dev"
		&& string((char*)argv[2]) != "staging"
		&& string((char*)argv[2]) != "prod"))
	{
		cout << "[--env {dev, staging, prod}]" << endl;
		return 1;
	}

	string environment((char*)argv[2]);

	if (environment == "prod")
	{
		char answer;
		cout << "Are you sure you want to run the application in production mode (y/n)? ";

		do
		{
			cin >> answer;

			if (answer == 'n' || answer == 'N')
			{
				return 1;
			}
		}
		while (answer != 'y' && answer != 'Y');
	}

	stringstream configBuffer;

	ifstream configInput("configs\\config.json");
	configBuffer << configInput.rdbuf();

	string configError;
	auto config = Json::parse(configBuffer.str(), configError);

	if (configError != "")
	{
		cerr << "failed to parse config file" << endl;
		return 1;
	}

	string metadataPath("metadata");

	if (!is_directory(metadataPath) && !create_directory(metadataPath))
	{
		cerr << "failed to create '" << metadataPath << "' directory" << endl;
		return 1;
	}

	Integro::HostAction(config, environment, metadataPath, Integro::MessageHandler::OnError, Integro::MessageHandler::OnEvent)();

	return 0;
}