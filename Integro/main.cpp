#define BSONCXX_STATIC
#define MONGOCXX_STATIC

#include <mongocxx/instance.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/options/create_collection.hpp>

#if defined(_WIN32) || defined(_WIN64)
#include <winsock2.h>
#include <windows.h>
#endif

#include "Integro.hpp"
#include "Debug.hpp"

auto INTEGRO_VERSION = "2.5";

int
main(
	int argc
	, wchar_t* argv[])
{
	mongocxx::instance inst{};

	//Integro::Debug().Run();
	Integro::Integro(argc, argv).Run();

	return 0;
}