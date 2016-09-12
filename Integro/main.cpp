#define STATIC_LIBMONGOCLIENT

#if defined(_WIN32) || defined(_WIN64)
#include <winsock2.h>
#include <windows.h>
#endif

#include "Integro.hpp"
#include "Debug.hpp"

auto INTEGRO_VERSION = "2.3";

int
	main(
	int argc
	, wchar_t* argv[])
{
	mongo::client::GlobalInstance mongoInstance;
	mongoInstance.assertInitialized();

	//Integro::Debug().Run();
	Integro::Integro(argc, argv).Run();

	return 0;
}