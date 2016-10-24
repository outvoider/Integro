#pragma once

#include <string>

namespace Integro
{
	using std::string;

	// fnv1a hash
	const int HASH_PRIME = 0x01000193;	// 16777619
	const int HASH_SEED = 0x811C9DC5;	// 2166136261

	inline
		int
		Hash(
			unsigned char data
			, int hash = HASH_SEED)
	{
		return (data ^ hash) * HASH_PRIME;
	}

	int
		Hash(
			const unsigned char *data
			, int count, int hash = HASH_SEED)
	{
		while (--count >= 0)
		{
			hash = Hash(*data++, hash);
		}

		return hash;
	}

	int
		Hash(
			const string &data)
	{
		return Hash((const unsigned char*)data.c_str(), data.size());
	}
}