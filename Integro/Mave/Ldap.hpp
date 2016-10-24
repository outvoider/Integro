#pragma once

#include "Mave/Mave.hpp"

#include "LDAPAsynConnection.h"
#include "LDAPSearchResult.h"
#include "LDAPResult.h"

namespace Integro
{
	namespace Mave
	{
		Mave FromLdap(const LDAPEntry &entry)
		{
			map<string, Mave> mm;
			auto al = entry.getAttributes();

			for (auto a = al->begin(); a != al->end(); ++a)
			{
				auto an = a->getName();
				auto vl = a->getValues();

				if (0 == vl.size())
				{
					mm.insert({ an, nullptr });
				}
				else if (1 == vl.size())
				{
					mm.insert({ an, *vl.begin() });
				}
				else
				{
					vector<Mave> mv;
					for (auto v = vl.begin(); v != vl.end(); ++v)
					{
						mv.push_back(*v);
					}
					mm.insert({ an, mv });
				}
			}

			return mm;
		}
	}
}