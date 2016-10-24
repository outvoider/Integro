#pragma once

#include "Mave/Mave.hpp"

#include "json11.hxx"

namespace Integro
{
	namespace Mave
	{
		using std::to_string;

		Mave FromJson(json11::Json json)
		{
			Mave result;
			vector<function<bool()>> continuations;
			auto root = json;

			while (true)
			{
				switch (json.type())
				{
					case json11::Json::NUL:
						result = nullptr;
						break;
					case json11::Json::BOOL:
						result = json.bool_value();
						break;
					case json11::Json::NUMBER:
						result = json.number_value();
						break;
					case json11::Json::STRING:
						result = json.string_value();
						break;
					case json11::Json::OBJECT:
						continuations.push_back(
							[&
							, m = map<string, Mave>()
							, i = json.object_items().cbegin()
							, e = json.object_items().cend()
							, f = false]() mutable -> bool
						{
							if (f)
							{
								m.insert({ i->first, result });
								++i;
							}
							if (i != e)
							{
								json = i->second;
								return f = true;
							}
							result = move(m);
							return false;
						});
						break;
					case json11::Json::ARRAY:
						continuations.push_back(
							[&
							, v = vector<Mave>()
							, i = json.array_items().cbegin()
							, e = json.array_items().cend()
							, f = false]() mutable -> bool
						{
							if (f)
							{
								v.push_back(result);
								++i;
							}
							if (i != e)
							{
								json = *i;
								return f = true;
							}
							result = move(v);
							return false;
						});
						break;
					default:
						throw exception("Mave::FromJson(): unsupported type encountered");
				}

				while (true)
				{
					if (continuations.size() == 0)
					{
						return result;
					}
					if (continuations.back()())
					{
						break;
					}
					continuations.pop_back();
				}
			}
		}

		json11::Json ToJson(Mave mave)
		{
			json11::Json result;
			vector<function<bool()>> continuations;
			auto root = mave;

			while (true)
			{
				switch (mave.GetType())
				{
					case MAVE_NULL:
						result = nullptr;
						break;
					case MAVE_BOOL:
						result = mave.AsBool();
						break;
					case MAVE_INT:
						result = mave.AsInt();
						break;
					case MAVE_LONG:
						result = to_string(mave.AsLong());
						break;
					case MAVE_DOUBLE:
						result = mave.AsDouble();
						break;
					case MAVE_MILLISECONDS:
						result = Milliseconds::ToUtc(mave.AsMilliseconds(), true);
						break;
					case MAVE_STRING:
						result = mave.AsString();
						break;
					case MAVE_CUSTOM:
						result = mave.AsCustom().second;
						break;
					case MAVE_MAP:
						continuations.push_back(
							[&
							, m = json11::Json::object()
							, i = mave.AsMap().cbegin()
							, e = mave.AsMap().cend()
							, f = false]() mutable -> bool
						{
							if (f)
							{
								m.insert({ i->first, result });
								++i;
							}
							if (i != e)
							{
								mave = i->second;
								return f = true;
							}
							result = move(m);
							return false;
						});
						break;
					case MAVE_VECTOR:
						continuations.push_back(
							[&
							, v = json11::Json::array()
							, i = mave.AsVector().cbegin()
							, e = mave.AsVector().cend()
							, f = false]() mutable -> bool
						{
							if (f)
							{
								v.push_back(result);
								++i;
							}
							if (i != e)
							{
								mave = *i;
								return f = true;
							}
							result = move(v);
							return false;
						});
						break;
					default:
						throw exception("Mave::ToJson(): unsupported type encountered");
				}

				while (true)
				{
					if (continuations.size() == 0)
					{
						return result;
					}
					if (continuations.back()())
					{
						break;
					}
					continuations.pop_back();
				}
			}
		}
	}
}