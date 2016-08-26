#pragma once

#include <memory>
#include <initializer_list>
#include <vector>
#include <map>
#include <string>
#include <chrono>

#include "LDAPAsynConnection.h"
#include "LDAPSearchResult.h"
#include "LDAPResult.h"
#include "mongo/client/dbclient.h"
#include "json11.hxx"

#include "Milliseconds.hpp"
#include "Hash.hpp"

namespace Integro
{
	using std::string;
	using std::vector;
	using std::map;
	using std::shared_ptr;
	using std::make_shared;
	using std::exception;
	using std::initializer_list;
	using std::move;
	using std::chrono::milliseconds;
	using mongo::OID;
	using json11::Json;

	class Mave final
	{
		enum Type;

		class Value
		{
		public:
			virtual Type GetType() const = 0;
		};

		struct Literals
		{
			static shared_ptr<Value> Null;
			static shared_ptr<Value> True;
			static shared_ptr<Value> False;
		};

		template <Type type>
		class EValue final : public Value
		{
		public:
			Type GetType() const override { return type; }
		};

		template <Type type, typename T>
		class VValue final : public Value
		{
			T value;
		public:
			VValue(T value) : value(value) {}
			Type GetType() const override { return type; }
			T GetValue() { return value; }
		};

		template <Type type, typename T>
		class RValue final : public Value
		{
			T value;
		public:
			RValue(T& value) : value(value) {}
			RValue(T&& value) : value(move(value)) {}
			Type GetType() const override { return type; }
			T& GetValue() { return value; }
		};

		shared_ptr<Value> value;

	public:
		enum Type
		{
			NULL_T
			, VECTOR
			, MAP
			, BOOL
			, INT
			, LONG
			, DOUBLE
			, STRING
			, MILLISECONDS
			, BSON_OID
		};

	private:
		typedef EValue<NULL_T> NullT;
		typedef RValue<VECTOR, vector<Mave>> Vector;
		typedef RValue<MAP, map<string, Mave>> Map;
		typedef VValue<BOOL, bool> Bool;
		typedef VValue<INT, int> Int;
		typedef VValue<LONG, long long> Long;
		typedef VValue<DOUBLE, double> Double;
		typedef RValue<STRING, string> String;
		typedef VValue<MILLISECONDS, milliseconds> Milliseconds;
		typedef RValue<BSON_OID, OID> BsonOid;

	public:
		Type GetType() const { return value->GetType(); }
		bool HasType(const Type type) const { return type == value->GetType(); }
		void Assert(const Type type) const { if (!HasType(type)) {
			throw exception("Mave::Assert(): invalid type"); } }

		Mave() : value(Literals::Null) {}
		Mave(nullptr_t) : value(Literals::Null) {}
		bool IsNullptr() const { return HasType(NULL_T); }
		nullptr_t AsNullptr() const { Assert(NULL_T); return nullptr; }

		template <class V, typename std::enable_if<
			std::is_constructible<Mave, typename V::value_type>::value,
			int>::type = 0>
			Mave(const V &v) : Mave(vector<Mave>(v.begin(), v.end())) {}
		Mave(vector<Mave> &value) : value(make_shared<Vector>(value)) {}
		Mave(vector<Mave> &&value) : value(make_shared<Vector>(move(value))) {}
		bool IsVector() const { return HasType(VECTOR); }
		vector<Mave>& AsVector() const { Assert(VECTOR); return ((Vector*)value.get())->GetValue(); }
		Mave& operator[](int index) const { return AsVector().at(index); }

		template <class M, typename std::enable_if<
			std::is_constructible<std::string, typename M::key_type>::value
			&& std::is_constructible<Mave, typename M::mapped_type>::value,
			int>::type = 0>
			Mave(const M &m) : Mave(map<string, Mave>(m.begin(), m.end())) {}
		Mave(map<string, Mave> &value) : value(make_shared<Map>(value)) {}
		Mave(map<string, Mave> &&value) : value(make_shared<Map>(move(value))) {}
		bool IsMap() const { return HasType(MAP); }
		map<string, Mave>& AsMap() const { Assert(MAP); return ((Map*)value.get())->GetValue(); }
		Mave& operator[](const string &key) const { return AsMap().at(key); }

		Mave(void *) = delete;
		Mave(bool value) : value(value ? Literals::True : Literals::False) {}
		bool IsBool() const { return HasType(BOOL); }
		bool AsBool() const { Assert(BOOL); return ((Bool*)value.get())->GetValue(); }

		Mave(int value) : value(make_shared<Int>(value)) {}
		bool IsInt() const { return HasType(INT); }
		int AsInt() const { Assert(INT); return ((Int*)value.get())->GetValue(); }

		Mave(long long value) : value(make_shared<Long>(value)) {}
		bool IsLong() const { return HasType(LONG); }
		long long AsLong() const { Assert(LONG); return ((Long*)value.get())->GetValue(); }

		Mave(double value) : value(make_shared<Double>(value)) {}
		bool IsDouble() const { return HasType(DOUBLE); }
		double AsDouble() const { Assert(DOUBLE); return ((Double*)value.get())->GetValue(); }

		Mave(const string &value) : value(make_shared<String>(const_cast<string&>(value))) {}
		Mave(string &&value) : value(make_shared<String>(move(value))) {}
		Mave(const char *value) : value(make_shared<String>(const_cast<char*>(value))) {}
		bool IsString() const { return HasType(STRING); }
		string& AsString() const { Assert(STRING); return ((String*)value.get())->GetValue(); }

		Mave(milliseconds value) : value(make_shared<Milliseconds>(value)) {}
		bool IsMilliseconds() const { return HasType(MILLISECONDS); }
		milliseconds AsMilliseconds() const { Assert(MILLISECONDS); return ((Milliseconds*)value.get())->GetValue(); }

		Mave(OID &value) : value(make_shared<BsonOid>(value)) {}
		Mave(OID &&value) : value(make_shared<BsonOid>(move(value))) {}
		bool IsBsonOid() const { return HasType(BSON_OID); }
		OID& AsBsonOid() const { Assert(BSON_OID); return ((BsonOid*)value.get())->GetValue(); }
	};

	shared_ptr<Mave::Value> Mave::Literals::Null = make_shared<NullT>();
	shared_ptr<Mave::Value> Mave::Literals::True = make_shared<Bool>(true);
	shared_ptr<Mave::Value> Mave::Literals::False = make_shared<Bool>(false);

	Mave Copy(const Mave &mave)
	{
		switch (mave.GetType())
		{
			case Mave::Type::NULL_T:
				return nullptr;
			case Mave::Type::BOOL:
				return mave.AsBool();
			case Mave::Type::INT:
				return mave.AsInt();
			case Mave::Type::LONG:
				return mave.AsLong();
			case Mave::Type::DOUBLE:
				return mave.AsDouble();
			case Mave::Type::MILLISECONDS:
				return mave.AsMilliseconds();
			case Mave::Type::BSON_OID:
				return mave.AsBsonOid();
			case Mave::Type::STRING:
				return mave.AsString();
			case Mave::Type::MAP:
			{
				map<string, Mave> m;
				for (auto &i : mave.AsMap())
				{
					m.insert({ i.first, Copy(i.second) });
				}
				return m;
			}
			case Mave::Type::VECTOR:
			{
				vector<Mave> v;
				for (auto &i : mave.AsVector())
				{
					v.push_back(Copy(i));
				}
				return v;
			}
			default:
				throw exception("Mave::Copy(): unsupported type encountered");
		}
	}

	void ToStringStream(const Mave &mave, stringstream &buffer)
	{
		switch (mave.GetType())
		{
			case Mave::Type::NULL_T:
				buffer << "(\"null_t\":\"null\")";
				break;
			case Mave::Type::BOOL:
				buffer << "(\"bool\":\"" << (mave.AsBool() ? "true" : "false") << "\")";
				break;
			case Mave::Type::INT:
				buffer << "(\"int\":\"" << mave.AsInt() << "\")";
				break;
			case Mave::Type::LONG:
				buffer << "(\"long\":\"" << mave.AsLong() << "\")";
				break;
			case Mave::Type::DOUBLE:
				buffer << "(\"double\":\"" << mave.AsDouble() << "\")";
				break;
			case Mave::Type::MILLISECONDS:
				buffer << "(\"milliseconds\":\"" << mave.AsMilliseconds().count() << "\")";
				break;
			case Mave::Type::BSON_OID:
				buffer << "(\"bson_oid\":\"" << mave.AsBsonOid().toString() << "\")";
				break;
			case Mave::Type::STRING:
				buffer << "(\"string\":\"" << mave.AsString() << "\")";
				break;
			case Mave::Type::MAP:
			{
				buffer << "{";
				auto &m = mave.AsMap();
				for (auto i = m.begin(); i != m.end();)
				{
					buffer << "\"" << i->first << "\":";
					ToStringStream(i->second, buffer);
					if (++i != m.end()) buffer << ",";
				}
				buffer << "}";
				break;
			}
			case Mave::Type::VECTOR:
			{
				buffer << "[";
				auto &v = mave.AsVector();
				for (auto i = v.begin(); i != v.end();)
				{
					ToStringStream(*i, buffer);
					if (++i != v.end()) buffer << ",";
				}
				buffer << "]";
				break;
			}
			default:
				throw exception("Mave::ToStringStream(): unsupported type encountered");
		}
	}

	string ToString(const Mave &mave)
	{
		stringstream buffer;
		ToStringStream(mave, buffer);
		return buffer.str();
	}

	int Hash(const Mave &mave)
	{
		return Hash(ToString(mave));
	}

	Mave ToMave(const LDAPEntry &ldap)
	{
		map<string, Mave> mm;
		auto al = ldap.getAttributes();

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

	Mave ToMave(const mongo::BSONObj &bson)
	{
		Mave r;
		map<string, Mave> m;
		vector<Mave> v;
		auto f = !bson.couldBeArray();

		for (auto i = bson.begin(); i.more();)
		{
			auto e = i.next();
			auto s = true;

			switch (e.type())
			{
				case mongo::BSONType::MinKey:
				case mongo::BSONType::EOO:
				case mongo::BSONType::MaxKey:
					s = false;
					break;
				case mongo::BSONType::jstNULL:
					r = nullptr;
					break;
				case mongo::BSONType::Bool:
					r = e.boolean();
					break;
				case mongo::BSONType::NumberInt:
					r = e.numberInt();
					break;
				case mongo::BSONType::NumberLong:
					r = e.numberLong();
					break;
				case mongo::BSONType::NumberDouble:
					r = e.numberDouble();
					break;
				case mongo::BSONType::Date:
					r = milliseconds(e.date().millis);
					break;
				case mongo::BSONType::String:
					r = e.str();
					break;
				case mongo::BSONType::jstOID:
					r = e.OID();
					break;
				case mongo::BSONType::Undefined:
				case mongo::BSONType::Timestamp:
				case mongo::BSONType::BinData:
				case mongo::BSONType::RegEx:
				case mongo::BSONType::Symbol:
				case mongo::BSONType::CodeWScope:
					r = e.toString();
					break;
				case mongo::BSONType::Object:
				case mongo::BSONType::Array:
					r = ToMave(e.Obj());
					break;
				default:
					throw exception("Mave::ToMave(): unsupported type encountered");
			}

			if (s) f ? m.insert({ e.fieldName(), r }) : v.push_back(r);
		}

		return f ? Mave(m) : v;
	}

	mongo::BSONObj ToBson(const Mave &mave)
	{
		mongo::BSONObjBuilder bo;
		mongo::BSONArrayBuilder ba;

		switch (mave.GetType())
		{
			case Mave::Type::NULL_T:
				ba.appendNull();
				break;
			case Mave::Type::BOOL:
				ba.append(mave.AsBool());
				break;
			case Mave::Type::INT:
				ba.append(mave.AsInt());
				break;
			case Mave::Type::LONG:
				ba.append(mave.AsLong());
				break;
			case Mave::Type::DOUBLE:
				ba.append(mave.AsDouble());
				break;
			case Mave::Type::MILLISECONDS:
				ba.appendDate(mongo::Date_t(mave.AsMilliseconds().count()));
				break;
			case Mave::Type::BSON_OID:
				ba.append(mave.AsBsonOid());
				break;
			case Mave::Type::STRING:
				ba.append(mave.AsString());
				break;
			case Mave::Type::MAP:
				for (auto &i : mave.AsMap())
				{
					switch (i.second.GetType())
					{
						case Mave::Type::NULL_T:
							bo.appendNull(i.first);
							break;
						case Mave::Type::BOOL:
							bo.append(i.first, i.second.AsBool());
							break;
						case Mave::Type::INT:
							bo.append(i.first, i.second.AsInt());
							break;
						case Mave::Type::LONG:
							bo.append(i.first, i.second.AsLong());
							break;
						case Mave::Type::DOUBLE:
							bo.append(i.first, i.second.AsDouble());
							break;
						case Mave::Type::MILLISECONDS:
							bo.appendDate(i.first, mongo::Date_t(i.second.AsMilliseconds().count()));
							break;
						case Mave::Type::BSON_OID:
							bo.append(i.first, i.second.AsBsonOid());
							break;
						case Mave::Type::STRING:
							bo.append(i.first, i.second.AsString());
							break;
						case Mave::Type::MAP:
						case Mave::Type::VECTOR:
							bo.append(i.first, ToBson(i.second));
							break;
						default:
							throw exception("Mave::ToBson(): unsupported type encountered");
					}
				}
				return bo.obj();
			case Mave::Type::VECTOR:
				for (auto &i : mave.AsVector())
				{
					switch (i.GetType())
					{
						case Mave::Type::NULL_T:
							ba.appendNull();
							break;
						case Mave::Type::BOOL:
							ba.append(i.AsBool());
							break;
						case Mave::Type::INT:
							ba.append(i.AsInt());
							break;
						case Mave::Type::LONG:
							ba.append(i.AsLong());
							break;
						case Mave::Type::DOUBLE:
							ba.append(i.AsDouble());
							break;
						case Mave::Type::MILLISECONDS:
							ba.appendDate(mongo::Date_t(i.AsMilliseconds().count()));
							break;
						case Mave::Type::BSON_OID:
							ba.append(i.AsBsonOid());
							break;
						case Mave::Type::STRING:
							ba.append(i.AsString());
							break;
						case Mave::Type::MAP:
						case Mave::Type::VECTOR:
							ba.append(ToBson(i));
							break;
						default:
							throw exception("Mave::ToBson(): unsupported type encountered");
					}
				}
				return ba.arr();
			default:
				throw exception("Mave::ToBson(): unsupported type encountered");
		}

		return ba.obj();
	}

	Mave ToMave(const Json &json)
	{
		switch (json.type())
		{
			case Json::Type::NUL:
				return nullptr;
			case Json::Type::BOOL:
				return json.bool_value();
			case Json::Type::NUMBER:
				return json.number_value();
			case Json::Type::STRING:
				return json.string_value();
			case Json::Type::OBJECT:
			{
				map<string, Mave> m;
				for (auto &i : json.object_items())
				{
					m.insert({ i.first, ToMave(i.second) });
				}
				return m;
			}
			case Json::Type::ARRAY:
			{
				vector<Mave> v;
				for (auto &i : json.array_items())
				{
					v.push_back(ToMave(i));
				}
				return v;
			}
			default:
				throw exception("Mave::ToMave(): unsupported type encountered");
		}

		throw exception("Mave::ToMave(): failed");
	}

	Json ToJson(const Mave &mave)
	{
		switch (mave.GetType())
		{
			case Mave::Type::NULL_T:
				return nullptr;
			case Mave::Type::BOOL:
				return mave.AsBool();
			case Mave::Type::INT:
				return mave.AsInt();
			case Mave::Type::LONG:
				return to_string(mave.AsLong());
			case Mave::Type::DOUBLE:
				return mave.AsDouble();
			case Mave::Type::MILLISECONDS:
				return MillisecondsToUtc(mave.AsMilliseconds(), true);
			case Mave::Type::BSON_OID:
				return mave.AsBsonOid().toString();
			case Mave::Type::STRING:
				return mave.AsString();
			case Mave::Type::MAP:
			{
				Json::object m;
				for (auto &i : mave.AsMap())
				{
					m.insert({ i.first, ToJson(i.second) });
				}
				return m;
			}
			case Mave::Type::VECTOR:
			{
				Json::array v;
				for (auto &i : mave.AsVector())
				{
					v.push_back(ToJson(i));
				}
				return v;
			}
			default:
				throw exception("Mave::ToJson(): unsupported type encountered");
		}

		throw exception("Mave::ToJson(): failed");
	}
}