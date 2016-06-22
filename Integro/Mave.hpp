#pragma once

#include <memory>
#include <initializer_list>

#include <vector>
#include <map>
#include <string>
#include <chrono>
#include "mongo/client/dbclient.h"

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
			NULLPTR
			, VECTOR
			, MAP
			, BOOL
			, LONG
			, DOUBLE
			, STRING
			, MILLISECONDS
			, BSON_OID
		};

	private:
		typedef EValue<NULLPTR> Nullptr;
		typedef RValue<VECTOR, vector<Mave>> Vector;
		typedef RValue<MAP, map<string, Mave>> Map;
		typedef VValue<BOOL, bool> Bool;
		typedef VValue<LONG, long long> Long;
		typedef VValue<DOUBLE, double> Double;
		typedef RValue<STRING, string> String;
		typedef VValue<MILLISECONDS, milliseconds> Milliseconds;
		typedef RValue<BSON_OID, OID> BsonOid;

	public:
		Type GetType() const { return value->GetType(); }
		bool HasType(const Type type) const { return type == value->GetType(); }
		void Assert(const Type type) const { if (!HasType(type)) { throw exception("Mave::Assert(): invalid type"); } }

		Mave() : value(Literals::Null) {}
		Mave(nullptr_t) : value(Literals::Null) {}
		bool IsNullptr() const { return HasType(NULLPTR); }
		nullptr_t AsNullptr() const { Assert(NULLPTR); return nullptr; }

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
			Mave(const M &m) : Json(map<string, Mave>(m.begin(), m.end())) {}
		Mave(map<string, Mave> &value) : value(make_shared<Map>(value)) {}
		Mave(map<string, Mave> &&value) : value(make_shared<Map>(move(value))) {}
		bool IsMap() const { return HasType(MAP); }
		map<string, Mave>& AsMap() const { Assert(MAP); return ((Map*)value.get())->GetValue(); }
		Mave& operator[](const string &key) const { return AsMap().at(key); }

		Mave(void *) = delete;
		Mave(bool value) : value(value ? Literals::True : Literals::False) {}
		bool IsBool() const { return HasType(BOOL); }
		bool AsBool() const { Assert(BOOL); return ((Bool*)value.get())->GetValue(); }

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

	shared_ptr<Mave::Value> Mave::Literals::Null = make_shared<Nullptr>();
	shared_ptr<Mave::Value> Mave::Literals::True = make_shared<Bool>(true);
	shared_ptr<Mave::Value> Mave::Literals::False = make_shared<Bool>(false);
}