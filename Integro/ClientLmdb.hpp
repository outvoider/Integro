#pragma once

#include <sstream>

#include "build_compability_includes/real/win32compability.h"
#include "lmdb.h"

namespace Integro
{
	using namespace std;

	class ClientLmdb
	{
	public:
		static string Get(const string &path, const string &key)
		{
			string value;

			if (!TryGet(path, key, value))
			{
				throw exception("ClientLmdb::Get(): failed");
			}

			return value;
		}

		static string GetOrDefault(const string &path, const string &key)
		{
			string value;
			TryGet(path, key, value);
			return value;
		}

		static bool TryGet(const string &path, const string &key, string &value)
		{
			value = "";

			int rc;
			MDB_env *env;
			MDB_dbi dbi;
			MDB_val key_p, data_p;
			MDB_txn *txn;

			rc = mdb_env_create(&env);
			rc |= mdb_env_open(env, path.c_str(), 0, 0664);
			rc |= mdb_txn_begin(env, NULL, 0, &txn);
			rc |= mdb_open(txn, NULL, 0, &dbi);

			key_p.mv_data = (void*)key.data();
			key_p.mv_size = strlen((const char*)key_p.mv_data);

			rc |= mdb_get(txn, dbi, &key_p, &data_p);

			if (rc == 0)
			{
				value.assign((const char*)data_p.mv_data, data_p.mv_size);
			}

			mdb_txn_abort(txn);
			mdb_close(env, dbi);
			mdb_env_close(env);

			return (rc == 0);
		}


		static void Set(const string &path, const string &key, const string &value)
		{
			int rc;
			MDB_env *env;
			MDB_dbi dbi;
			MDB_val key_p, data_p;
			MDB_txn *txn;

			rc = mdb_env_create(&env);
			rc |= mdb_env_open(env, path.c_str(), 0, 0664);
			rc |= mdb_txn_begin(env, NULL, 0, &txn);
			rc |= mdb_open(txn, NULL, 0, &dbi);

			key_p.mv_data = (void*)key.data();
			key_p.mv_size = strlen((const char*)key_p.mv_data);
			data_p.mv_data = (void*)value.data();
			data_p.mv_size = strlen((const char*)data_p.mv_data);

			rc |= mdb_put(txn, dbi, &key_p, &data_p, 0);
			rc |= mdb_txn_commit(txn);
			mdb_close(env, dbi);
			mdb_env_close(env);

			if (rc != 0)
			{
				throw exception("ClientLmdb::Set(): failed");
			}
		}

		static void Remove(const string &path, const string &key)
		{
			int rc;
			MDB_env *env;
			MDB_dbi dbi;
			MDB_val key_p, data_p;
			MDB_txn *txn;

			rc = mdb_env_create(&env);
			rc |= mdb_env_open(env, path.c_str(), 0, 0664);
			rc |= mdb_txn_begin(env, NULL, 0, &txn);
			rc |= mdb_open(txn, NULL, 0, &dbi);

			key_p.mv_data = (void*)key.data();
			key_p.mv_size = strlen((const char*)key_p.mv_data);

			rc |= mdb_del(txn, dbi, &key_p, NULL);

			if (rc == 0)
			{
				rc |= mdb_txn_commit(txn);
			}
			else
			{
				mdb_txn_abort(txn);
			}

			mdb_close(env, dbi);
			mdb_env_close(env);

			if (rc != 0)
			{
				throw exception("ClientLmdb::Remove(): failed");
			}
		}
	};
}
