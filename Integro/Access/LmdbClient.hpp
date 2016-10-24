#pragma once

#include <string>
#include <functional>

#include "build_compability_includes/real/win32compability.h"
#include "lmdb.h"

namespace Integro
{
	namespace Access
	{
		using std::string;
		using std::exception;
		using std::function;

		class LmdbClient
		{
		public:
			static
				string
				Get(
					const string &path
					, const string &key)
			{
				string value;

				if (!TryGet(path, key, value))
				{
					throw exception("LmdbClient::Get(): failed to get the value of a key");
				}

				return value;
			}

			static
				string
				GetOrDefault(
					const string &path
					, const string &key)
			{
				string value;
				TryGet(path, key, value);
				return value;
			}

			static
				bool
				TryGet(
					const string &path
					, const string &key
					, string &value)
			{
				value = "";

				int rc = 0;
				MDB_env *env = NULL;
				MDB_dbi dbi = 0;
				MDB_txn *txn = NULL;
				MDB_val key_p, data_p;
				key_p.mv_data = NULL;
				key_p.mv_size = 0;
				data_p.mv_data = NULL;
				data_p.mv_size = 0;

				rc = mdb_env_create(&env);

				if (rc != 0)
				{
					mdb_env_close(env);
					throw exception("LmdbClient::TryGet(): failed to create an environment");
				}

				rc = mdb_env_open(env, path.c_str(), 0, 0664);

				if (rc != 0)
				{
					mdb_env_close(env);
					throw exception("LmdbClient::TryGet(): failed to open an environment");
				}

				rc = mdb_txn_begin(env, NULL, 0, &txn);

				if (rc != 0)
				{
					mdb_txn_abort(txn);
					mdb_env_close(env);
					throw exception("LmdbClient::TryGet(): failed to begin a transaction");
				}

				rc = mdb_open(txn, NULL, 0, &dbi);

				if (rc != 0)
				{
					mdb_txn_abort(txn);
					mdb_close(env, dbi);
					mdb_env_close(env);
					throw exception("LmdbClient::TryGet(): failed to open a database");
				}

				key_p.mv_data = (void*)key.data();
				key_p.mv_size = strlen((const char*)key_p.mv_data);

				rc = mdb_get(txn, dbi, &key_p, &data_p);

				if (rc == 0)
				{
					value.assign((const char*)data_p.mv_data, data_p.mv_size);
				}

				mdb_txn_abort(txn);
				mdb_close(env, dbi);
				mdb_env_close(env);

				return (rc == 0);
			}

			static
				void
				Set(
					const string &path
					, const string &key
					, const string &value)
			{
				int rc = 0;
				MDB_env *env = NULL;
				MDB_dbi dbi = 0;
				MDB_txn *txn = NULL;
				MDB_val key_p, data_p;
				key_p.mv_data = NULL;
				key_p.mv_size = 0;
				data_p.mv_data = NULL;
				data_p.mv_size = 0;

				rc = mdb_env_create(&env);

				if (rc != 0)
				{
					mdb_env_close(env);
					throw exception("LmdbClient::Set(): failed to create an environment");
				}

				rc = mdb_env_open(env, path.c_str(), 0, 0664);

				if (rc != 0)
				{
					mdb_env_close(env);
					throw exception("LmdbClient::Set(): failed to open an environment");
				}

				rc = mdb_txn_begin(env, NULL, 0, &txn);

				if (rc != 0)
				{
					mdb_txn_abort(txn);
					mdb_env_close(env);
					throw exception("LmdbClient::Set(): failed to begin a transaction");
				}

				rc = mdb_open(txn, NULL, 0, &dbi);

				if (rc != 0)
				{
					mdb_txn_abort(txn);
					mdb_close(env, dbi);
					mdb_env_close(env);
					throw exception("LmdbClient::Set(): failed to open a database");
				}

				key_p.mv_data = (void*)key.data();
				key_p.mv_size = strlen((const char*)key_p.mv_data);
				data_p.mv_data = (void*)value.data();
				data_p.mv_size = strlen((const char*)data_p.mv_data);

				rc = mdb_put(txn, dbi, &key_p, &data_p, 0);

				if (rc != 0)
				{
					mdb_txn_abort(txn);
					mdb_close(env, dbi);
					mdb_env_close(env);
					throw exception("LmdbClient::Set(): failed to set the value of a key");
				}

				rc = mdb_txn_commit(txn);

				if (rc != 0)
				{
					mdb_txn_abort(txn);
					mdb_close(env, dbi);
					mdb_env_close(env);
					throw exception("LmdbClient::Set(): failed to commit a transaction");
				}

				mdb_close(env, dbi);
				mdb_env_close(env);
			}

			static
				void
				Remove(
					const string &path
					, const string &key)
			{
				int rc = 0;
				MDB_env *env = NULL;
				MDB_dbi dbi = 0;
				MDB_txn *txn = NULL;
				MDB_val key_p;
				key_p.mv_data = NULL;
				key_p.mv_size = 0;

				rc = mdb_env_create(&env);

				if (rc != 0)
				{
					mdb_env_close(env);
					throw exception("LmdbClient::Remove(): failed to create an environment");
				}

				rc = mdb_env_open(env, path.c_str(), 0, 0664);

				if (rc != 0)
				{
					mdb_env_close(env);
					throw exception("LmdbClient::Remove(): failed to open an environment");
				}

				rc = mdb_txn_begin(env, NULL, 0, &txn);

				if (rc != 0)
				{
					mdb_txn_abort(txn);
					mdb_env_close(env);
					throw exception("LmdbClient::Remove(): failed to begin a transaction");
				}

				rc = mdb_open(txn, NULL, 0, &dbi);

				if (rc != 0)
				{
					mdb_txn_abort(txn);
					mdb_close(env, dbi);
					mdb_env_close(env);
					throw exception("LmdbClient::Remove(): failed to open a database");
				}

				key_p.mv_data = (void*)key.data();
				key_p.mv_size = strlen((const char*)key_p.mv_data);

				rc = mdb_del(txn, dbi, &key_p, NULL);

				if (rc != 0)
				{
					mdb_txn_abort(txn);
					mdb_close(env, dbi);
					mdb_env_close(env);
					throw exception("LmdbClient::Remove(): failed to remove a key");
				}

				rc = mdb_txn_commit(txn);

				if (rc != 0)
				{
					mdb_txn_abort(txn);
					mdb_close(env, dbi);
					mdb_env_close(env);
					throw exception("LmdbClient::Remove(): failed to commit a transaction");
				}

				mdb_close(env, dbi);
				mdb_env_close(env);
			}

			static
				void
				Query(
					const string &path
					, function<void(const string &key, const string &value)> OnKyeValue)
			{
				int rc = 0;
				MDB_env *env = NULL;
				MDB_dbi dbi = 0;
				MDB_txn *txn = NULL;
				MDB_val key_p, data_p;
				key_p.mv_data = NULL;
				key_p.mv_size = 0;
				data_p.mv_data = NULL;
				data_p.mv_size = 0;
				MDB_cursor *cursor = NULL;

				rc = mdb_env_create(&env);

				if (rc != 0)
				{
					mdb_env_close(env);
					throw exception("LmdbClient::Query(): failed to create an environment");
				}

				rc = mdb_env_open(env, path.c_str(), 0, 0664);

				if (rc != 0)
				{
					mdb_env_close(env);
					throw exception("LmdbClient::Query(): failed to open an environment");
				}

				rc = mdb_txn_begin(env, NULL, 0, &txn);

				if (rc != 0)
				{
					mdb_txn_abort(txn);
					mdb_env_close(env);
					throw exception("LmdbClient::Query(): failed to begin a transaction");
				}

				rc = mdb_open(txn, NULL, 0, &dbi);

				if (rc != 0)
				{
					mdb_txn_abort(txn);
					mdb_close(env, dbi);
					mdb_env_close(env);
					throw exception("LmdbClient::Query(): failed to open a database");
				}

				rc = mdb_cursor_open(txn, dbi, &cursor);

				if (rc != 0)
				{
					mdb_cursor_close(cursor);
					mdb_txn_abort(txn);
					mdb_close(env, dbi);
					mdb_env_close(env);
					throw exception("LmdbClient::Query(): failed to open a cursor");
				}

				while (0 == (rc = mdb_cursor_get(cursor, &key_p, &data_p, MDB_NEXT)))
				{
					OnKyeValue(string((const char*)key_p.mv_data, key_p.mv_size), string((const char*)data_p.mv_data, data_p.mv_size));
				}

				mdb_cursor_close(cursor);
				mdb_txn_abort(txn);
				mdb_close(env, dbi);
				mdb_env_close(env);
			}
		};
	}
}