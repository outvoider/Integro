#pragma once

#include <vector>
#include <atomic>

namespace Integro
{
	using namespace std;

	template <typename T>
	class SynchronizedBuffer
	{
		vector<T> items;
		atomic_flag lock;

	public:
		SynchronizedBuffer()
		{
			lock.clear();
		}

		bool IsEmpty()
		{
			while (lock.test_and_set());
				bool result = items.empty();
			lock.clear();

			return result;
		}

		void AddOne(const T &item)
		{
			while (lock.test_and_set());
			items.push_back(item);
			lock.clear();
		}

		void AddOne(const T &&item)
		{
			while (lock.test_and_set());
			items.emplace_back(move(item));
			lock.clear();
		}

		vector<T> GetAll()
		{
			while (lock.test_and_set());
				vector<T> result(move(items));
				items = vector<T>();
			lock.clear();

			return result;
		}
	};
}