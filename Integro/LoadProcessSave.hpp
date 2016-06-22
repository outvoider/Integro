#pragma once

#include <atomic>

#include "SynchronizedBuffer.hpp"

namespace Integro
{
	class LoadProcessSave
	{
	public:
		template <typename MetadataStore
			, typename SourceStore
			, typename DestinationStore
			, typename DataService
			, typename MetadataProvider>
			static void FromStoreToStoreBatch(
			MetadataStore metadataStore
			, SourceStore sourceStore
			, DestinationStore destinationStore
			, DataService dataService
			, MetadataProvider metadataProvider)
		{
			auto startTime = metadataStore.LoadStartTime();
			vector<DataService::Data> data;

			sourceStore.LoadData(startTime, [&](const DataService::Data &datum)
			{
				auto time = metadataProvider.GetTime(datum);

				if (startTime < time)
				{
					startTime = time;
				}

				data.push_back(datum);
				dataService.ProcessData(data.back());
			});

			destinationStore.SaveData(data);
			metadataStore.SaveStartTime(startTime);
		}

		template <typename MetadataStore
			, typename SourceStore
			, typename DestinationStore
			, typename DataService
			, typename MetadataProvider>
			static void FromStoreToStore(
			MetadataStore metadataStore
			, SourceStore sourceStore
			, DestinationStore destinationStore
			, DataService dataService
			, MetadataProvider metadataProvider)
		{
			auto startTime = metadataStore.LoadStartTime();
			SynchronizedBuffer<DataService::Data> buffer;
			auto hasFinishedLoadingData = false;
			auto hasFinishedProcessingSavingData = false;
			auto hasFailed = false;
			atomic_flag hasHadFailure = ATOMIC_FLAG_INIT;
			exception failure;

			auto TryAbort = [&](bool isFinal)
			{
				if (hasFailed)
				{
					throw isFinal ? failure : exception("abortion requested");
				}
			};

			auto OnError = [&](function<void()> action, bool &hasFinished)
			{
				try
				{
					action();
				}
				catch (const exception &ex)
				{
					if (!hasHadFailure.test_and_set())
					{
						hasFailed = true;
						failure = exception(ex.what());
					}
				}
				catch (...)
				{
					if (!hasHadFailure.test_and_set())
					{
						hasFailed = true;
						failure = exception("unspecified error");
					}
				}

				atomic_thread_fence(memory_order_seq_cst);
				hasFinished = true;
			};

			auto LoadData = [&]()
			{
				sourceStore.LoadData(startTime, [&](const DataService::Data &datum)
				{
					TryAbort(false);
					buffer.AddOne(datum);
				});
			};

			auto ProcessSaveData = [&]()
			{
				while (!hasFinishedLoadingData || !buffer.IsEmpty())
				{
					if (buffer.IsEmpty())
					{
						this_thread::sleep_for(chrono::milliseconds(1));
					}
					else
					{
						auto data = buffer.GetAll();

						for (auto &datum : data)
						{
							auto time = metadataProvider.GetTime(datum);
							assert(time >= startTime);
							startTime = time;

							dataService.ProcessData(datum);
						}

						destinationStore.SaveData(data);
						metadataStore.SaveStartTime(startTime);
					}
				}
			};

			thread ProcessSaveDataThread(OnError, ProcessSaveData, hasFinishedProcessingSavingData);
			OnError(LoadData, hasFinishedLoadingData);

			ProcessSaveDataThread.join();

			TryAbort(true);
		}

		template <typename MetadataStore
			, typename SourceStream
			, typename SourceStore
			, typename DestinationStore
			, typename DataService
			, typename MetadataProvider>
			static void FromStreamOrStoreToStore(
			MetadataStore metadataStore
			, SourceStream sourceStream
			, SourceStore sourceStore
			, DestinationStore destinationStore
			, DataService dataService
			, MetadataProvider metadataProvider)
		{
			auto streamStartId = metadataStore.LoadStartId();
			auto streamStartTime = metadataStore.LoadStartTime();
			auto storeStartId = streamStartId;
			auto storeStartTime = streamStartTime;
			SynchronizedBuffer<DataService::Data> streamBuffer;
			SynchronizedBuffer<DataService::Data> storeBuffer;
			auto hasLoadingStoreDataBeenRequested = false;
			auto hasLoadingStoreDataBeenDisabled = false;
			auto hasFailed = false;
			atomic_flag hasHadFailure = ATOMIC_FLAG_INIT;
			exception failure;

			auto TryThrow = [&](bool doThrow, bool isFinal)
			{
				if (doThrow)
				{
					throw isFinal ? failure : exception("abortion requested");
				}
			};

			enum ActionName { LoadStreamData, LoadStoreData, ProcessSaveStreamData, ProcessSaveStoreData };
			bool hasActionFinished[] = { false, false, false, false };
			function<void()> actions[] =
			{
				[&]() // LoadStreamData
				{
					sourceStream.LoadData(streamStartId, [&](const DataService::Data &datum)
					{
						TryThrow(hasFailed, false);
						streamBuffer.AddOne(datum);
					});
				},
					[&]() // LoadStoreData
				{
					while (!hasLoadingStoreDataBeenDisabled
						&& !hasActionFinished[ProcessSaveStreamData])
					{
						if (!hasLoadingStoreDataBeenRequested)
						{
							this_thread::sleep_for(chrono::milliseconds(1));
						}
						else
						{
							hasLoadingStoreDataBeenDisabled = true;

							sourceStore.LoadData(storeStartTime, [&](const DataService::Data &datum)
							{
								TryThrow(hasActionFinished[ProcessSaveStoreData], false);
								storeBuffer.AddOne(datum);
							});
						}
					}
				},
					[&]() // ProcessSaveStreamData
				{
					auto hasSavedMetadata = false;

					while (!hasActionFinished[LoadStreamData] || !streamBuffer.IsEmpty())
					{
						if (streamBuffer.IsEmpty())
						{
							this_thread::sleep_for(chrono::milliseconds(1));
						}
						else
						{
							auto data = streamBuffer.GetAll();

							for (auto &datum : data)
							{
								auto id = metadataProvider.GetId(datum);

								if (!hasLoadingStoreDataBeenDisabled)
								{
									if (id == streamStartId)
									{
										hasLoadingStoreDataBeenDisabled = true;
									}
									else
									{
										hasLoadingStoreDataBeenRequested = true;

										while (!hasLoadingStoreDataBeenDisabled
											&& !hasActionFinished[LoadStoreData])
										{
											this_thread::sleep_for(chrono::milliseconds(1));
										}
									}
								}

								streamStartId = id;

								auto time = metadataProvider.GetTime(datum);
								assert(time >= streamStartTime);
								streamStartTime = time;

								dataService.ProcessData(datum);
							}

							destinationStore.SaveData(data);

							if (hasActionFinished[ProcessSaveStoreData] && !hasFailed)
							{
								metadataStore.SaveStartId(streamStartId);
								metadataStore.SaveStartTime(streamStartTime);
								hasSavedMetadata = true;
							}
						}
					}

					if (!hasLoadingStoreDataBeenDisabled)
					{
						hasSavedMetadata = true;
						hasLoadingStoreDataBeenRequested = true;
					}

					if (hasLoadingStoreDataBeenRequested)
					{
						while (!hasActionFinished[ProcessSaveStoreData])
						{
							this_thread::sleep_for(chrono::milliseconds(1));
						}
					}

					if (!hasSavedMetadata && !hasFailed)
					{
						metadataStore.SaveStartId(streamStartId);
						metadataStore.SaveStartTime(streamStartTime);
					}
				},
					[&]() // ProcessSaveStoreData
				{
					while (!hasActionFinished[LoadStoreData] || !storeBuffer.IsEmpty())
					{
						if (storeBuffer.IsEmpty())
						{
							this_thread::sleep_for(chrono::milliseconds(1));
						}
						else
						{
							auto data = storeBuffer.GetAll();

							for (auto &datum : data)
							{
								auto id = metadataProvider.GetId(datum);
								storeStartId = id;

								auto time = metadataProvider.GetTime(datum);
								assert(time >= storeStartTime);
								storeStartTime = time;

								dataService.ProcessData(datum);
							}

							destinationStore.SaveData(data);
							metadataStore.SaveStartId(storeStartId);
							metadataStore.SaveStartTime(storeStartTime);
						}
					}
				}
			};

			auto OnError = [&](ActionName actionName)
			{
				try
				{
					actions[actionName]();
				}
				catch (const exception &ex)
				{
					if (!hasHadFailure.test_and_set())
					{
						hasFailed = true;
						failure = exception(ex.what());
					}
				}
				catch (...)
				{
					if (!hasHadFailure.test_and_set())
					{
						hasFailed = true;
						failure = exception("unspecified error");
					}
				}

				atomic_thread_fence(memory_order_seq_cst);
				hasActionFinished[actionName] = true;
			};

			thread ProcessSaveStoreDataThread(OnError, ProcessSaveStoreData);
			thread ProcessSaveStreamDataThread(OnError, ProcessSaveStreamData);
			thread LoadStoreDataThread(OnError, LoadStoreData);
			OnError(LoadStreamData);

			ProcessSaveStoreDataThread.join();
			ProcessSaveStreamDataThread.join();
			LoadStoreDataThread.join();

			TryThrow(hasFailed, true);
		}
	};
}