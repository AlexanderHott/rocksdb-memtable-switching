#pragma once
#include <rocksdb/db.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/options.h>
#include <rocksdb/slice_transform.h>
#include <rocksdb/statistics.h>
#include <rocksdb/table.h>

#include <iostream>

#include "db_env.h"

using namespace rocksdb;

void configOptions(DBEnv *env, Options *options,
                   BlockBasedTableOptions *table_options,
                   WriteOptions *write_options, ReadOptions *read_options,
                   FlushOptions *flush_options) {
  options->statistics = CreateDBStatistics();

#pragma region[DBOptions]
  options->create_if_missing = env->create_if_missing;
  options->max_open_files = env->max_open_files;
  options->max_file_opening_threads = env->max_file_opening_threads;
  options->bytes_per_sync = env->bytes_per_sync;
  options->enable_thread_tracking = env->enable_thread_tracking;
  options->allow_concurrent_memtable_write =
      env->allow_concurrent_memtable_write;
  options->stats_history_buffer_size = env->stats_history_buffer_size;
  options->dump_malloc_stats = env->dump_malloc_stats;
  options->avoid_flush_during_shutdown = env->avoid_flush_during_shutdown;
  options->advise_random_on_open = env->advise_random_on_open;
  options->delete_obsolete_files_period_micros =
      env->delete_obsolete_files_period_micros;
  options->allow_mmap_reads = env->allow_mmap_reads;
  options->allow_mmap_writes = env->allow_mmap_writes;

 // switch (env->verbosity) {
 //   case 0:
 //     options->verbosity = Verbosity::NONE;
 //     break;
 //   case 1:
 //     options->verbosity = Verbosity::LOW;
 //     break;
 //   case 2:
 //     options->verbosity = Verbosity::MEDIUM;
 //     break;
 //   case 3:
 //     options->verbosity = Verbosity::HIGH;
 //     break;
 //   case 4:
 //     options->verbosity = Verbosity::EXTREME;
 //     break;
 //   default:
 //     std::cerr << "Error[" << __FILE__ << " : " << __LINE__
 //               << "]: Invalid verbosity level!" << std::endl;
  // }
#pragma endregion

  options->max_bytes_for_level_multiplier = env->size_ratio;
  options->write_buffer_size = env->GetBufferSize();
  options->target_file_size_base = env->GetTargetFileSizeBase();
  options->max_bytes_for_level_base = env->GetMaxBytesForLevelBase();
  options->max_write_buffer_number = env->max_write_buffer_number;

  if (env->bits_per_key == 0) {
    ;  // do nothing
  } else {
    // currently build full filter instead of block-based filter
    table_options->filter_policy.reset(
        NewBloomFilterPolicy(env->bits_per_key, false));
  }

  switch (env->compaction_pri) {
    case 1:
      options->compaction_pri = CompactionPri::kMinOverlappingRatio;
      break;
    case 2:
      options->compaction_pri = CompactionPri::kByCompensatedSize;
      break;
    case 3:
      options->compaction_pri = CompactionPri::kOldestLargestSeqFirst;
      break;
    case 4:
      options->compaction_pri = CompactionPri::kOldestSmallestSeqFirst;
      break;
    case 5:
      options->compaction_pri = CompactionPri::kRoundRobin;
      break;
    default:
      std::cerr << "Error[" << __FILE__ << " : " << __LINE__
                << "]: Invalid data movement policy!" << std::endl;
  }

  switch (env->memtable_factory) {
    case 1:
      options->memtable_factory.reset(new SkipListFactory);
      break;
    case 2:
      options->memtable_factory.reset(
          new VectorRepFactory(env->vector_preallocation_size_in_bytes));
      break;
    case 3:
      options->memtable_factory.reset(
          NewHashSkipListRepFactory(env->bucket_count, env->skiplist_height,
                                    env->skiplist_branching_factor));
      options->prefix_extractor.reset(
          NewFixedPrefixTransform(env->prefix_length));
      break;
    case 4:
      options->memtable_factory.reset(NewHashLinkListRepFactory(
          env->bucket_count, env->linklist_huge_page_tlb_size,
          env->linklist_bucket_entries_logging_threshold,
          env->linklist_if_log_bucket_dist_when_flash,
          env->linklist_threshold_use_skiplist));
      options->prefix_extractor.reset(
          NewFixedPrefixTransform(env->prefix_length));
      break;
//    case 5:
//      options->memtable_factory.reset(new UnsortedVectorRepFactory(
//          env->vector_preallocation_size_in_bytes));
//      break;
    default:
      std::cerr << "Error[" << __FILE__ << " : " << __LINE__
                << "]: Invalid memtable factory!" << std::endl;
  }

  options->level_compaction_dynamic_level_bytes =
      env->level_compaction_dynamic_level_bytes;

  switch (env->compaction_style) {
    case 1:
      options->compaction_style = CompactionStyle::kCompactionStyleLevel;
      break;
    case 2:
      options->compaction_style = CompactionStyle::kCompactionStyleUniversal;
      break;
    case 3:
      options->compaction_style = CompactionStyle::kCompactionStyleFIFO;
      break;
    case 4:
      options->compaction_style = CompactionStyle::kCompactionStyleNone;
      break;
    default:
      std::cerr << "Error[" << __FILE__ << " : " << __LINE__
                << "]: Invalid compaction eagerness!" << std::endl;
  }

  options->disable_auto_compactions = env->disable_auto_compactions;
  if (options->compaction_style != CompactionStyle::kCompactionStyleUniversal) {
    options->level0_file_num_compaction_trigger =
        env->level0_file_num_compaction_trigger;
    options->num_levels = env->num_levels;
  }

  options->target_file_size_multiplier = env->target_file_size_multiplier;
  options->max_background_jobs = env->max_background_jobs;
  options->soft_pending_compaction_bytes_limit =
      env->soft_pending_compaction_bytes_limit;
  options->hard_pending_compaction_bytes_limit =
      env->hard_pending_compaction_bytes_limit;
  options->periodic_compaction_seconds = env->periodic_compaction_seconds;
  options->use_direct_io_for_flush_and_compaction =
      env->use_direct_io_for_flush_and_compaction;
  options->use_direct_reads = env->use_direct_reads;

#pragma region[TableOptions]
  if (env->block_cache == 0) {
    table_options->no_block_cache = true;
    table_options->cache_index_and_filter_blocks = false;
  } else {
    table_options->no_block_cache = false;
    auto _1MB = 1024 * 1024;
    auto BLOCK_CACHE_TIMES_MB = env->block_cache * _1MB;
    std::shared_ptr<Cache> cache = NewLRUCache(
        BLOCK_CACHE_TIMES_MB, -1, false, env->block_cache_high_priority_ratio);
    table_options->block_cache = cache;
    table_options->cache_index_and_filter_blocks =
        env->cache_index_and_filter_blocks;
  }
  env->no_block_cache = table_options->no_block_cache;

  table_options->read_amp_bytes_per_bit = env->read_amp_bytes_per_bit;

  switch (env->data_block_index_type) {
    case 1:
      table_options->data_block_index_type =
          BlockBasedTableOptions::kDataBlockBinarySearch;
      break;
    case 2:
      table_options->data_block_index_type =
          BlockBasedTableOptions::kDataBlockBinaryAndHash;
      break;
    default:
      std::cerr << "Error[" << __FILE__ << " : " << __LINE__
                << "]: Invalid index type for data block!" << std::endl;
  }

  switch (env->index_type) {
    case 1:
      table_options->index_type = BlockBasedTableOptions::kBinarySearch;
      break;
    case 2:
      table_options->index_type = BlockBasedTableOptions::kHashSearch;
      break;
    case 3:
      table_options->index_type = BlockBasedTableOptions::kTwoLevelIndexSearch;
      break;
    case 4:
      table_options->index_type =
          BlockBasedTableOptions::kBinarySearchWithFirstKey;
      break;
    default:
      std::cerr << "Error[" << __FILE__ << " : " << __LINE__
                << "]: Invalid index type!" << std::endl;
  }

  table_options->partition_filters = env->partition_filters;
  table_options->block_size = env->GetBlockSize();
  table_options->metadata_block_size = env->metadata_block_size;
  table_options->pin_top_level_index_and_filter =
      env->pin_top_level_index_and_filter;

  switch (env->index_shortening) {
    case 1:
      table_options->index_shortening =
          BlockBasedTableOptions::IndexShorteningMode::kNoShortening;
      break;
    case 2:
      table_options->index_shortening =
          BlockBasedTableOptions::IndexShorteningMode::kShortenSeparators;
      break;
    case 3:
      table_options->index_shortening = BlockBasedTableOptions::
          IndexShorteningMode::kShortenSeparatorsAndSuccessor;
      break;
    default:
      std::cerr << "Error[" << __FILE__ << " : " << __LINE__
                << "]: Invalid index shortening!" << std::endl;
  }
  table_options->block_size_deviation = env->block_size_deviation;
  table_options->enable_index_compression = env->enable_index_compression;

  options->table_factory.reset(NewBlockBasedTableFactory(*table_options));
#pragma endregion  // [TableOptions]

  switch (env->compression) {
    case 1:
      options->compression = CompressionType::kNoCompression;
      break;
    case 2:
      options->compression = CompressionType::kSnappyCompression;
      break;
    case 3:
      options->compression = CompressionType::kZlibCompression;
      break;
    case 4:
      options->compression = CompressionType::kBZip2Compression;
      break;
    case 5:
      options->compression = CompressionType::kLZ4Compression;
      break;
    case 6:
      options->compression = CompressionType::kLZ4HCCompression;
      break;
    case 7:
      options->compression = CompressionType::kXpressCompression;
      break;
    case 8:
      options->compression = CompressionType::kZSTD;
      break;
    case 9:
      options->compression = CompressionType::kZSTDNotFinalCompression;
      break;
    case 10:
      options->compression = CompressionType::kDisableCompressionOption;
      break;

    default:
      std::cerr << "Error[" << __FILE__ << " : " << __LINE__
                << "]: Invalid compression type!" << std::endl;
  }

#pragma region[ReadOptions]
  read_options->verify_checksums = env->verify_checksums;
  read_options->fill_cache = env->fill_cache;
  read_options->ignore_range_deletions = env->ignore_range_deletions;
  switch (env->read_tier) {
    case 1:
      read_options->read_tier = ReadTier::kReadAllTier;
      break;
    case 2:
      read_options->read_tier = ReadTier::kBlockCacheTier;
      break;
    case 3:
      read_options->read_tier = ReadTier::kPersistedTier;
      break;
    case 4:
      read_options->read_tier = ReadTier::kMemtableTier;
      break;
    default:
      std::cerr << "Error[" << __FILE__ << " : " << __LINE__
                << "]: Invalid read tier!" << std::endl;
  }
#pragma endregion  // [ReadOptions]

#pragma region[WriteOptions]
  write_options->low_pri = env->low_pri;
  write_options->sync = env->sync;
  write_options->disableWAL = env->disableWAL;
  write_options->no_slowdown = env->no_slowdown;
  write_options->ignore_missing_column_families =
      env->ignore_missing_column_families;
#pragma endregion  // [WriteOptions]

#pragma region[ColumnFamilyOptions]
  switch (env->comparator) {
    case 1:
      options->comparator = BytewiseComparator();
      break;
    case 2:
      options->comparator = ReverseBytewiseComparator();
      break;
    default:
      std::cerr << "Error[" << __FILE__ << " : " << __LINE__
                << "]: Invalid comparator!" << std::endl;
  }

  options->max_sequential_skip_in_iterations =
      env->max_sequential_skip_in_iterations;
  options->memtable_prefix_bloom_size_ratio =
      env->memtable_prefix_bloom_size_ratio;
  options->level0_slowdown_writes_trigger = env->level0_slowdown_writes_trigger;
  options->level0_stop_writes_trigger = env->level0_stop_writes_trigger;
  options->paranoid_file_checks = env->paranoid_file_checks;
  options->optimize_filters_for_hits = env->optimize_filters_for_hits;
  options->inplace_update_support = env->inplace_update_support;
  options->inplace_update_num_locks = env->inplace_update_num_locks;
  options->report_bg_io_stats = env->report_bg_io_stats;
#pragma endregion  // [ColumnFamilyOptions]

#pragma region[FlushOptions]
  flush_options->wait = env->wait;
  flush_options->allow_write_stall = env->allow_write_stall;
#pragma endregion  // [FlushOptions]

#pragma region[LSMMemoryBuffer]
  // nothing to configure
  // everything is handled by parse_arguments
#pragma endregion  // [LSMMemoryBuffer]
}