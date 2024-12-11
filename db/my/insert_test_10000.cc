#include <gtest/gtest.h>
#include <chrono>
#include <iostream>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/env.h"
#include "rocksdb/slice.h"
//#include "rocksdb/compaction_filter.h"

#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {

class InsertTest : public testing::Test {
 public:
  InsertTest() {
    // 设置数据库路径
    db_name_ = test::PerThreadDBPath("insert_test");
  }

  ~InsertTest() {
    // 清理代码，例如关闭数据库
  }

  std::string db_name_;
  DB* db_ = nullptr;
  
  std::string convertToPath(std::string path) {
    // 替换反斜杠为正斜杠
    std::replace(path.begin(), path.end(), '\\', '/');
    return path;
  }

  void SetUp() override {
    // 测试前的准备工作，例如打开数据库
    Options options;
    options.create_if_missing = true;
    ASSERT_OK(DB::Open(options, db_name_, &db_));
  }

  void TearDown() override {
    // 测试后的清理工作，例如关闭并删除数据库
    delete db_;
    db_ = nullptr;
    ASSERT_OK(DestroyDB(db_name_, Options()));
  }
};

// 测试用例：Insert10000 插入1w数据花费时间 check 1w数据花费时间 flush 1w数据花费时间 check 1w数据花费时间
TEST_F(InsertTest, Insert10000) {
  std::cout << "begin Insert 10000 test" << std::endl;
  std::cout << convertToPath(db_name_) << std::endl;

  std::cout << "Insert 10000 begin" << std::endl;
  // 开始计时
  auto insert_start = std::chrono::high_resolution_clock::now();
  // 插入一万条数据
  for (int i = 0; i < 10000; ++i) {
    std::string key = "key" + std::to_string(i);
    std::string value = "value" + std::to_string(i);
    ASSERT_OK(db_->Put(WriteOptions(), Slice(key), Slice(value)));
  }
  // 结束计时
  auto insert_end = std::chrono::high_resolution_clock::now();
  std::cout << "Insert 10000 end" << std::endl;
  std::chrono::duration<double, std::milli> insert_time = insert_end - insert_start;
  // 输出插入操作所花费的时间
  std::cout << "Insert 10000 records took " << insert_time.count() << " milliseconds" << std::endl << std::endl;

  std::cout << "check 10000 begin" << std::endl;
  auto check_before_flush_start = std::chrono::high_resolution_clock::now();
  // 验证数据是否正确（可选）
  for (int i = 0; i < 10000; ++i) {
    std::string key = "key" + std::to_string(i);
    std::string value;
    ASSERT_OK(db_->Get(ReadOptions(), Slice(key), &value));
    ASSERT_EQ(value, "value" + std::to_string(i));
  }
  auto check_before_flush_end = std::chrono::high_resolution_clock::now();
  std::cout << "check 10000 end" << std::endl;
  std::chrono::duration<double, std::milli> check_before_flush_time = check_before_flush_end - check_before_flush_start;
  std::cout << "check 10000 records took " << check_before_flush_time.count() << " milliseconds" << std::endl << std::endl;

  std::cout << "flush 10000 begin" << std::endl;
  auto flush_start = std::chrono::high_resolution_clock::now();
  ASSERT_OK(db_->Flush(FlushOptions()));
  auto flush_end = std::chrono::high_resolution_clock::now();
  std::cout << "flush 10000 end" << std::endl;
  std::chrono::duration<double, std::milli> flush_time = flush_end - flush_start;
  std::cout << "flush 10000 records took " << flush_time.count() << " milliseconds" << std::endl << std::endl;

  std::cout << "check 10000 begin" << std::endl;
  auto check_after_flush_start = std::chrono::high_resolution_clock::now();
  // 验证数据是否正确（可选）
  for (int i = 0; i < 10000; ++i) {
    std::string key = "key" + std::to_string(i);
    std::string value;
    ASSERT_OK(db_->Get(ReadOptions(), Slice(key), &value));
    ASSERT_EQ(value, "value" + std::to_string(i));
  }
  auto check_after_flush_end = std::chrono::high_resolution_clock::now();
  std::cout << "check 10000 end" << std::endl;
  std::chrono::duration<double, std::milli> check_after_flush_time = check_after_flush_end - check_after_flush_start;
  std::cout << "check 10000 records took " << check_after_flush_time.count() << " milliseconds" << std::endl << std::endl;

  std::cout << "end insert 10000 test" << std::endl;
}

TEST_F(InsertTest, FlushAndCompaction) {
  std::cout << "begin flush and compaction test" << std::endl;
  std::cout << convertToPath(db_name_) << std::endl;

  // 插入数据
  std::cout << "FlushAndCompaction Insert 10000: insert begin" << std::endl;
  for (int i = 1; i <= 10000; ++i) {
    std::string key = "key" + std::to_string(i);
    std::string value = "value" + std::to_string(i);
    ASSERT_OK(db_->Put(WriteOptions(), Slice(key), Slice(value)));
    // per 1000 record run one flush
    if(i % 1000 == 0){
      // 显式执行Flush操作
      std::cout << "FlushAndCompaction Insert 10000: flush begin" << std::endl;
      ASSERT_OK(db_->Flush(FlushOptions()));
      std::cout << "FlushAndCompaction Insert 10000: flush end" << std::endl;
    }
    // per 1000 record run one compaction
    if(i % 4000 == 0){
      // 执行Compaction操作
      std::cout << "FlushAndCompaction Insert 10000: compaction begin" << std::endl;
      CompactRangeOptions compact_options;
      compact_options.exclusive_manual_compaction = false;
      ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
      std::cout << "FlushAndCompaction Insert 10000: compaction end" << std::endl;
    }
  }
  std::cout << "FlushAndCompaction Insert 10000: insert end" << std::endl;

  std::cout << "end flush and compaction test" << std::endl;
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  std::cout << "insert_test_10000 main run" << std::endl;
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}