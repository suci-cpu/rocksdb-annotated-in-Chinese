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

// 测试用例：InsertMany
TEST_F(InsertTest, InsertMany) {
  std::cout << "begin insert many test" << std::endl;

  // 开始计时
  auto start = std::chrono::high_resolution_clock::now();

  // 插入一万条数据
  for (int i = 0; i < 10000; ++i) {
    std::string key = "key" + std::to_string(i);
    std::string value = "value" + std::to_string(i);
    ASSERT_OK(db_->Put(WriteOptions(), Slice(key), Slice(value)));
//    ASSERT_OK(db_->Flush(FlushOptions()));
  }

  // 结束计时
  auto end = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double, std::milli> insert_time = end - start;

  // 输出插入操作所花费的时间
  std::cout << "Insert 10000 records took " << insert_time.count() << " milliseconds" << std::endl;

  // 验证数据是否正确（可选）
  for (int i = 0; i < 10000; ++i) {
    std::string key = "key" + std::to_string(i);
    std::string value;
    ASSERT_OK(db_->Get(ReadOptions(), Slice(key), &value));
    ASSERT_EQ(value, "value" + std::to_string(i));
  }

  std::cout << "end insert many test" << std::endl;
}

TEST_F(InsertTest, FlushAndCompaction) {
  std::cout << "begin flush and compaction test" << std::endl;

  // 插入数据
  for (int i = 0; i < 10000; ++i) {
    std::string key = "key" + std::to_string(i);
    std::string value = "value" + std::to_string(i);
    ASSERT_OK(db_->Put(WriteOptions(), Slice(key), Slice(value)));
  }

  // 显式执行Flush操作
  ASSERT_OK(db_->Flush(FlushOptions()));

  // 等待所有背景Compaction完成
//  db_->WaitForFlushMemTable();

  // 执行Compaction操作
  CompactRangeOptions compact_options;
  compact_options.exclusive_manual_compaction = false;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));

  std::cout << "end flush and compaction test" << std::endl;
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  std::cout << "insert_test_10000 main run" << std::endl;
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}