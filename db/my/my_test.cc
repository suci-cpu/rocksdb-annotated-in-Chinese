#include <gtest/gtest.h>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/env.h"
#include "rocksdb/slice.h"
#include "rocksdb/utilities/db_ttl.h"

#include "test_util/testharness.h"
#include "test_util/testutil.h"

namespace ROCKSDB_NAMESPACE {

class MyTest : public testing::Test {
 public:
  MyTest() {
    // 测试初始化代码，例如设置数据库路径
    db_name_ = test::PerThreadDBPath("my_test");
  }

  ~MyTest() {
    // 测试清理代码，例如关闭数据库
  }

  std::string db_name_;
  // 可以添加其他成员变量，例如数据库实例

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
    std::cout << "end my test" << std::endl;
  }

  DB* db_; // 用于存储数据库实例的指针
};

// 测试用例：TestMy
TEST_F(MyTest, TestMy) {
  std::cout << "begin my test" << std::endl;
  std::string value;
  Slice key_slice("key");
  Slice value_slice("value");

  // 写入测试数据
  ASSERT_OK(db_->Put(WriteOptions(), key_slice, value_slice));

  // 读取测试数据
  ASSERT_OK(db_->Get(ReadOptions(), key_slice, &value));
  ASSERT_EQ(value, value_slice);

  // 验证数据是否正确
  ASSERT_EQ(value, "value");
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}