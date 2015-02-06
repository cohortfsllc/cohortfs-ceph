#include "os/FileNamer.h"
#include "gtest/gtest.h"

TEST(OsFileNamer, HashPrefixNamer)
{
  cohort::HashPrefixFileNamer namer(200);

  object_t oid("abcdefg");
  const uint64_t hash = 0x0123456789ABCDEFll;

  const std::string name = namer.get_name(oid, hash);
  ASSERT_EQ(name, "0123456789ABCDEFabcdefg");
}

TEST(OsFileNamer, HashPrefixNamer_Truncate)
{
  cohort::HashPrefixFileNamer namer(20);

  object_t oid("abcdefg"); // gets truncated to "abcd"
  const uint64_t hash = 0x0123456789ABCDEFll;

  const std::string name = namer.get_name(oid, hash);
  ASSERT_EQ(name, "0123456789ABCDEFabcd");
}

TEST(OsFileNamer, DedupNamer)
{
  cohort::DedupFileNamer namer;

  object_t oid("0123456789ABCDEF");
  const uint64_t hash = 0xFEDCBA9876543210ll;

  const std::string name = namer.get_name(oid, hash);
  // filename matches oid and ignores hash
  ASSERT_EQ(name, oid.name);
}
