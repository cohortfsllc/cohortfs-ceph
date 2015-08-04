// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "gtest/gtest.h"

#include "os/PartitionedPageSet.h"

// test PartitionedPageSet with various parameters for partition count and
// pages_per_stripe
typedef std::pair<size_t, size_t> Param;
class TestParams : public ::testing::TestWithParam<Param> {
 public:
  size_t param1() const { return GetParam().first; }
  size_t param2() const { return GetParam().second; }
};
INSTANTIATE_TEST_CASE_P(PartitionedPageSet, TestParams, ::testing::Values(
        Param{1, 1}, Param{1, 2}, Param{1, 4},
        Param{2, 1}, Param{2, 2}, Param{2, 4},
        Param{3, 1}, Param{3, 2}, Param{3, 4},
        Param{4, 1}, Param{4, 2}, Param{4, 4}));

TEST_P(TestParams, AllocAligned)
{
  typedef PartitionedPageSet<1> page_set;
  page_set pages(param1(), param2());
  page_set::page_vector range;

  pages.alloc_range(0, 4, range);
  ASSERT_EQ(0u, range[0]->offset);
  ASSERT_EQ(1u, range[1]->offset);
  ASSERT_EQ(2u, range[2]->offset);
  ASSERT_EQ(3u, range[3]->offset);
}

TEST_P(TestParams, AllocUnaligned)
{
  typedef PartitionedPageSet<2> page_set;
  page_set pages(param1(), param2());
  page_set::page_vector range;

  // front of first page
  pages.alloc_range(0, 1, range);
  ASSERT_EQ(0u, range[0]->offset);
  range.clear();

  // back of first page
  pages.alloc_range(1, 1, range);
  ASSERT_EQ(0u, range[0]->offset);
  range.clear();

  // back of first page and front of second
  pages.alloc_range(1, 2, range);
  ASSERT_EQ(0u, range[0]->offset);
  ASSERT_EQ(2u, range[1]->offset);
  range.clear();

  // back of first page and all of second
  pages.alloc_range(1, 3, range);
  ASSERT_EQ(0u, range[0]->offset);
  ASSERT_EQ(2u, range[1]->offset);
  range.clear();

  // back of first page, all of second, and front of third
  pages.alloc_range(1, 4, range);
  ASSERT_EQ(0u, range[0]->offset);
  ASSERT_EQ(2u, range[1]->offset);
  ASSERT_EQ(4u, range[2]->offset);
  range.clear();
}

TEST_P(TestParams, GetAligned)
{
  // allocate 4 pages
  typedef PartitionedPageSet<1> page_set;
  page_set pages(param1(), param2());
  page_set::page_vector range;
  pages.alloc_range(0, 4, range);
  range.clear();

  // get first page
  pages.get_range(0, 1, range);
  ASSERT_EQ(1u, range.size());
  ASSERT_EQ(0u, range[0]->offset);
  range.clear();

  // get second and third pages
  pages.get_range(1, 2, range);
  ASSERT_EQ(2u, range.size());
  ASSERT_EQ(1u, range[0]->offset);
  ASSERT_EQ(2u, range[1]->offset);
  range.clear();

  // get all four pages
  pages.get_range(0, 4, range);
  ASSERT_EQ(4u, range.size());
  ASSERT_EQ(0u, range[0]->offset);
  ASSERT_EQ(1u, range[1]->offset);
  ASSERT_EQ(2u, range[2]->offset);
  ASSERT_EQ(3u, range[3]->offset);
  range.clear();
}

TEST_P(TestParams, GetUnaligned)
{
  // allocate 3 pages
  typedef PartitionedPageSet<2> page_set;
  page_set pages(param1(), param2());
  page_set::page_vector range;
  pages.alloc_range(0, 6, range);
  range.clear();

  // front of first page
  pages.get_range(0, 1, range);
  ASSERT_EQ(1u, range.size());
  ASSERT_EQ(0u, range[0]->offset);
  range.clear();

  // back of first page
  pages.get_range(1, 1, range);
  ASSERT_EQ(1u, range.size());
  ASSERT_EQ(0u, range[0]->offset);
  range.clear();

  // back of first page and front of second
  pages.get_range(1, 2, range);
  ASSERT_EQ(2u, range.size());
  ASSERT_EQ(0u, range[0]->offset);
  ASSERT_EQ(2u, range[1]->offset);
  range.clear();

  // back of first page and all of second
  pages.get_range(1, 3, range);
  ASSERT_EQ(2u, range.size());
  ASSERT_EQ(0u, range[0]->offset);
  ASSERT_EQ(2u, range[1]->offset);
  range.clear();

  // back of first page, all of second, and front of third
  pages.get_range(1, 4, range);
  ASSERT_EQ(3u, range.size());
  ASSERT_EQ(0u, range[0]->offset);
  ASSERT_EQ(2u, range[1]->offset);
  ASSERT_EQ(4u, range[2]->offset);
  range.clear();

  // back of third page with nothing beyond
  pages.get_range(5, 999, range);
  ASSERT_EQ(1u, range.size());
  ASSERT_EQ(4u, range[0]->offset);
  range.clear();
}

TEST_P(TestParams, GetHoles)
{
  // allocate pages at offsets 1, 2, 5, and 7
  typedef PartitionedPageSet<1> page_set;
  page_set pages(param1(), param2());
  page_set::page_vector range;
  for (uint64_t i : {1, 2, 5, 7})
    pages.alloc_range(i, 1, range);
  range.clear();

  // nothing at offset 0, page at offset 1
  pages.get_range(0, 2, range);
  ASSERT_EQ(1u, range.size());
  ASSERT_EQ(1u, range[0]->offset);
  range.clear();

  // nothing at offset 0, pages at offset 1 and 2, nothing at offset 3
  pages.get_range(0, 4, range);
  ASSERT_EQ(2u, range.size());
  ASSERT_EQ(1u, range[0]->offset);
  ASSERT_EQ(2u, range[1]->offset);
  range.clear();

  // page at offset 2, nothing at offset 3 or 4
  pages.get_range(2, 3, range);
  ASSERT_EQ(1u, range.size());
  ASSERT_EQ(2u, range[0]->offset);
  range.clear();

  // get the full range
  pages.get_range(0, 999, range);
  ASSERT_EQ(4u, range.size());
  ASSERT_EQ(1u, range[0]->offset);
  ASSERT_EQ(2u, range[1]->offset);
  ASSERT_EQ(5u, range[2]->offset);
  ASSERT_EQ(7u, range[3]->offset);
  range.clear();
}

TEST_P(TestParams, FreeAligned)
{
  // allocate 4 pages
  typedef PartitionedPageSet<1> page_set;
  page_set pages(param1(), param2());
  page_set::page_vector range;
  pages.alloc_range(0, 4, range);
  range.clear();

  // get the full range
  pages.get_range(0, 4, range);
  ASSERT_EQ(4u, range.size());
  range.clear();

  // free after offset 4 has no effect
  pages.free_pages_after(4);
  pages.get_range(0, 4, range);
  ASSERT_EQ(4u, range.size());
  range.clear();

  // free page 4
  pages.free_pages_after(3);
  pages.get_range(0, 4, range);
  ASSERT_EQ(3u, range.size());
  range.clear();

  // free pages 2 and 3
  pages.free_pages_after(1);
  pages.get_range(0, 4, range);
  ASSERT_EQ(1u, range.size());
  range.clear();
}

TEST_P(TestParams, FreeUnaligned)
{
  // allocate 4 pages
  typedef PartitionedPageSet<2> page_set;
  page_set pages(param1(), param2());
  page_set::page_vector range;
  pages.alloc_range(0, 8, range);
  range.clear();

  // get the full range
  pages.get_range(0, 8, range);
  ASSERT_EQ(4u, range.size());
  range.clear();

  // free after offset 7 has no effect
  pages.free_pages_after(7);
  pages.get_range(0, 8, range);
  ASSERT_EQ(4u, range.size());
  range.clear();

  // free page 4
  pages.free_pages_after(5);
  pages.get_range(0, 8, range);
  ASSERT_EQ(3u, range.size());
  range.clear();

  // free pages 2 and 3
  pages.free_pages_after(1);
  pages.get_range(0, 8, range);
  ASSERT_EQ(1u, range.size());
  range.clear();
}

TEST_P(TestParams, FreeHoles)
{
  // allocate pages at offsets 1, 2, 5, and 7
  typedef PartitionedPageSet<1> page_set;
  page_set pages(param1(), param2());
  page_set::page_vector range;
  for (uint64_t i : {1, 2, 5, 7})
    pages.alloc_range(i, 1, range);
  range.clear();

  // get the full range
  pages.get_range(0, 8, range);
  ASSERT_EQ(4u, range.size());
  range.clear();

  // free page 7
  pages.free_pages_after(6);
  pages.get_range(0, 8, range);
  ASSERT_EQ(3u, range.size());
  range.clear();

  // free page 5
  pages.free_pages_after(3);
  pages.get_range(0, 8, range);
  ASSERT_EQ(2u, range.size());
  range.clear();

  // free pages 1 and 2
  pages.free_pages_after(0);
  pages.get_range(0, 8, range);
  ASSERT_EQ(0u, range.size());
}
