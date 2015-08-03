// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "gtest/gtest.h"

#include "os/PageSet.h"

TEST(PageSet, AllocAligned)
{
  typedef PageSet<1> page_set;
  page_set pages;
  page_set::page_vector range(4);

  pages.alloc_range(0, 4, range.begin(), range.end());
  ASSERT_EQ(0u, range[0]->offset);
  ASSERT_EQ(1u, range[1]->offset);
  ASSERT_EQ(2u, range[2]->offset);
  ASSERT_EQ(3u, range[3]->offset);
}

TEST(PageSet, AllocUnaligned)
{
  typedef PageSet<2> page_set;
  page_set pages;
  page_set::page_vector range(3);

  // front of first page
  pages.alloc_range(0, 1, range.begin(), range.begin() + 1);
  ASSERT_EQ(0u, range[0]->offset);
  range[0].reset();

  // back of first page
  pages.alloc_range(1, 1, range.begin(), range.begin() + 1);
  ASSERT_EQ(0u, range[0]->offset);
  range[0].reset();

  // back of first page and front of second
  pages.alloc_range(1, 2, range.begin(), range.begin() + 2);
  ASSERT_EQ(0u, range[0]->offset);
  ASSERT_EQ(2u, range[1]->offset);
  range[0].reset();
  range[1].reset();

  // back of first page and all of second
  pages.alloc_range(1, 3, range.begin(), range.begin() + 2);
  ASSERT_EQ(0u, range[0]->offset);
  ASSERT_EQ(2u, range[1]->offset);
  range[0].reset();
  range[1].reset();

  // back of first page, all of second, and front of third
  pages.alloc_range(1, 4, range.begin(), range.begin() + 3);
  ASSERT_EQ(0u, range[0]->offset);
  ASSERT_EQ(2u, range[1]->offset);
  ASSERT_EQ(4u, range[2]->offset);
}

TEST(PageSet, GetAligned)
{
  // allocate 4 pages
  typedef PageSet<1> page_set;
  page_set pages;
  page_set::page_vector range(4);
  pages.alloc_range(0, 4, range.begin(), range.end());
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

TEST(PageSet, GetUnaligned)
{
  // allocate 3 pages
  typedef PageSet<2> page_set;
  page_set pages;
  page_set::page_vector range(3);
  pages.alloc_range(0, 6, range.begin(), range.end());
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

TEST(PageSet, GetHoles)
{
  // allocate pages at offsets 1, 2, 5, and 7
  typedef PageSet<1> page_set;
  page_set pages;
  page_set::page_vector range(1);
  for (uint64_t i : {1, 2, 5, 7})
    pages.alloc_range(i, 1, range.begin(), range.end());
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

TEST(PageSet, FreeAligned)
{
  // allocate 4 pages
  typedef PageSet<1> page_set;
  page_set pages;
  page_set::page_vector range(4);
  pages.alloc_range(0, 4, range.begin(), range.end());
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

TEST(PageSet, FreeUnaligned)
{
  // allocate 4 pages
  typedef PageSet<2> page_set;
  page_set pages;
  page_set::page_vector range(4);
  pages.alloc_range(0, 8, range.begin(), range.end());
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

TEST(PageSet, FreeHoles)
{
  // allocate pages at offsets 1, 2, 5, and 7
  typedef PageSet<1> page_set;
  page_set pages;
  page_set::page_vector range(1);
  for (uint64_t i : {1, 2, 5, 7})
    pages.alloc_range(i, 1, range.begin(), range.end());
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
