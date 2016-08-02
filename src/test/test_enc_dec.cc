// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 * Copyright (C) 2016 Western Digital Corporation
 *
 * Author: Allen Samuels <allen.samuels@sandisk.com>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */

#include <stdio.h>

#include "global/global_init.h"
#include "common/ceph_argparse.h"
#include "global/global_context.h"
#include "gtest/gtest.h"
#include "os/bluestore/bluestore_types.h"
#include "os/bluestore/BlueStore.h"
#include "include/enc_dec.h"


static size_t break_point = 0;

void dumpbuf(string s,bufferlist& b) {
  cout << s;
  for (size_t i = 0; i < b.length(); ++i) cout << " " << hex << (int)(b[i]) << dec;
  cout << "\n";
}

template<typename t> void enc_dec_buffer(t v) {

   bufferlist b;
   enc(b,v);
   //
   // Now decode :)
   //
   t d;

   break_point = (break_point + 1) % 7;

   bufferlist b1,b2,b3;
   bufferlist::iterator it = b.begin();

   if (break_point == 0 || break_point >= b.length()) {
     it = b.begin();
   } else {
     it.copy(break_point,b1);
     it.copy(b.length()-break_point,b2);
     b3.claim_append(b1);
     b3.claim_append(b2);
     it = b3.begin();
   }

   dec(it,d);
   EXPECT_EQ(v,d);
}

char buf[1000];

template<typename t> bool enc_dec_scalar(t v) {

   for (size_t i = 0; i < sizeof(buf); ++i) buf[i] = (char) i; // poison the buffer :)

   size_t estimate = enc_dec(size_t(0), v);
   EXPECT_TRUE(estimate < sizeof(buf));
   
   char *end = enc_dec(buf,v);
   
   size_t actual = end - buf;
   EXPECT_TRUE(actual < sizeof(buf));

   for (size_t i = estimate; i < sizeof(buf); ++i) EXPECT_EQ((char)i,buf[i]); // verify not exceeded estimate

   //
   // Now decode :)
   //
   t dec;
   const char *done = enc_dec((const char *)buf,dec);
   size_t consumed = done - buf;
   EXPECT_EQ(consumed,actual);

   EXPECT_EQ(v,dec);

   return estimate == actual;
}


template<typename t> bool enc_dec_varint(t v) {

   for (size_t i = 0; i < sizeof(buf); ++i) buf[i] = (char) i; // poison the buffer :)

   size_t estimate = enc_dec_varint(size_t(0), v);
   EXPECT_TRUE(estimate < sizeof(buf));
   
   char *end = enc_dec_varint(buf,v);
   
   size_t actual = end - buf;
   EXPECT_TRUE(actual < sizeof(buf));

   for (size_t i = estimate; i < sizeof(buf); ++i) EXPECT_EQ((char)i,buf[i]); // verify not exceeded estimate
   //
   // Now decode :)
   //
   t d;
   const char *done = enc_dec_varint((const char *)buf,d);
   size_t consumed = done - buf;
   EXPECT_EQ(consumed,actual);

   EXPECT_EQ(v,d);

   return estimate == actual; // this is how we detect whether is_bounded_size is triggered...
      
}

template<typename t> bool enc_dec_int(t v) {
   enc_dec_buffer(v);
   EXPECT_TRUE(enc_dec_scalar(v)); // always true
   return enc_dec_varint(v); // sometimes true
}

bool enc_dec_test(unsigned long long v) {
   bool always_equal = true;
   always_equal &= enc_dec_int((char) v);
   always_equal &= enc_dec_int((unsigned char) v);
   always_equal &= enc_dec_int((short) v);
   always_equal &= enc_dec_int((unsigned short) v);
   always_equal &= enc_dec_int((int)v);
   always_equal &= enc_dec_int((unsigned)v);
   if (v <= 0xFFFFFFFF) {
      always_equal &= enc_dec_int((size_t)v);
   }
   always_equal &= enc_dec_int((long long)v);
   always_equal &= enc_dec_int((unsigned long long) v);
   return always_equal;
}

const unsigned long long SIGN_BIT = 0x8000000000000000ull;

TEST(test_enc_dec, int)
{
   bool always_equal = true;
   always_equal &= enc_dec_test(0);
   always_equal &= enc_dec_test(SIGN_BIT);

   for (unsigned long long i = 1; i != 0; i <<= 1) {
      always_equal &= enc_dec_test(i);
      always_equal &= enc_dec_test(i | SIGN_BIT);
   }

   for (unsigned long long i = 1; i != ~0ull; i = (i << 1) | 1) {
      always_equal &= enc_dec_test(i);
      always_equal &= enc_dec_test(i | SIGN_BIT);
   }

   for (unsigned long long i = 0; i != ~0ull; i = (i >> 1) | SIGN_BIT) {
      always_equal &= enc_dec_test(i);
      always_equal &= enc_dec_test(i | SIGN_BIT);
   }

   EXPECT_FALSE(always_equal); // somewhere estimate != actual :)

}

TEST(test_enc_dec, string)
{
   EXPECT_TRUE(enc_dec_scalar(string()));
   EXPECT_TRUE(enc_dec_scalar(string("1")));
   EXPECT_TRUE(enc_dec_scalar(string("\0"))); //; tricky :)
   EXPECT_TRUE(enc_dec_scalar(string("123")));
   EXPECT_TRUE(enc_dec_scalar(string("123\0")));
}

 struct set_temp {
    set<int> s1;
    DECLARE_ENC_DEC_MEMBER_FUNCTION();
    bool operator==(const set_temp& s) const { return s.s1 == s1; }
 };

DECLARE_ENC_DEC_CLASS(set_temp)
DEFINE_ENC_DEC_MEMBER_FUNCTION(set_temp) {
   p = enc_dec(p,s1);
   return p;
}

TEST(test_enc_dec, map) 
{
   map<int,int> m1;
   m1[1] = 1;
   m1[2] = 2;
   EXPECT_TRUE(enc_dec_scalar(m1));

   map<int,set_temp> m2;

   EXPECT_TRUE(enc_dec_scalar(m2));

   map<string,string> s;
   s["a"] = "b";
   EXPECT_TRUE(enc_dec_scalar(s));   
}

TEST(test_enc_dec, set) 
{
   set<int> s1;
   s1.insert(1);
   EXPECT_TRUE(enc_dec_scalar(s1));

   set_temp t;
   t.s1.insert(3);
   t.s1.insert(-1);
   enc_dec_scalar(t);

   set<string> s;
   s.insert("b");
   EXPECT_TRUE(enc_dec_scalar(s));   

}


TEST(test_enc_dec, buff1) {
   int i = 42;
   bufferlist b;
   enc(b,i);

   //
   // decode it
   //

   bufferlist::iterator it = b.begin();

   int z;
   dec(it,z);
   EXPECT_EQ(z,i);

}

struct map_context_test : public enc_dec_map_context<string,int> {
   int index;
   size_t      operator()(size_t p,string& s, int& i) { EXPECT_EQ(index,1); return enc_dec_pair(p,s,i); }
   char *      operator()(char * p,string& s, int& i) { EXPECT_EQ(index,i); ++i; return enc_dec_pair(p,s,i); }
   const char *operator()(const char *p,string&s,int&i) {p = enc_dec_pair(p,s,i); EXPECT_EQ(index,i); ++index; return p; }
};


TEST(test_enc_dec, map_context) {
   map_context_test t;
   map<string,int> m,m2;
   t.index = 1;
   
   m["a"] = 1;
   m["b"] = 2;
   m["c"] = 3;

   char buffer[100];

   size_t sz = enc_dec(size_t(0),m,t);

   char *end = enc_dec(buffer,m,t);

   EXPECT_EQ(sz,size_t(end-buffer));

   const char *dec_end = enc_dec((const char *)buffer,m2,t);

   EXPECT_EQ(dec_end,end);

   EXPECT_EQ(m,m2);
}

struct set_context_test : public enc_dec_set_context<int> {
   int index;
   size_t      operator()(size_t p,int& i) { EXPECT_EQ(index,1); return enc_dec(p,i); }
   char *      operator()(char * p,int& i) { EXPECT_EQ(index,i); ++i; return enc_dec(p,i); }
   const char *operator()(const char *p,int&i) {p = enc_dec(p,i); EXPECT_EQ(index,i); ++index; return p; }
};


TEST(test_enc_dec, set_context) {
   set_context_test t;
   set<int> s,s2;
   t.index = 1;
   
   s.insert(1);
   s.insert(2);
   s.insert(3);

   char buffer[100];

   size_t sz = enc_dec(size_t(0),s,t);

   char *end = enc_dec(buffer,s,t);

   EXPECT_EQ(sz,size_t(end-buffer));

   const char *dec_end = enc_dec((const char *)buffer,s2,t);

   EXPECT_EQ(dec_end,end);

   EXPECT_EQ(s,s2);
}

struct vector_context_test : public enc_dec_vector_context<int> {
   int index;
   size_t      operator()(size_t p,int& i) { EXPECT_EQ(index,1); return enc_dec(p,i); }
   char *      operator()(char * p,int& i) { EXPECT_EQ(index,i); ++i; return enc_dec(p,i); }
   const char *operator()(const char *p,int&i) {p = enc_dec(p,i); EXPECT_EQ(index,i); ++index; return p; }
};


TEST(test_enc_dec, vector_context) {
   vector_context_test t;
   vector<int> s,s2;
   t.index = 1;
   
   s.push_back(1);
   s.push_back(2);
   s.push_back(3);

   char buffer[100];

   size_t sz = enc_dec(size_t(0),s,t);

   char *end = enc_dec(buffer,s,t);

   EXPECT_EQ(sz,size_t(end-buffer));

   const char *dec_end = enc_dec((const char *)buffer,s2,t);

   EXPECT_EQ(dec_end,end);

   EXPECT_EQ(s,s2);
}

//#if 0
// bluestore_onode_t and blob_map
//
void
generate_dummy_onode(bluestore_onode_t& onode)
{
	onode.nid = 1252;
	onode.size = 65536;
        onode.attrs["attr1"] = bufferptr("attr_val1",10); 
        onode.attrs["attr2"] = bufferptr("attr_val2",10); 
        onode.extent_map[0] =  bluestore_lextent_t(23232, 0, 4096);
        onode.extent_map[1] =  bluestore_lextent_t(23232, 16384, 8192);
        onode.extent_map[2] =  bluestore_lextent_t(23232, 32768, 8192);
	onode.omap_head = 12;
	onode.expected_object_size = 4 * 65536;
	onode.expected_write_size = 65536;
	onode.alloc_hint_flags = 100;
}

char* gen_buffer(uint64_t size)
{
    char *buffer = new char[size];
    boost::random::random_device rand;
    rand.generate(buffer, buffer + size);
    return buffer;
}

void
generate_dummy_blob(bluestore_blob_t& blob)
{
  blob.init_csum(bluestore_blob_t::CSUM_XXHASH32, 16, 65536);
  //blob.csum_data = buffer::claim_malloc(4, strdup("abcd"));
  char *buf = gen_buffer(8);
  blob.csum_data = buffer::claim_char(8, buf);
  blob.ref_map.get(3, 5);
  blob.add_unused(0, 3, 4096);
  blob.add_unused(8, 8, 4096);
  blob.extents.emplace_back(bluestore_pextent_t(0x40100000, 0x10000));
  blob.extents.emplace_back(
    bluestore_pextent_t(bluestore_pextent_t::INVALID_OFFSET, 0x1000));
  blob.extents.emplace_back(bluestore_pextent_t(0x40120000, 0x10000));
}

void
generate_dummy_blob_map(BlueStore::BlobMap& map)
{
  BlueStore::Blob *b = new BlueStore::Blob();
  b->blob.init_csum(bluestore_blob_t::CSUM_XXHASH32, 16, 65536);
  b->blob.csum_data = buffer::claim_malloc(4, strdup("abcd"));
  b->blob.ref_map.get(3, 5);
  b->blob.add_unused(0, 3, 4096);
  b->blob.add_unused(8, 8, 4096);
  b->blob.extents.emplace_back(bluestore_pextent_t(0x40100000, 0x10000));
  b->blob.extents.emplace_back(
    bluestore_pextent_t(bluestore_pextent_t::INVALID_OFFSET, 0x1000));
  b->blob.extents.emplace_back(bluestore_pextent_t(0x40120000, 0x10000));
  map.claim(b);
  b = new BlueStore::Blob();
  b->blob.init_csum(bluestore_blob_t::CSUM_XXHASH32, 16, 65536);
  b->blob.csum_data = buffer::claim_malloc(4, strdup("abcd"));
  b->blob.ref_map.get(3, 5);
  b->blob.add_unused(0, 3, 4096);
  b->blob.add_unused(8, 8, 4096);
  b->blob.extents.emplace_back(bluestore_pextent_t(0x40100000, 0x10000));
  b->blob.extents.emplace_back(
    bluestore_pextent_t(bluestore_pextent_t::INVALID_OFFSET, 0x1000));
  b->blob.extents.emplace_back(bluestore_pextent_t(0x40120000, 0x10000));
  map.claim(b);
  b = new BlueStore::Blob();
  b->blob.init_csum(bluestore_blob_t::CSUM_XXHASH32, 16, 65536);
  b->blob.csum_data = buffer::claim_malloc(4, strdup("abcd"));
  b->blob.ref_map.get(3, 5);
  b->blob.add_unused(0, 3, 4096);
  b->blob.add_unused(8, 8, 4096);
  b->blob.extents.emplace_back(bluestore_pextent_t(0x40100000, 0x10000));
  b->blob.extents.emplace_back(
    bluestore_pextent_t(bluestore_pextent_t::INVALID_OFFSET, 0x1000));
  b->blob.extents.emplace_back(bluestore_pextent_t(0x40120000, 0x10000));
  map.claim(b);
  b = new BlueStore::Blob();
  b->blob.init_csum(bluestore_blob_t::CSUM_XXHASH32, 16, 65536);
  b->blob.csum_data = buffer::claim_malloc(4, strdup("abcd"));
  b->blob.ref_map.get(3, 5);
  b->blob.add_unused(0, 3, 4096);
  b->blob.add_unused(8, 8, 4096);
  b->blob.extents.emplace_back(bluestore_pextent_t(0x40100000, 0x10000));
  b->blob.extents.emplace_back(
    bluestore_pextent_t(bluestore_pextent_t::INVALID_OFFSET, 0x1000));
  b->blob.extents.emplace_back(bluestore_pextent_t(0x40120000, 0x10000));
  map.claim(b);
}


TEST(test_enc_dec, lextent_enc_dec) {
   bluestore_lextent_t l1(23232, 0, 4096);
   bluestore_lextent_t l2;
   bufferlist bl;
   std::cout << " L1 extent: " << l1 << std::endl;
   enc(bl, l1);
   bufferlist::iterator it = bl.begin();
   dec(it,l2);
   std::cout << " L2 extent: " << l2 << std::endl;
}

TEST(test_enc_dec, onode_enc_dec) {
  bluestore_onode_t onode;
  generate_dummy_onode(onode);
  bluestore_onode_t o2;
  std::cout << " nid: " << onode.nid << std::endl;
  bufferlist bl;
  enc(bl, onode);
  bufferlist::iterator it = bl.begin();
  dec(it,o2);
  std::cout << " nid: " << o2.nid << std::endl;
}

TEST(test_enc_dec, blob_enc_dec) {
  bluestore_blob_t blob;
  generate_dummy_blob(blob);
  std::cout << "Blob: " << blob << std::endl;
  bluestore_blob_t blob2;
  bufferlist bl;
  enc(bl, blob);
  std::cout<< " Blob encoded length: " << bl.length() << std::endl;
  bufferlist::iterator it = bl.begin();
  dec(it, blob2);
  std::cout << "Blob: " << blob2 << std::endl;
}

TEST(test_enc_dec, blobmap_enc_dec) {
  BlueStore::BlobMap map1, map2;
  generate_dummy_blob_map(map1);
  std::cout << "BlobMap1: " << map1 << std::endl;
  bufferlist bl;
  enc(bl, map1);
  std::cout<< " Blob map encoded length: " << bl.length() << std::endl;
  bufferlist::iterator it = bl.begin();
  dec(it,map2);
  std::cout << "BlobMap2: " << map2 << std::endl;
}


int main(int argc, char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}


/*
 * Local Variables:
 * compile-command: "cd .. ; make -j4 &&
 *   make unittest_enc_dec &&
 *   valgrind --tool=memcheck ./unittest_enc_dec --gtest_filter=*.*"
 * End:
 */
