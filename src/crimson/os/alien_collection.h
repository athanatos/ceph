#pragma once

#include "os/ObjectStore.h"

#include "futurized_collection.h"
#include "futurized_store.h"
#include "alien_store.h"

namespace ceph::os {

struct AlienCollection final : public FuturizedCollection {

  ObjectStore::CollectionHandle sch;

  AlienCollection(ObjectStore::CollectionHandle ch)
  : FuturizedCollection(ch->cid),
    sch(ch) {}

  ~AlienCollection() {}
  seastar::future<> clear (FuturizedStore* store) final
  {
    auto s = static_cast<AlienStore*>(store);
    return  s->tp.submit([this](){
      sch.reset();
      return 0;
    }).then([](int r){
      return seastar::now();
    });
  }
};

}
