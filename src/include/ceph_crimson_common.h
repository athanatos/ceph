#pragma once

#if defined(WITH_SEASTAR) && !defined(WITH_ALIEN)
#define TOPNSPC crimson
#define CRIMSONENV
#elif defined(WITH_ALIEN)
#define TOPNSPC alien
#define ALIENENV
#else
#define TOPNSPC ceph
#endif

