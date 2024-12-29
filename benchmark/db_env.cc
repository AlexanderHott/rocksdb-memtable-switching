#include "db_env.h"
#include <mutex>

DBEnv* DBEnv::instance_ = nullptr;
std::mutex DBEnv::mutex_;