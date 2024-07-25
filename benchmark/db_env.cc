#include "db_env.h"

DBEnv* DBEnv::instance_ = nullptr;
std::mutex DBEnv::mutex_;