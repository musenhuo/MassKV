#include "hmasstree_wrapper.h"

// thread_local typename HMasstreeWrapper::table_params::threadinfo_type* HMasstreeWrapper::ti = nullptr;
thread_local int HMasstreeWrapper::thread_id = 0;
bool HMasstreeWrapper::stopping = false;
uint32_t HMasstreeWrapper::printing = 0;
kvtimestamp_t initial_timestamp;

volatile mrcu_epoch_type active_epoch = 1;
volatile uint64_t globalepoch = 1;
volatile bool recovering = false;
