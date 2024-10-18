#include "tasksys.h"


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync() {
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

void threadSpinSleep(
    std::queue<Work>* work_queue, std::map<TaskID, BulkLaunch>* bulk_launch_map,
    std::mutex* work_m, std::mutex* task_m,
    std::condition_variable* queue_cv, std::condition_variable* done_cv,
    bool* stop_threads
) {
    while(true) {
        std::unique_lock<std::mutex> work_lock(*work_m);
        queue_cv->wait(
            work_lock,
            [stop_threads, work_queue, bulk_launch_map] { return *stop_threads || !work_queue->empty() || !bulk_launch_map->empty(); }
        );

        if (!work_queue->empty()) {
            Work task = work_queue->front();
            work_queue->pop();
            work_lock.unlock();

            task.runnable->runTask(task.task_num, task.num_total_tasks);

            std::unique_lock<std::mutex> task_lock(*task_m);
            //std::cout << work_queue->size() << " " << task.id << " " <<  task.task_num << " " << bulk_launch_map->at(task.id).tasks_done << "\n" << std::flush;
            bulk_launch_map->at(task.id).tasks_done++;
            //std::cout << task.id << " " <<  task.task_num << " " << bulk_launch_map->at(task.id).tasks_done << "\n" << std::flush;

            if (bulk_launch_map->at(task.id).tasks_done == bulk_launch_map->at(task.id).num_total_tasks) {
                bulk_launch_map->erase(task.id);

                if (work_queue->empty() && bulk_launch_map->empty()) {
                    done_cv->notify_one();
                }
            }
            // task_lock.unlock();
        } else if (!bulk_launch_map->empty()) {
            work_lock.unlock();
            std::unique_lock<std::mutex> task_lock(*task_m);
            auto iter = bulk_launch_map->begin();

            while (iter != bulk_launch_map->end()) { 
                // std::cout << << std::flush;
                bool add = !iter->second.working;
                for (TaskID dep : iter->second.deps) {
                    if (bulk_launch_map->count(dep)) {
                        add = false;
                        break;
                    }
                }

                if (add) {
                    work_lock.lock();
                    for (int i=0; i<iter->second.num_total_tasks; i++) {
                        Work task;
                        task.runnable = iter->second.runnable;
                        task.task_num = i;
                        task.num_total_tasks = iter->second.num_total_tasks;
                        task.id = iter->first;
                        work_queue->push(task);
                    }
                    bulk_launch_map->at(iter->first).working = true;
                    work_lock.unlock();
                    queue_cv->notify_all();
                }

                iter++;
            }
        } else if (*stop_threads) {
            return;
        }
    }
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    this->num_threads = num_threads;
    this->threads = new std::thread[num_threads]();
    this->stop_threads = false;

    for (int i=0; i < this->num_threads; i++) {
        this->threads[i] = std::thread(
            threadSpinSleep,
            &this->work_queue, &this->bulk_launch_map,
            &this->work_m, &this->task_m,
            &this->queue_cv, &this->done_cv,
            &this->stop_threads);
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    stop_threads = true;
    queue_cv.notify_all();
    for (int i = 0; i < this->num_threads; i++) {
        threads[i].join();
    }
    delete[] threads;
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    std::vector<TaskID> no_deps;
    runAsyncWithDeps(runnable, num_total_tasks, no_deps);
    sync();
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {
    std::unique_lock<std::mutex> task_lock(this->task_m);
    TaskID id = bulk_launch_count;
    bulk_launch_count++;

    BulkLaunch bulk_launch;
    bulk_launch.runnable = runnable;
    bulk_launch.tasks_done = 0;
    bulk_launch.num_total_tasks = num_total_tasks;
    bulk_launch.deps = deps;
    bulk_launch.working = false;

    bulk_launch_map.insert({id, bulk_launch});
    //std::cout << "BULK LAUNCH: " << id << ", TASKS: " << num_total_tasks << "\n" << std::flush;
    task_lock.unlock();
    this->queue_cv.notify_all();

    return id;
}

void TaskSystemParallelThreadPoolSleeping::sync() {
    std::unique_lock<std::mutex> tasks_lock(this->task_m);

    this->done_cv.wait(
        tasks_lock,
        [this] {
            return this->bulk_launch_map.empty() && this->work_queue.empty();
        }
    );

    return;
}

/* 
Map to store TaskID : queue of work (also consider deps)
Set for TaskIDs completed
Sync only called after Async Runs have been called
Threads spin and look in normal work queue, else look in TaskMap to find more pieces of work to add to queue
Possibly more maps for task done vs. total tasks per bulk launch
*/ 