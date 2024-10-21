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

void TaskSystemParallelThreadPoolSleeping::threadSpinSleep() {
    while(true) {
        std::unique_lock<std::mutex> work_lock(work_m);
        queue_cv.wait(
            work_lock,
            [this] { return stop_threads || !work_queue.empty() || !bulk_launch_map.empty(); }
        );

        if (!work_queue.empty()) {
            Work task = work_queue.front();
            work_queue.pop();
            work_lock.unlock();

            task.runnable->runTask(task.task_num, task.num_total_tasks);

            std::unique_lock<std::mutex> launch_lock(*bulk_launch_map.at(task.id).m);
            //std::cout << work_queue.size() << " " << task.id << " " <<  task.task_num << " " << bulk_launch_map.at(task.id).tasks_done << "\n" << std::flush;
            bulk_launch_map.at(task.id).tasks_done++;

            if (bulk_launch_map.at(task.id).tasks_done == bulk_launch_map.at(task.id).num_total_tasks) {
                std::unique_lock<std::mutex> launch_map_lock(launch_m);
                for (TaskID dep : deps_map.at(task.id)) {
                    BulkLaunch* new_launch = &bulk_launch_map.at(dep);
                    new_launch->deps.erase(task.id);

                    if (new_launch->deps.empty()) {
                        work_lock.lock();
                        for (int i=0; i<new_launch->num_total_tasks; i++) {
                            Work task;
                            task.runnable = new_launch->runnable;
                            task.task_num = i;
                            task.num_total_tasks = new_launch->num_total_tasks;
                            task.id = dep;
                            work_queue.push(task);
                        }
                        work_lock.unlock();
                    }
                }

                bulk_launch_map.erase(task.id);
                launch_map_lock.unlock();

                if (work_queue.empty() && bulk_launch_map.empty()) {
                    done_cv.notify_one();
                }
            }
            launch_lock.unlock();
            queue_cv.notify_all();
        } else if (stop_threads) {
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
            &TaskSystemParallelThreadPoolSleeping::threadSpinSleep, this);
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
    TaskID id = bulk_launch_count;
    bulk_launch_count++;

    std::mutex* mutex = new std::mutex();

    BulkLaunch bulk_launch;
    bulk_launch.runnable = runnable;
    bulk_launch.tasks_done = 0;
    bulk_launch.num_total_tasks = num_total_tasks;
    bulk_launch.deps = std::set<TaskID> (deps.begin(), deps.end());
    bulk_launch.working = false;
    bulk_launch.m = mutex;

    std::unique_lock<std::mutex> map_lock(this->launch_m);
    bulk_launch_map.insert({id, bulk_launch});
    deps_map.insert({id, {}});
    
    if (deps.empty()) {
        map_lock.unlock();
        std::unique_lock<std::mutex> work_lock(work_m);
        for (int i=0; i<num_total_tasks; i++) {
            Work task;
            task.runnable = runnable;
            task.task_num = i;
            task.num_total_tasks = num_total_tasks;
            task.id = id;
            work_queue.push(task);
        }
        work_lock.unlock();
    } else {
        for (TaskID dep : deps) {
            deps_map.at(dep).push_back(id);
        }
        map_lock.unlock();
    }
    //std::cout << "BULK LAUNCH: " << id << ", TASKS: " << num_total_tasks << "\n" << std::flush;
    queue_cv.notify_all();

    return id;
}

void TaskSystemParallelThreadPoolSleeping::sync() {
    std::unique_lock<std::mutex> launch_lock(launch_m);

    done_cv.wait(
        launch_lock,
        [this] {
            return bulk_launch_map.empty() && work_queue.empty();
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