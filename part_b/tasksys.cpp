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
            [this] { return stop_threads || !work_queue.empty() || !(bulk_launch_count == launches_completed); }
        );

        if (!work_queue.empty()) {
            Work task = work_queue.front();
            work_queue.pop();
            work_lock.unlock();

            task.runnable->runTask(task.task_num, task.num_total_tasks);
            std::unique_lock<std::mutex> launch_lock(*bulk_launch_map.at(task.id).m);
            bulk_launch_map.at(task.id).tasks_done++;

            if (bulk_launch_map.at(task.id).tasks_done == bulk_launch_map.at(task.id).num_total_tasks) {
                std::unique_lock<std::mutex> launches_completed_lock(completed_m);
                launches_completed++;
                launches_completed_lock.unlock();

                if (!deps_map.count(task.id)) {
                    std::cout << "deps map task id\n" << std::flush;
                    usleep(100000);
                }
                for (TaskID dep : deps_map.at(task.id)) {
                    if (!bulk_launch_map.count(dep)) {
                        std::cout << "dep\n" << std::flush;
                        usleep(100000);
                    }
                    BulkLaunch* new_launch = &bulk_launch_map.at(dep);
                    std::unique_lock<std::mutex> dep_lock(*new_launch->m);
                    new_launch->deps.erase(task.id);
                    dep_lock.unlock();

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
                if (work_queue.empty() && bulk_launch_map.size() == launches_completed) {
                    done_cv.notify_one();
                }
            }
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
    std::unique_lock<std::mutex> id_lock(id_m);
    TaskID id = bulk_launch_count;
    bulk_launch_count++;
    id_lock.unlock();

    std::mutex* mutex = new std::mutex();
    std::unique_lock<std::mutex> self_lock(*mutex);

    BulkLaunch* bulk_launch_ptr = new BulkLaunch();
    bulk_launch_ptr->runnable = runnable;
    bulk_launch_ptr->tasks_done = 0;
    bulk_launch_ptr->num_total_tasks = num_total_tasks;
    bulk_launch_ptr->deps = std::set<TaskID> (deps.begin(), deps.end());
    bulk_launch_ptr->working = false;
    bulk_launch_ptr->m = mutex;

    std::unique_lock<std::mutex> deps_lock(deps_m);
    if (!deps_map.count(id)) {
        id_to_ptr.insert({id, bulk_launch_ptr});
        deps_map.insert({id, std::vector<TaskID>()});
    }
    deps_lock.unlock();
    
    if (deps.empty()) {
        std::unique_lock<std::mutex> work_lock(work_m);
        for (int i=0; i<num_total_tasks; i++) {
            Work task;
            task.runnable = runnable;
            task.task_num = i;
            task.num_total_tasks = num_total_tasks;
            task.launch_ptr = bulk_launch_ptr;
            work_queue.push(task);
        }
        work_lock.unlock();
    } else {
        deps_lock.lock();
        for (TaskID dep : deps) {
            if (!deps_map.count(dep)) {
                deps_map.insert({dep, std::vector<TaskID>()});
            }
            deps_map.at(dep).push_back(id);
        }
        deps_lock.unlock();
    }
    queue_cv.notify_all();

    return id;
}

void TaskSystemParallelThreadPoolSleeping::sync() {
    if ((bulk_launch_count == launches_completed) && work_queue.empty()) {
        return;
    }

    std::unique_lock<std::mutex> done_lock(done_m);
    done_cv.wait(
        done_lock,
        [this] {
            return (bulk_launch_count == launches_completed) && work_queue.empty();
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