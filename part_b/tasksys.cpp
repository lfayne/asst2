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
            [this] { return stop_threads || !work_queue.empty(); }
        );

        if (!work_queue.empty()) {
            Work task = work_queue.front();
            work_queue.pop();
            work_lock.unlock();
            
            task.runnable->runTask(task.task_num, task.num_total_tasks);
            task.launch_ptr->tasks_done++;

            if (task.launch_ptr->tasks_done == task.launch_ptr->num_total_tasks) {
                std::call_once(task.launch_ptr->done, [this, task](){
                    launches_completed++;

                    std::unique_lock<std::mutex> deps_lock(deps_m);
                    for (TaskID dep : deps_map.at(task.launch_ptr->id)) {
                        BulkLaunch* new_launch = id_to_ptr[dep];
                        std::unique_lock<std::mutex> new_launch_lock(new_launch->m);
                        new_launch->deps.erase(task.launch_ptr->id);

                        if (new_launch->deps.empty()) {
                            std::unique_lock<std::mutex> work_lock(work_m);
                            for (int i=0; i<new_launch->num_total_tasks; i++) {
                                Work task;
                                task.runnable = new_launch->runnable;
                                task.task_num = i;
                                task.num_total_tasks = new_launch->num_total_tasks;
                                task.launch_ptr = new_launch;
                                work_queue.push(task);
                            }
                            work_lock.unlock();
                        }
                        new_launch_lock.unlock();
                    }
                    deps_lock.unlock();
                    
                    if (bulk_launch_count == launches_completed) {
                        std::unique_lock<std::mutex> done_lock(done_m);
                        done_cv.notify_all();
                    }
                });
                queue_cv.notify_all();
            }
        } else if (stop_threads) {
            work_lock.unlock();
            queue_cv.notify_all();
            return;
        }
    }
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    this->num_threads = num_threads;
    this->threads = new std::thread[num_threads]();
    this->stop_threads = false;
    launches_completed = 0;
    bulk_launch_count = 0;

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
    for (const auto& pair : id_to_ptr) {
        delete pair.second;
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
    std::unique_lock<std::mutex> deps_lock(deps_m);
    TaskID id = bulk_launch_count;
    bulk_launch_count++;

    BulkLaunch* bulk_launch_ptr = new BulkLaunch();
    std::unique_lock<std::mutex> self_lock(bulk_launch_ptr->m);
    bulk_launch_ptr->runnable = runnable;
    bulk_launch_ptr->tasks_done = 0;
    bulk_launch_ptr->num_total_tasks = num_total_tasks;
    bulk_launch_ptr->deps = std::set<TaskID> (deps.begin(), deps.end());
    bulk_launch_ptr->id = id;

    id_to_ptr.insert({id, bulk_launch_ptr});
    if (!deps_map.count(id)) {
        deps_map.insert({id, std::vector<TaskID>()});
    }
    
    if (!deps.empty()) {
        for (TaskID dep : deps) {
            if (id_to_ptr.count(dep) && id_to_ptr[dep]->tasks_done == id_to_ptr[dep]->num_total_tasks) {
                bulk_launch_ptr->deps.erase(dep);
            } else {
                if (!deps_map.count(dep)) {
                    deps_map.insert({dep, std::vector<TaskID>()});
                }
                deps_map.at(dep).push_back(id);
            }
        }
    }
    
    if (bulk_launch_ptr->deps.empty()) {
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
    } 

    self_lock.unlock();
    queue_cv.notify_all();
    
    return id;
}

void TaskSystemParallelThreadPoolSleeping::sync() {
    if (bulk_launch_count == launches_completed) {
        return;
    }

    std::unique_lock<std::mutex> done_lock(done_m);
    done_cv.wait(
        done_lock,
        [this] {
            return bulk_launch_count == launches_completed;
        }
    );
    return;
}
