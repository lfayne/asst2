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
            [this] { return stop_threads || !(bulk_launch_count == launches_completed); }
        );

        if (!work_queue.empty()) {
            Work task = work_queue.front();
            work_queue.pop();
            work_lock.unlock();

            task.runnable->runTask(task.task_num, task.num_total_tasks);
            // std::unique_lock<std::mutex> launch_lock(task.launch_ptr->m);
            task.launch_ptr->tasks_done++;
            //std::cout << task.launch_ptr->id << " " << task.launch_ptr->tasks_done << " " << task.launch_ptr->num_total_tasks << "\n" << std::flush;

            if (task.launch_ptr->tasks_done == task.launch_ptr->num_total_tasks) {
                std::cout << task.launch_ptr->id << "\n" << std::flush;
                //std::unique_lock<std::mutex> launch_lock(task.launch_ptr->m);
                //std::unique_lock<std::mutex> launches_completed_lock(completed_m);
                launches_completed++;
                std::cout << "happy\n" << std::flush;
                //launches_completed_lock.unlock();

                std::unique_lock<std::mutex> deps_lock(deps_m);
                for (TaskID dep : deps_map.at(task.launch_ptr->id)) {
                    BulkLaunch* new_launch = id_to_ptr[dep];
                    new_launch->deps.erase(task.launch_ptr->id);

                    if (new_launch->deps.empty()) {
                        std::cout << new_launch->id << "\n" << std::flush;
                        work_lock.lock();
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
                }
                deps_lock.unlock();
                
                //std::unique_lock<std::mutex> id_lock(id_m);
                //launches_completed_lock.lock();
                if (bulk_launch_count == launches_completed) {
                    done_cv.notify_one();
                }
                //launches_completed_lock.unlock();
                //id_lock.unlock();
                std::cout << "sad\n" << std::flush;
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
    bulk_launch_count = 0;
    launches_completed = 0;

    for (int i=0; i < this->num_threads; i++) {
        this->threads[i] = std::thread(
            &TaskSystemParallelThreadPoolSleeping::threadSpinSleep, this);
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    // TODO: Deallocate BulkLaunch objs.
    stop_threads = true;
    queue_cv.notify_all();
    // for (const auto& pair : id_to_ptr) {
    //     delete pair.second;
    // }
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
    //std::unique_lock<std::mutex> id_lock(id_m);
    TaskID id = bulk_launch_count;
    bulk_launch_count++;
    //id_lock.unlock();

    BulkLaunch* bulk_launch_ptr = new BulkLaunch();
    std::unique_lock<std::mutex> self_lock(bulk_launch_ptr->m);
    bulk_launch_ptr->runnable = runnable;
    bulk_launch_ptr->tasks_done = 0;
    bulk_launch_ptr->num_total_tasks = num_total_tasks;
    bulk_launch_ptr->deps = std::set<TaskID> (deps.begin(), deps.end());
    bulk_launch_ptr->working = false;
    bulk_launch_ptr->id = id;

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
    self_lock.unlock();
    queue_cv.notify_all();

    return id;
}

void TaskSystemParallelThreadPoolSleeping::sync() {
    //std::unique_lock<std::mutex> id_lock(id_m);
    //std::unique_lock<std::mutex> launches_completed_lock(completed_m);
    if (bulk_launch_count == launches_completed) {
        return;
    }
    //id_lock.unlock();
    //launches_completed_lock.unlock();

    std::unique_lock<std::mutex> done_lock(done_m);
    done_cv.wait(
        done_lock,
        [this] {
            //std::unique_lock<std::mutex> id_lock(id_m);
            //std::unique_lock<std::mutex> launches_completed_lock(completed_m);
            return bulk_launch_count == launches_completed;
        }
    );

    //std::cout << "FINISHED FINISHED FINISHED FINISHED\n" << std::flush;
    return;
}

/* 
Map to store TaskID : queue of work (also consider deps)
Set for TaskIDs completed
Sync only called after Async Runs have been called
Threads spin and look in normal work queue, else look in TaskMap to find more pieces of work to add to queue
Possibly more maps for task done vs. total tasks per bulk launch
*/ 