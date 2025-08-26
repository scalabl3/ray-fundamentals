#!/usr/bin/env python3
"""
RAY CORE DEBUGGING & TROUBLESHOOTING - File 04
==============================================

This file covers debugging techniques essential for Ray Core:
1. Common Ray errors and their solutions
2. Performance debugging and profiling
3. Memory management and object store issues
4. Task and actor monitoring
5. Resource management problems
6. Debugging distributed applications

FOCUS AREAS:
- Diagnosing Ray application issues
- Understanding Ray's internal mechanics
- Performance optimization strategies
- Resource monitoring and management
- Common pitfalls and solutions
"""

import ray
import time
import random
import threading
import psutil
import sys
import gc
from typing import List, Dict, Any, Optional
import numpy as np
import traceback
from contextlib import contextmanager

# =============================================================================
# 1. RAY INITIALIZATION AND CONNECTION ISSUES
# =============================================================================

def demonstrate_initialization_issues():
    """
    Common Ray initialization problems and solutions.
    SCENARIO: "Your Ray application won't start - debug it!"
    """
    print("\n=== RAY INITIALIZATION DEBUGGING ===")
    
    # Issue 1: Multiple initialization attempts
    print("1. Multiple initialization issue:")
    try:
        # First initialization
        if not ray.is_initialized():
            ray.init(num_cpus=2)
            print("✅ Ray initialized successfully")
        
        # Second initialization attempt (will fail without ignore_reinit_error)
        try:
            ray.init(num_cpus=4)  # This should fail
        except RuntimeError as e:
            print(f"❌ Expected error: {e}")
            print("💡 Solution: Use ignore_reinit_error=True or check ray.is_initialized()")
        
        # Correct way to reinitialize
        ray.shutdown()
        ray.init(num_cpus=4, ignore_reinit_error=True)
        print("✅ Correct reinitialization")
        
    except Exception as e:
        print(f"❌ Initialization error: {e}")
    
    # Issue 2: Resource specification problems
    print("\n2. Resource specification issues:")
    try:
        # Get available resources
        available = ray.available_resources()
        print(f"Available resources: {available}")
        
        # Try to request more resources than available
        requested_cpus = available.get('CPU', 0) + 10
        print(f"Requesting {requested_cpus} CPUs (more than available)")
        
        # This won't fail immediately but tasks may hang
        print("💡 Tasks requesting unavailable resources will hang!")
        
    except Exception as e:
        print(f"Resource error: {e}")

# =============================================================================
# 2. OBJECT STORE AND MEMORY DEBUGGING
# =============================================================================

@ray.remote
def memory_intensive_task(size_mb: int) -> np.ndarray:
    """Create large array to test memory issues."""
    print(f"Creating {size_mb}MB array")
    # 1MB = 1024*1024 bytes, float64 = 8 bytes
    array_size = (size_mb * 1024 * 1024) // 8
    return np.random.rand(array_size)

@ray.remote
def memory_leak_task(iterations: int) -> List[np.ndarray]:
    """Task that accumulates memory without cleanup."""
    arrays = []
    for i in range(iterations):
        array = np.random.rand(1000, 1000)  # ~8MB each
        arrays.append(array)
        if i % 10 == 0:
            print(f"Created {i+1} arrays, memory growing...")
    return arrays

def demonstrate_memory_debugging():
    """
    Memory management and object store debugging.
    CRITICAL: "Your Ray app is running out of memory - fix it!"
    """
    print("\n=== MEMORY AND OBJECT STORE DEBUGGING ===")
    
    # Check initial memory state
    def get_memory_info():
        """Get current memory usage information."""
        process = psutil.Process()
        memory_info = process.memory_info()
        return {
            "rss_mb": memory_info.rss / 1024 / 1024,
            "vms_mb": memory_info.vms / 1024 / 1024,
            "percent": process.memory_percent()
        }
    
    print("1. Object store monitoring:")
    initial_memory = get_memory_info()
    print(f"Initial memory: {initial_memory}")
    
    # Create some large objects
    print("Creating large objects in object store...")
    large_objects = []
    for i in range(3):
        obj_ref = memory_intensive_task.remote(50)  # 50MB each
        large_objects.append(obj_ref)
    
    # Get the objects (this loads them into memory)
    arrays = ray.get(large_objects)
    current_memory = get_memory_info()
    print(f"Memory after loading: {current_memory}")
    print(f"Memory increase: {current_memory['rss_mb'] - initial_memory['rss_mb']:.1f}MB")
    
    # Issue 1: Object store spillage
    print("\n2. Object store spillage detection:")
    try:
        # Create many large objects to trigger spillage
        spillage_objects = []
        for i in range(5):
            obj_ref = memory_intensive_task.remote(100)  # 100MB each
            spillage_objects.append(obj_ref)
            print(f"Created object {i+1}")
        
        print("💡 Check Ray dashboard for object store spillage warnings!")
        print("💡 Monitor /tmp/ray/session_*/logs/ for spillage messages")
        
    except Exception as e:
        print(f"Spillage test error: {e}")
    
    # Issue 2: Memory leaks
    print("\n3. Memory leak detection:")
    try:
        # This task creates memory leaks
        leak_future = memory_leak_task.remote(20)
        
        # Don't get the result - this keeps objects in memory
        print("Created memory leak task (not retrieving result)")
        print("💡 Objects remain in object store until explicitly deleted or process ends")
        
        # Solution: Explicitly delete references
        del large_objects
        del arrays
        gc.collect()  # Force garbage collection
        
        after_cleanup = get_memory_info()
        print(f"Memory after cleanup: {after_cleanup}")
        
    except Exception as e:
        print(f"Memory leak test error: {e}")
    
    # Debugging tips
    print("\n4. Memory debugging tips:")
    print("✅ Monitor Ray dashboard object store tab")
    print("✅ Use ray.get() judiciously - don't hold unnecessary references")
    print("✅ Check for object spillage in Ray logs")
    print("✅ Use ray.put() for large shared data")
    print("✅ Explicitly delete large ObjectRefs when done")

# =============================================================================
# 3. TASK AND ACTOR DEBUGGING
# =============================================================================

@ray.remote
class DebuggableActor:
    """Actor with debugging capabilities."""
    
    def __init__(self, actor_id: str):
        self.actor_id = actor_id
        self.call_count = 0
        self.error_count = 0
        self.total_time = 0
    
    def working_method(self, delay: float = 0.1) -> Dict[str, Any]:
        """Method that works correctly."""
        start_time = time.time()
        self.call_count += 1
        
        time.sleep(delay)
        
        duration = time.time() - start_time
        self.total_time += duration
        
        return {
            "actor_id": self.actor_id,
            "call_number": self.call_count,
            "duration": duration,
            "status": "success"
        }
    
    def buggy_method(self, should_fail: bool = False) -> str:
        """Method that sometimes fails for debugging."""
        self.call_count += 1
        
        if should_fail:
            self.error_count += 1
            raise ValueError(f"Intentional error in {self.actor_id}")
        
        return f"Success from {self.actor_id}"
    
    def hanging_method(self, hang_duration: float = 10.0) -> str:
        """Method that hangs to test timeout behavior."""
        print(f"Starting hanging method for {hang_duration}s")
        time.sleep(hang_duration)
        return f"Finally completed after {hang_duration}s"
    
    def get_debug_info(self) -> Dict[str, Any]:
        """Get debugging information about the actor."""
        return {
            "actor_id": self.actor_id,
            "call_count": self.call_count,
            "error_count": self.error_count,
            "total_time": self.total_time,
            "avg_time": self.total_time / self.call_count if self.call_count > 0 else 0,
            "error_rate": self.error_count / self.call_count if self.call_count > 0 else 0
        }

@ray.remote
def flaky_task(task_id: int, failure_rate: float = 0.3) -> Dict[str, Any]:
    """Task that randomly fails for testing error handling."""
    start_time = time.time()
    
    # Random processing time
    processing_time = random.uniform(0.1, 0.5)
    time.sleep(processing_time)
    
    # Random failure
    if random.random() < failure_rate:
        raise RuntimeError(f"Task {task_id} failed randomly")
    
    return {
        "task_id": task_id,
        "processing_time": processing_time,
        "completed_at": time.time(),
        "duration": time.time() - start_time
    }

def demonstrate_task_actor_debugging():
    """
    Debugging tasks and actors.
    SCENARIO: "Tasks are failing/hanging - debug and fix!"
    """
    print("\n=== TASK AND ACTOR DEBUGGING ===")
    
    # 1. Task failure debugging
    print("1. Task failure debugging:")
    task_futures = [flaky_task.remote(i, 0.4) for i in range(10)]
    
    successful_tasks = []
    failed_tasks = []
    
    for i, future in enumerate(task_futures):
        try:
            result = ray.get(future)
            successful_tasks.append(result)
            print(f"✅ Task {i}: {result['duration']:.3f}s")
        except Exception as e:
            failed_tasks.append({"task_id": i, "error": str(e)})
            print(f"❌ Task {i}: {e}")
    
    print(f"Success rate: {len(successful_tasks)}/{len(task_futures)}")
    
    # 2. Actor debugging
    print("\n2. Actor debugging:")
    actor = DebuggableActor.remote("debug-actor-1")
    
    # Test working methods
    working_futures = [actor.working_method.remote(0.1) for _ in range(5)]
    working_results = ray.get(working_futures)
    
    # Test buggy methods
    buggy_futures = [
        actor.buggy_method.remote(i % 3 == 0)  # Fail every 3rd call
        for i in range(6)
    ]
    
    for i, future in enumerate(buggy_futures):
        try:
            result = ray.get(future)
            print(f"✅ Buggy method {i}: {result}")
        except Exception as e:
            print(f"❌ Buggy method {i}: {e}")
    
    # Get debug info
    debug_info = ray.get(actor.get_debug_info.remote())
    print(f"Actor debug info: {debug_info}")
    
    # 3. Timeout debugging
    print("\n3. Timeout and hanging task debugging:")
    try:
        # Start a hanging method
        hanging_future = actor.hanging_method.remote(2.0)
        
        # Use ray.wait with timeout to detect hanging
        ready, not_ready = ray.wait([hanging_future], timeout=1.0)
        
        if not ready:
            print("⚠️  Method is taking longer than expected")
            print("💡 In production, you might cancel or retry")
            
            # Wait a bit more
            result = ray.get(hanging_future)
            print(f"✅ Eventually completed: {result}")
        else:
            result = ray.get(ready[0])
            print(f"✅ Completed quickly: {result}")
    
    except Exception as e:
        print(f"❌ Timeout debugging error: {e}")

# =============================================================================
# 4. PERFORMANCE DEBUGGING AND PROFILING
# =============================================================================

@contextmanager
def ray_performance_monitor():
    """Context manager for monitoring Ray performance."""
    start_time = time.time()
    start_memory = psutil.Process().memory_info().rss / 1024 / 1024
    
    print("🔍 Starting performance monitoring...")
    
    try:
        yield
    finally:
        end_time = time.time()
        end_memory = psutil.Process().memory_info().rss / 1024 / 1024
        
        duration = end_time - start_time
        memory_delta = end_memory - start_memory
        
        print(f"📊 Performance Summary:")
        print(f"   Duration: {duration:.3f}s")
        print(f"   Memory change: {memory_delta:+.1f}MB")
        print(f"   Current memory: {end_memory:.1f}MB")

@ray.remote
def cpu_intensive_task(n: int) -> float:
    """CPU-intensive task for performance testing."""
    result = 0.0
    for i in range(n):
        result += i ** 0.5
    return result

@ray.remote
def io_simulation_task(delay: float) -> str:
    """Simulate I/O bound task."""
    time.sleep(delay)
    return f"IO completed after {delay}s"

def demonstrate_performance_debugging():
    """
    Performance debugging techniques.
    GOLD: "Your Ray app is slow - profile and optimize it!"
    """
    print("\n=== PERFORMANCE DEBUGGING ===")
    
    # 1. CPU-bound vs I/O-bound analysis
    print("1. Task type analysis:")
    
    with ray_performance_monitor():
        # CPU-intensive tasks
        cpu_futures = [cpu_intensive_task.remote(1000000) for _ in range(4)]
        cpu_results = ray.get(cpu_futures)
        print(f"CPU tasks completed: {len(cpu_results)} results")
    
    with ray_performance_monitor():
        # I/O-bound tasks
        io_futures = [io_simulation_task.remote(0.5) for _ in range(4)]
        io_results = ray.get(io_futures)
        print(f"I/O tasks completed: {len(io_results)} results")
    
    # 2. Task scheduling analysis
    print("\n2. Task scheduling analysis:")
    
    # Bad pattern: Sequential ray.get() calls
    print("Bad pattern - Sequential execution:")
    with ray_performance_monitor():
        sequential_results = []
        for i in range(4):
            future = io_simulation_task.remote(0.2)
            result = ray.get(future)  # Bad: immediate get()
            sequential_results.append(result)
    
    # Good pattern: Parallel execution
    print("Good pattern - Parallel execution:")
    with ray_performance_monitor():
        parallel_futures = [io_simulation_task.remote(0.2) for _ in range(4)]
        parallel_results = ray.get(parallel_futures)  # Good: batch get()
    
    # 3. Resource utilization debugging
    print("\n3. Resource utilization debugging:")
    
    # Check available resources
    available_resources = ray.available_resources()
    cluster_resources = ray.cluster_resources()
    
    print(f"Available resources: {available_resources}")
    print(f"Cluster resources: {cluster_resources}")
    
    # Create resource-aware tasks
    @ray.remote(num_cpus=0.5)
    def light_task(x):
        return x * 2
    
    @ray.remote(num_cpus=2.0)
    def heavy_task(x):
        return sum(i ** 2 for i in range(x))
    
    # Monitor resource usage
    print("Launching resource-aware tasks...")
    light_futures = [light_task.remote(i) for i in range(8)]
    heavy_futures = [heavy_task.remote(1000) for _ in range(2)]
    
    # Check resource usage while tasks run
    print("Resources while tasks running:")
    available_while_running = ray.available_resources()
    print(f"Available during execution: {available_while_running}")
    
    # Wait for completion
    light_results = ray.get(light_futures)
    heavy_results = ray.get(heavy_futures)
    
    print(f"Light tasks: {len(light_results)}, Heavy tasks: {len(heavy_results)}")

# =============================================================================
# 5. DISTRIBUTED DEBUGGING TECHNIQUES
# =============================================================================

def demonstrate_distributed_debugging():
    """
    Debugging distributed Ray applications.
    SCENARIO: "Your distributed app has issues - debug across nodes!"
    """
    print("\n=== DISTRIBUTED DEBUGGING ===")
    
    # 1. Node and worker information
    print("1. Cluster and worker information:")
    
    try:
        # Get cluster information
        nodes = ray.nodes()
        print(f"Number of nodes: {len(nodes)}")
        
        for i, node in enumerate(nodes):
            print(f"Node {i}: {node['NodeManagerAddress']}:{node['NodeManagerPort']}")
            print(f"  Resources: {node['Resources']}")
            print(f"  Alive: {node['Alive']}")
    
    except Exception as e:
        print(f"Node info error: {e}")
    
    # 2. Task placement debugging
    print("\n2. Task placement and worker debugging:")
    
    @ray.remote
    def worker_info_task() -> Dict[str, Any]:
        """Get information about the worker executing this task."""
        import os
        import socket
        
        return {
            "hostname": socket.gethostname(),
            "pid": os.getpid(),
            "worker_id": ray.get_runtime_context().worker_id.hex(),
            "node_id": ray.get_runtime_context().node_id.hex(),
        }
    
    # Launch tasks and see where they run
    worker_futures = [worker_info_task.remote() for _ in range(5)]
    worker_results = ray.get(worker_futures)
    
    for i, result in enumerate(worker_results):
        print(f"Task {i}: Worker {result['worker_id'][:8]}... on {result['hostname']}")
    
    # 3. Error propagation debugging
    print("\n3. Error propagation debugging:")
    
    @ray.remote
    def nested_error_task(should_fail: bool = False) -> str:
        """Task that may fail with nested error information."""
        if should_fail:
            try:
                # Simulate nested operation that fails
                raise ValueError("Deep nested error occurred")
            except ValueError as e:
                # Re-raise with more context
                raise RuntimeError(f"Task failed in nested operation: {e}") from e
        
        return "Task completed successfully"
    
    # Test error propagation
    error_futures = [
        nested_error_task.remote(i % 2 == 0)  # Every other task fails
        for i in range(4)
    ]
    
    for i, future in enumerate(error_futures):
        try:
            result = ray.get(future)
            print(f"✅ Task {i}: {result}")
        except Exception as e:
            print(f"❌ Task {i}: {type(e).__name__}: {e}")
            # Print full traceback for debugging
            print(f"   Traceback: {traceback.format_exc().split('\\n')[-3]}")

# =============================================================================
# 6. DEBUGGING BEST PRACTICES AND TOOLS
# =============================================================================

def demonstrate_debugging_best_practices():
    """
    Best practices for debugging Ray applications.
    ESSENTIAL: Know these techniques!
    """
    print("\n=== DEBUGGING BEST PRACTICES ===")
    
    print("1. Ray Dashboard and Monitoring:")
    print("✅ Use Ray Dashboard (default: http://localhost:8265)")
    print("✅ Monitor task timeline and resource usage")
    print("✅ Check object store usage and spillage")
    print("✅ View actor lifecycle and method calls")
    
    print("\n2. Logging and Instrumentation:")
    print("✅ Add structured logging to tasks and actors")
    print("✅ Use ray.get_runtime_context() for worker info")
    print("✅ Monitor task duration and resource usage")
    print("✅ Log errors with full context")
    
    print("\n3. Common Debugging Patterns:")
    
    # Pattern 1: Timeout detection
    @ray.remote
    def potentially_hanging_task(delay: float) -> str:
        time.sleep(delay)
        return f"Completed after {delay}s"
    
    print("Pattern 1 - Timeout detection:")
    future = potentially_hanging_task.remote(0.5)
    ready, not_ready = ray.wait([future], timeout=1.0)
    
    if ready:
        result = ray.get(ready[0])
        print(f"✅ Task completed: {result}")
    else:
        print("⚠️  Task is taking longer than expected")
    
    # Pattern 2: Graceful error handling
    print("\nPattern 2 - Graceful error handling:")
    
    def safe_ray_get(futures, timeout=10.0):
        """Safely get results with error handling."""
        results = []
        errors = []
        
        for i, future in enumerate(futures):
            try:
                ready, _ = ray.wait([future], timeout=timeout)
                if ready:
                    result = ray.get(ready[0])
                    results.append({"index": i, "result": result, "status": "success"})
                else:
                    errors.append({"index": i, "error": "timeout", "status": "timeout"})
            except Exception as e:
                errors.append({"index": i, "error": str(e), "status": "error"})
        
        return results, errors
    
    # Test safe get
    test_futures = [potentially_hanging_task.remote(0.1) for _ in range(3)]
    safe_results, safe_errors = safe_ray_get(test_futures)
    
    print(f"Safe results: {len(safe_results)} success, {len(safe_errors)} errors")
    
    print("\n4. Performance Debugging Checklist:")
    print("✅ Profile CPU vs I/O bound tasks differently")
    print("✅ Monitor memory usage and object store")
    print("✅ Check resource utilization and scheduling")
    print("✅ Use appropriate parallelism for workload")
    print("✅ Avoid sequential ray.get() calls")
    print("✅ Monitor network usage in distributed setup")

# =============================================================================
# 7. MAIN DEMONSTRATION FUNCTION
# =============================================================================

def main():
    """
    Run all debugging demonstrations.
    PREP: Master these debugging techniques!
    """
    try:
        print("RAY CORE DEBUGGING & TROUBLESHOOTING DEMONSTRATION")
        print("=================================================")
        
        if not ray.is_initialized():
            ray.init(num_cpus=4, ignore_reinit_error=True)
        
        # Core debugging techniques
        demonstrate_initialization_issues()
        demonstrate_memory_debugging()
        demonstrate_task_actor_debugging()
        demonstrate_performance_debugging()
        demonstrate_distributed_debugging()
        demonstrate_debugging_best_practices()
        
        print("\n=== SUCCESS: All debugging techniques demonstrated! ===")
        
    except Exception as e:
        print(f"Error in main: {e}")
        traceback.print_exc()
    finally:
        if ray.is_initialized():
            ray.shutdown()

if __name__ == "__main__":
    main()

# =============================================================================
# DEBUGGING MASTERY CHECKLIST
# =============================================================================
"""
RAY DEBUGGING - MASTERY:

✅ INITIALIZATION ISSUES:
   - Check ray.is_initialized() before init
   - Use ignore_reinit_error=True for reinit
   - Verify resource specifications
   - Monitor Ray cluster connectivity

✅ MEMORY AND OBJECT STORE:
   - Monitor object store usage and spillage
   - Use ray.put() for large shared data
   - Explicitly delete unnecessary ObjectRefs
   - Watch for memory leaks in actors

✅ TASK AND ACTOR DEBUGGING:
   - Handle task failures gracefully
   - Use timeouts to detect hanging tasks
   - Monitor actor state and lifecycle
   - Log errors with full context

✅ PERFORMANCE DEBUGGING:
   - Profile CPU vs I/O bound workloads
   - Monitor resource utilization
   - Avoid sequential ray.get() calls
   - Use Ray Dashboard for visualization

✅ DISTRIBUTED DEBUGGING:
   - Check cluster node status
   - Monitor task placement across workers
   - Debug error propagation in distributed setup
   - Use structured logging for traceability

✅ DEBUGGING TOOLS:
   - Ray Dashboard (localhost:8265)
   - ray.available_resources() and ray.cluster_resources()
   - ray.get_runtime_context() for worker info
   - ray.wait() for timeout handling
   - Performance monitoring with psutil

COMMON QUESTIONS:
1. "Ray app is slow - how do you debug?" -> Profile, check dashboard, monitor resources
2. "Tasks are hanging - what do you do?" -> Use ray.wait() with timeout, check resources
3. "Out of memory errors - how to fix?" -> Monitor object store, use ray.put(), cleanup refs
4. "Tasks failing inconsistently - debug approach?" -> Error handling, logging, monitoring
5. "How do you debug distributed Ray apps?" -> Node monitoring, worker info, error propagation

""" 