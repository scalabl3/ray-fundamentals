#!/usr/bin/env python3
"""
RAY CORE FUNDAMENTALS - File 01
===============================

This file covers the absolute basics of Ray Core that you MUST:
1. Ray initialization and shutdown
2. @ray.remote decorator basics
3. ray.get() and ray.put() 
4. Understanding futures/ObjectRefs
5. Basic parallel execution patterns

KEY CONCEPTS:
- Ray transforms regular Python functions into distributed tasks
- Functions return ObjectRefs (futures) not actual values
- ray.get() blocks and retrieves actual values
- Ray automatically handles serialization and distribution
"""

import ray
import time
import random
from typing import List

# =============================================================================
# 1. RAY INITIALIZATION - CRITICAL FIRST STEP
# =============================================================================

def initialize_ray():
    """
    Ray must be initialized before using any Ray functionality.
    
    Common initialization patterns:
    - ray.init() - Auto-detect resources, local mode
    - ray.init(num_cpus=4) - Specify CPU count
    - ray.init(address="ray://head-node:10001") - Connect to cluster
    
    TIP: Always check if Ray is already initialized!
    """
    if not ray.is_initialized():
        # Initialize Ray with specific CPU count for consistency
        ray.init(num_cpus=4, ignore_reinit_error=True)
        print(f"Ray initialized with {ray.available_resources()}")
    else:
        print("Ray already initialized")

def shutdown_ray():
    """
    Always clean up Ray resources when done.
    TIP: Missing ray.shutdown() can cause resource leaks!
    """
    if ray.is_initialized():
        ray.shutdown()
        print("Ray shutdown completed")

# =============================================================================
# 2. BASIC REMOTE FUNCTIONS - THE FOUNDATION OF RAY
# =============================================================================

@ray.remote
def simple_task(x: int) -> int:
    """
    A basic Ray remote function.
    
    KEY POINTS:
    - @ray.remote decorator makes this function distributable
    - When called, returns ObjectRef (future) not the actual result
    - Function executes on a Ray worker process (possibly different machine)
    - Arguments and return values are automatically serialized
    """
    # Simulate some work
    time.sleep(0.1)
    result = x * x
    print(f"Task processing {x} -> {result} on worker")
    return result

@ray.remote
def slow_task(x: int, delay: float = 1.0) -> str:
    """
    A slower task to demonstrate parallel execution benefits.
    
    TIP: SCENARIO: "How do you make this faster?"
    ANSWER: Run multiple instances in parallel using Ray!
    """
    print(f"Starting slow task with input {x}")
    time.sleep(delay)
    result = f"Processed {x} after {delay}s delay"
    print(f"Completed slow task: {result}")
    return result

# =============================================================================
# 3. UNDERSTANDING OBJECTREFS (FUTURES) - CRITICAL CONCEPT
# =============================================================================

def demonstrate_objectrefs():
    """
    ObjectRefs are Ray's futures - they represent values that will be available later.
    
    CRITICAL TIP: CONCEPT:
    - Remote function calls return ObjectRefs immediately (non-blocking)
    - Use ray.get() to retrieve actual values (blocking)
    - ObjectRefs can be passed to other remote functions without ray.get()
    """
    print("\n=== OBJECTREF DEMONSTRATION ===")
    
    # 1. Call remote function - returns immediately with ObjectRef
    future_result = simple_task.remote(5)
    print(f"Got ObjectRef immediately: {future_result}")
    print(f"ObjectRef type: {type(future_result)}")
    
    # 2. Continue with other work while task runs in background
    print("Doing other work while task runs...")
    time.sleep(0.05)
    
    # 3. Get actual result when needed (blocks until complete)
    actual_result = ray.get(future_result)
    print(f"Actual result: {actual_result}")
    print(f"Result type: {type(actual_result)}")

# =============================================================================
# 4. PARALLEL EXECUTION PATTERNS 
# =============================================================================

def sequential_vs_parallel():
    """
    Classic question: "Show me the difference between sequential and parallel execution"
    
    This demonstrates the core value proposition of Ray!
    """
    print("\n=== SEQUENTIAL VS PARALLEL EXECUTION ===")
    
    # Sequential execution (slow)
    print("Sequential execution:")
    start_time = time.time()
    sequential_results = []
    for i in range(4):
        # Note: Using .remote() but immediately calling ray.get() = sequential
        result = ray.get(slow_task.remote(i, 0.5))
        sequential_results.append(result)
    sequential_time = time.time() - start_time
    print(f"Sequential time: {sequential_time:.2f}s")
    
    # Parallel execution (fast!)
    print("\nParallel execution:")
    start_time = time.time()
    # Step 1: Launch all tasks (non-blocking)
    futures = [slow_task.remote(i, 0.5) for i in range(4)]
    # Step 2: Wait for all results (blocking)
    parallel_results = ray.get(futures)
    parallel_time = time.time() - start_time
    print(f"Parallel time: {parallel_time:.2f}s")
    print(f"Speedup: {sequential_time/parallel_time:.2f}x")
    
    return sequential_results, parallel_results

# =============================================================================
# 5. HANDLING MULTIPLE RESULTS - COMMON PATTERNS
# =============================================================================

def multiple_results_patterns():
    """
    Different ways to handle multiple ObjectRefs.
    TIP: Know when to use each pattern!
    """
    print("\n=== MULTIPLE RESULTS PATTERNS ===")
    
    # Pattern 1: Wait for ALL results
    print("1. Wait for ALL results:")
    futures = [simple_task.remote(i) for i in range(3)]
    all_results = ray.get(futures)  # Blocks until ALL complete
    print(f"All results: {all_results}")
    
    # Pattern 2: Process results as they complete
    print("2. Process results as available:")
    futures = [slow_task.remote(i, random.uniform(0.1, 0.3)) for i in range(3)]
    
    # Get results one by one as they complete
    while futures:
        # ray.wait returns (ready, not_ready) ObjectRef lists
        ready, futures = ray.wait(futures, num_returns=1)
        result = ray.get(ready[0])
        print(f"Got result: {result}")

# =============================================================================
# 6. RAY.PUT() - EFFICIENT DATA SHARING
# =============================================================================

@ray.remote
def process_data(data_or_ref, multiplier: int):
    """
    Process shared data efficiently using ObjectRefs.
    
    CONCEPT: Large data should be put in object store once,
    then referenced by ObjectRef in multiple tasks.
    """
    # Handle both ObjectRef and direct data for demonstration
    if hasattr(data_or_ref, '__ray_object_ref__') or str(type(data_or_ref)).find('ObjectRef') != -1:
        # data_or_ref is an ObjectRef - get the actual data
        data = ray.get(data_or_ref)
    else:
        # data_or_ref is direct data
        data = data_or_ref
    
    return [x * multiplier for x in data]

def demonstrate_ray_put():
    """
    ray.put() stores data in Ray's object store for efficient sharing.
    
    SCENARIO: "How do you efficiently pass large data to multiple tasks?"
    ANSWER: Use ray.put() to avoid copying data multiple times!
    """
    print("\n=== RAY.PUT() DEMONSTRATION ===")
    
    # Large data that multiple tasks need
    large_data = list(range(1000))
    
    # Method 1: Inefficient - data copied to each task
    print("Inefficient method (data copied multiple times):")
    try:
        futures_bad = [process_data.remote(large_data, i) for i in range(3)]
        bad_results = ray.get(futures_bad)
        print(f"Inefficient results: {len(bad_results)} tasks completed")
    except Exception as e:
        print(f"Error with inefficient method: {e}")
    
    # Method 2: Efficient - data stored once in object store
    print("Efficient method (data stored once):")
    data_ref = ray.put(large_data)  # Store in object store
    futures_good = [process_data.remote(data_ref, i) for i in range(3)]
    
    # Both approaches work, but ray.put() is more efficient for large data
    results = ray.get(futures_good)
    print(f"First few results: {[r[:5] for r in results]}")

# =============================================================================
# 7. COMMON MISTAKES AND DEBUGGING - PREPARATION
# =============================================================================

def common_mistakes():
    """
    Common Ray Mistakes    
    """
    print("\n=== COMMON MISTAKES TO AVOID ===")
    
    # Mistake 1: Calling ray.get() too early (kills parallelism)
    print("❌ MISTAKE: Calling ray.get() immediately")
    print("Don't do: result = ray.get(simple_task.remote(5))")
    print("Better: future = simple_task.remote(5); # do other work; result = ray.get(future)")
    
    # Mistake 2: Not handling ObjectRefs properly
    print("\n❌ MISTAKE: Treating ObjectRefs like regular values")
    future = simple_task.remote(10)
    # This won't work: print(f"Result: {future + 5}")
    print("Don't do: future + 5 (ObjectRef + int)")
    print("Better: ray.get(future) + 5")
    
    # Mistake 3: Forgetting to initialize Ray
    print("\n❌ MISTAKE: Using Ray without initialization")
    print("Always call ray.init() before using Ray functions!")
    
    # Mistake 4: Not shutting down Ray
    print("\n❌ MISTAKE: Not cleaning up Ray resources")
    print("Always call ray.shutdown() when done!")

# =============================================================================
# 8. MAIN DEMONSTRATION FUNCTION
# =============================================================================

def main():
    """
    Main function demonstrating all fundamental Ray concepts.
    
    RUN THIS to see everything in action!
    """
    try:
        print("RAY CORE FUNDAMENTALS DEMONSTRATION")
        print("===================================")
        
        # Initialize Ray
        initialize_ray()
        
        # Core concepts
        demonstrate_objectrefs()
        sequential_vs_parallel()
        multiple_results_patterns()
        demonstrate_ray_put()
        common_mistakes()
        
        print("\n=== SUCCESS: All Ray fundamentals demonstrated! ===")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Always clean up
        shutdown_ray()

if __name__ == "__main__":
    main()
    future = simple_task.remote(5)
print(f"Type: {type(future)}")
print(f"Value: {future}")


"""
KEY TAKEAWAYS:

1. INITIALIZATION:
   - Always ray.init() before using Ray
   - Check ray.is_initialized() to avoid errors
   - Clean up with ray.shutdown()

2. REMOTE FUNCTIONS:
   - @ray.remote decorator creates distributed functions
   - Returns ObjectRefs (futures), not actual values
   - Functions execute on worker processes

3. OBJECTREFS:
   - Represent future values
   - Use ray.get() to retrieve actual values (blocking)
   - Can be passed to other remote functions without ray.get()

4. PARALLEL PATTERNS:
   - Launch multiple tasks: [func.remote(i) for i in range(n)]
   - Wait for results: ray.get(futures)
   - Process as available: ray.wait()

5. EFFICIENCY:
   - Use ray.put() for large data shared across tasks
   - Avoid early ray.get() calls (kills parallelism)
   - Batch operations when possible

6. DEBUGGING:
   - Watch for ObjectRef vs value confusion
   - Check Ray initialization
   - Monitor resource usage
   - Handle exceptions properly

""" 