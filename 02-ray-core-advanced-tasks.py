#!/usr/bin/env python3
"""
RAY CORE ADVANCED TASKS - File 02
=================================

This file covers advanced task patterns essential for Ray Core:
1. Task dependencies and chaining
2. Dynamic task creation
3. Error handling and fault tolerance
4. Performance optimization patterns
5. Resource management
6. Common troubleshooting scenarios

FOCUS AREAS:
- Task graphs and dependency management
- Handling failures gracefully
- Optimizing task performance
- Debugging common issues
"""

import ray
import time
import random
import numpy as np
from typing import List, Dict, Any, Optional
import traceback
import sys

# =============================================================================
# 1. TASK DEPENDENCIES AND CHAINING - CRITICAL TOPIC
# =============================================================================

@ray.remote
def fetch_data(source_id: int) -> List[int]:
    """
    Simulate fetching data from a source.
    SCENARIO: "How do you chain dependent tasks?"
    """
    print(f"Fetching data from source {source_id}")
    time.sleep(0.2)  # Simulate network delay
    data = [random.randint(1, 100) for _ in range(10)]
    print(f"Source {source_id} returned {len(data)} items")
    return data

@ray.remote
def process_data(data: List[int], operation: str) -> List[int]:
    """
    Process data with specified operation.
    Demonstrates task chaining - this task depends on fetch_data output.
    """
    print(f"Processing data with operation: {operation}")
    time.sleep(0.1)
    
    if operation == "double":
        result = [x * 2 for x in data]
    elif operation == "square":
        result = [x ** 2 for x in data]
    elif operation == "filter_even":
        result = [x for x in data if x % 2 == 0]
    else:
        result = data
    
    print(f"Operation {operation} produced {len(result)} items")
    return result

@ray.remote
def aggregate_results(results: List[List[int]]) -> Dict[str, float]:
    """
    Aggregate multiple processed results.
    Final step in task chain - depends on all process_data tasks.
    """
    print("Aggregating all results")
    all_values = [val for sublist in results for val in sublist]
    
    if not all_values:
        return {"count": 0, "mean": 0, "sum": 0}
    
    aggregated = {
        "count": len(all_values),
        "sum": sum(all_values),
        "mean": sum(all_values) / len(all_values),
        "min": min(all_values),
        "max": max(all_values)
    }
    print(f"Aggregated {aggregated['count']} values")
    return aggregated

def demonstrate_task_chaining():
    """
    GOLD: Demonstrates building complex task graphs.
    
    Pattern: fetch -> process -> aggregate
    Shows how ObjectRefs flow through task chains without intermediate ray.get() calls.
    """
    print("\n=== TASK CHAINING DEMONSTRATION ===")
    
    # Step 1: Launch data fetching tasks (parallel)
    num_sources = 3
    fetch_futures = [fetch_data.remote(i) for i in range(num_sources)]
    print(f"Launched {num_sources} fetch tasks")
    
    # Step 2: Launch processing tasks (depend on fetch tasks)
    # NOTE: Passing ObjectRefs directly - no ray.get() needed!
    operations = ["double", "square", "filter_even"]
    process_futures = []
    
    for i, fetch_future in enumerate(fetch_futures):
        operation = operations[i % len(operations)]
        # Pass ObjectRef directly to next task
        process_future = process_data.remote(fetch_future, operation)
        process_futures.append(process_future)
    
    print(f"Launched {len(process_futures)} processing tasks")
    
    # Step 3: Launch aggregation task (depends on all processing tasks)
    aggregate_future = aggregate_results.remote(process_futures)
    
    # Step 4: Get final result
    final_result = ray.get(aggregate_future)
    print(f"Final aggregated result: {final_result}")
    
    return final_result

# =============================================================================
# 2. DYNAMIC TASK CREATION - ADVANCED PATTERN
# =============================================================================

@ray.remote
def fibonacci_task(n: int) -> int:
    """
    Recursive Fibonacci using Ray tasks.
    CHALLENGE: "Implement recursive algorithms with Ray"
    
    WARNING: This creates many tasks - good for demonstrating concepts,
    but not efficient for actual Fibonacci computation!
    """
    if n <= 1:
        return n
    
    # Create two dependent tasks dynamically
    left_future = fibonacci_task.remote(n - 1)
    right_future = fibonacci_task.remote(n - 2)
    
    # Wait for both results
    left_val, right_val = ray.get([left_future, right_future])
    return left_val + right_val

@ray.remote
def map_reduce_task(data_chunk: List[int], operation: str) -> int:
    """
    Map phase of a map-reduce pattern.
    """
    if operation == "sum":
        return sum(data_chunk)
    elif operation == "count":
        return len(data_chunk)
    elif operation == "max":
        return max(data_chunk) if data_chunk else 0
    else:
        return 0

def demonstrate_dynamic_tasks():
    """
    Shows dynamic task creation patterns.
    TOPIC: "How do you handle variable numbers of tasks?"
    """
    print("\n=== DYNAMIC TASK CREATION ===")
    
    # Example 1: Fibonacci (creates task tree dynamically)
    print("1. Dynamic recursive tasks (Fibonacci):")
    fib_future = fibonacci_task.remote(10)
    fib_result = ray.get(fib_future)
    print(f"Fibonacci(10) = {fib_result}")
    
    # Example 2: Map-Reduce pattern
    print("\n2. Dynamic map-reduce pattern:")
    large_dataset = list(range(1000))
    chunk_size = 100
    
    # Dynamically create chunks and map tasks
    map_futures = []
    for i in range(0, len(large_dataset), chunk_size):
        chunk = large_dataset[i:i + chunk_size]
        future = map_reduce_task.remote(chunk, "sum")
        map_futures.append(future)
    
    print(f"Created {len(map_futures)} map tasks")
    
    # Reduce phase
    partial_sums = ray.get(map_futures)
    total_sum = sum(partial_sums)
    print(f"Map-reduce result: {total_sum}")
    print(f"Verification: {sum(large_dataset)}")

# =============================================================================
# 3. ERROR HANDLING AND FAULT TOLERANCE - CRITICAL
# =============================================================================

@ray.remote
def unreliable_task(task_id: int, failure_rate: float = 0.3) -> str:
    """
    Task that randomly fails to demonstrate error handling.
    SCENARIO: "How do you handle task failures?"
    """
    print(f"Starting unreliable task {task_id}")
    
    if random.random() < failure_rate:
        raise ValueError(f"Task {task_id} failed randomly!")
    
    time.sleep(0.1)
    return f"Task {task_id} completed successfully"

@ray.remote
def robust_task(data: List[int]) -> Dict[str, Any]:
    """
    Task with internal error handling.
    Shows defensive programming practices.
    """
    try:
        if not data:
            return {"status": "error", "message": "Empty data provided"}
        
        # Simulate potential division by zero
        if len(data) == 1 and data[0] == 0:
            raise ZeroDivisionError("Cannot process data with single zero")
        
        result = {
            "status": "success",
            "mean": sum(data) / len(data),
            "count": len(data),
            "processed_at": time.time()
        }
        return result
        
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "error_type": type(e).__name__
        }

def demonstrate_error_handling():
    """
    CRITICAL: Shows how to handle failures in distributed systems.
    
    Key patterns:
    1. Try-catch around ray.get()
    2. Internal error handling in tasks
    3. Retry strategies
    4. Graceful degradation
    """
    print("\n=== ERROR HANDLING DEMONSTRATION ===")
    
    # Pattern 1: Basic error catching
    print("1. Handling task failures:")
    futures = [unreliable_task.remote(i, 0.5) for i in range(5)]
    
    results = []
    for i, future in enumerate(futures):
        try:
            result = ray.get(future)
            results.append(result)
            print(f"✅ {result}")
        except Exception as e:
            error_msg = f"❌ Task {i} failed: {e}"
            results.append(error_msg)
            print(error_msg)
    
    # Pattern 2: Robust tasks with internal error handling
    print("\n2. Internal error handling:")
    test_data = [
        [1, 2, 3, 4, 5],  # Valid data
        [],               # Empty data
        [0],              # Problematic data
        [10, 20, 30]      # Valid data
    ]
    
    robust_futures = [robust_task.remote(data) for data in test_data]
    robust_results = ray.get(robust_futures)
    
    for i, result in enumerate(robust_results):
        status = result["status"]
        if status == "success":
            print(f"✅ Data {i}: Mean = {result['mean']:.2f}")
        else:
            print(f"❌ Data {i}: {result['message']}")

# =============================================================================
# 4. PERFORMANCE OPTIMIZATION PATTERNS - FOCUS
# =============================================================================

@ray.remote(num_cpus=0.5)  # Use half a CPU core
def cpu_light_task(x: int) -> int:
    """
    Task that doesn't need a full CPU core.
    TIP: Resource specification can improve efficiency!
    """
    return x * 2

@ray.remote(num_cpus=2)  # Use 2 CPU cores
def cpu_intensive_task(data: List[int]) -> List[int]:
    """
    CPU-intensive task that benefits from multiple cores.
    """
    # Simulate CPU-intensive work
    result = []
    for x in data:
        # Expensive computation
        val = sum(i * i for i in range(x % 100))
        result.append(val)
    return result

@ray.remote
def memory_efficient_task(start: int, end: int) -> int:
    """
    Process data range without loading everything into memory.
    TIP: Process data in chunks to avoid memory issues!
    """
    total = 0
    for i in range(start, end):
        total += i * i
    return total

def demonstrate_performance_patterns():
    """
    Shows performance optimization techniques.
    GOLD: "How do you optimize Ray applications?"
    """
    print("\n=== PERFORMANCE OPTIMIZATION ===")
    
    # Pattern 1: Resource specification
    print("1. Resource-aware task scheduling:")
    light_futures = [cpu_light_task.remote(i) for i in range(8)]
    heavy_futures = [cpu_intensive_task.remote([i] * 10) for i in range(2)]
    
    # Both types can run efficiently with proper resource allocation
    light_results = ray.get(light_futures)
    heavy_results = ray.get(heavy_futures)
    print(f"Light tasks: {light_results}")
    print(f"Heavy tasks completed: {len(heavy_results)} results")
    
    # Pattern 2: Memory-efficient processing
    print("\n2. Memory-efficient chunk processing:")
    total_range = 1000000
    chunk_size = 100000
    
    chunk_futures = []
    for start in range(0, total_range, chunk_size):
        end = min(start + chunk_size, total_range)
        future = memory_efficient_task.remote(start, end)
        chunk_futures.append(future)
    
    chunk_results = ray.get(chunk_futures)
    final_sum = sum(chunk_results)
    print(f"Processed {total_range} items in {len(chunk_futures)} chunks")
    print(f"Result: {final_sum}")

# =============================================================================
# 5. TROUBLESHOOTING SCENARIOS - PREPARATION
# =============================================================================

@ray.remote
def memory_hog_task(size_mb: int) -> str:
    """
    Task that uses specified amount of memory.
    Used to demonstrate memory-related issues.
    """
    # Allocate memory (size_mb megabytes)
    data = np.random.rand(size_mb * 1024 * 1024 // 8)  # 8 bytes per float64
    return f"Allocated {size_mb}MB of memory"

@ray.remote
def slow_task_debug(task_id: int, delay: float) -> Dict[str, Any]:
    """
    Task for debugging performance issues.
    Returns timing information for analysis.
    """
    start_time = time.time()
    print(f"Task {task_id} starting at {start_time}")
    
    # Simulate work
    time.sleep(delay)
    
    end_time = time.time()
    return {
        "task_id": task_id,
        "start_time": start_time,
        "end_time": end_time,
        "duration": end_time - start_time,
        "expected_delay": delay
    }

def demonstrate_troubleshooting():
    """
    Common troubleshooting scenarios you might face.
    PREP: Know how to diagnose and fix these issues!
    """
    print("\n=== TROUBLESHOOTING SCENARIOS ===")
    
    # Scenario 1: Performance bottlenecks
    print("1. Diagnosing performance bottlenecks:")
    delays = [0.1, 0.2, 0.5, 0.1, 0.3]  # One slow task
    debug_futures = [slow_task_debug.remote(i, delay) for i, delay in enumerate(delays)]
    debug_results = ray.get(debug_futures)
    
    # Analyze results
    for result in debug_results:
        expected = result["expected_delay"]
        actual = result["duration"]
        overhead = actual - expected
        print(f"Task {result['task_id']}: Expected {expected:.2f}s, "
              f"Actual {actual:.2f}s, Overhead {overhead:.2f}s")
    
    # Scenario 2: Resource monitoring
    print("\n2. Resource monitoring:")
    print(f"Available resources: {ray.available_resources()}")
    print(f"Ray cluster info: {ray.cluster_resources()}")

# =============================================================================
# 6. REAL-WORLD PATTERNS - SCENARIOS
# =============================================================================

@ray.remote
def web_scraper_task(url_id: int) -> Dict[str, Any]:
    """
    Simulate web scraping task.
    SCENARIO: "Build a distributed web scraper"
    """
    delay = random.uniform(0.1, 0.5)  # Simulate variable response times
    time.sleep(delay)
    
    # Simulate different outcomes
    if random.random() < 0.1:  # 10% failure rate
        raise ConnectionError(f"Failed to connect to URL {url_id}")
    
    return {
        "url_id": url_id,
        "status": "success",
        "response_time": delay,
        "data_size": random.randint(1000, 10000),
        "scraped_at": time.time()
    }

@ray.remote
def batch_processor_task(batch_id: int, items: List[Any]) -> Dict[str, Any]:
    """
    Process a batch of items.
    SCENARIO: "Process large datasets in batches"
    """
    start_time = time.time()
    
    # Simulate processing
    processed_count = 0
    for item in items:
        # Simulate item processing
        time.sleep(0.01)
        processed_count += 1
    
    return {
        "batch_id": batch_id,
        "items_processed": processed_count,
        "processing_time": time.time() - start_time,
        "avg_time_per_item": (time.time() - start_time) / len(items)
    }

def demonstrate_real_world_patterns():
    """
    Real-world common patterns 
    """
    print("\n=== REAL-WORLD PATTERNS ===")
    
    # Pattern 1: Web scraping with error handling
    print("1. Distributed web scraping:")
    url_count = 10
    scraper_futures = [web_scraper_task.remote(i) for i in range(url_count)]
    
    successful_scrapes = []
    failed_scrapes = []
    
    for future in scraper_futures:
        try:
            result = ray.get(future)
            successful_scrapes.append(result)
        except Exception as e:
            failed_scrapes.append(str(e))
    
    print(f"Successful scrapes: {len(successful_scrapes)}")
    print(f"Failed scrapes: {len(failed_scrapes)}")
    
    # Pattern 2: Batch processing
    print("\n2. Batch processing:")
    large_dataset = list(range(1000))
    batch_size = 100
    
    batch_futures = []
    for i in range(0, len(large_dataset), batch_size):
        batch = large_dataset[i:i + batch_size]
        batch_id = i // batch_size
        future = batch_processor_task.remote(batch_id, batch)
        batch_futures.append(future)
    
    batch_results = ray.get(batch_futures)
    total_items = sum(r["items_processed"] for r in batch_results)
    total_time = sum(r["processing_time"] for r in batch_results)
    
    print(f"Processed {total_items} items in {len(batch_results)} batches")
    print(f"Total processing time: {total_time:.2f}s")

# =============================================================================
# 7. MAIN DEMONSTRATION FUNCTION
# =============================================================================

def main():
    """
    Run all advanced task demonstrations.
    PREP: Practice explaining each pattern!
    """
    try:
        print("RAY CORE ADVANCED TASKS DEMONSTRATION")
        print("====================================")
        
        if not ray.is_initialized():
            ray.init(num_cpus=4, ignore_reinit_error=True)
        
        # Core advanced patterns
        demonstrate_task_chaining()
        demonstrate_dynamic_tasks()
        demonstrate_error_handling()
        demonstrate_performance_patterns()
        demonstrate_troubleshooting()
        demonstrate_real_world_patterns()
        
        print("\n=== SUCCESS: All advanced task patterns demonstrated! ===")
        
    except Exception as e:
        print(f"Error in main: {e}")
        traceback.print_exc()
    finally:
        if ray.is_initialized():
            ray.shutdown()

if __name__ == "__main__":
    main()

# =============================================================================
# PREPARATION CHECKLIST
# =============================================================================
"""
ADVANCED TASKS - CHECKLIST:

✅ TASK DEPENDENCIES:
   - Pass ObjectRefs between tasks (no intermediate ray.get())
   - Build task graphs: fetch -> process -> aggregate
   - Understand task scheduling and execution order

✅ DYNAMIC TASK CREATION:
   - Create tasks based on runtime conditions
   - Recursive task patterns (like Fibonacci)
   - Map-reduce patterns with variable task counts

✅ ERROR HANDLING:
   - Try-catch around ray.get() calls
   - Internal error handling in tasks
   - Graceful degradation strategies
   - Task retry patterns

✅ PERFORMANCE OPTIMIZATION:
   - Resource specification (@ray.remote(num_cpus=2))
   - Memory-efficient chunk processing
   - Avoid unnecessary ray.get() calls
   - Monitor resource usage

✅ TROUBLESHOOTING:
   - Diagnose performance bottlenecks
   - Monitor resource utilization
   - Debug task failures
   - Analyze task timing and overhead

✅ REAL-WORLD PATTERNS:
   - Web scraping with fault tolerance
   - Batch processing large datasets
   - Data pipeline construction
   - Parallel algorithm implementation

COMMON QUESTIONS:
1. "How do you chain tasks together?" -> Pass ObjectRefs directly
2. "What if a task fails?" -> Error handling with try-catch
3. "How do you optimize performance?" -> Resource specs and chunking
4. "How do you debug slow tasks?" -> Timing analysis and monitoring

NEXT: Practice with 03-ray-core-actors.py for stateful computing!
""" 