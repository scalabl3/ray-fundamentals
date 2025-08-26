#!/usr/bin/env python3
"""
RAY CORE ACTORS - File 03
=========================

This file covers Ray Actors - essential stateful distributed computing:
1. Basic actor concepts and patterns
2. Stateful computation with actors
3. Actor-task interaction patterns
4. Actor lifecycle management
5. Performance patterns with actors
6. Common actor anti-patterns and debugging

FOCUS AREAS:
- Understanding when to use actors vs tasks
- Stateful distributed computing patterns
- Actor communication and coordination
- Performance implications of actors
- Debugging actor-based applications
"""

import ray
import time
import random
import threading
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
import numpy as np

# =============================================================================
# 1. BASIC ACTOR CONCEPTS - FOUNDATION
# =============================================================================

@ray.remote
class SimpleCounter:
    """
    Basic Ray actor demonstrating state management.
    
    KEY POINTS:
    - Actors maintain state between method calls
    - Each actor instance runs on a single worker process
    - Actor methods return ObjectRefs like remote functions
    - Use .remote() to call actor methods
    """
    
    def __init__(self, initial_value: int = 0):
        """
        Actor constructor - sets up initial state.
        TIP: Constructor is called once when actor is created!
        """
        self.value = initial_value
        self.call_count = 0
        print(f"SimpleCounter initialized with value: {initial_value}")
    
    def increment(self, amount: int = 1) -> int:
        """
        Increment counter by amount.
        CONCEPT: State is preserved between calls!
        """
        self.call_count += 1
        self.value += amount
        print(f"Call #{self.call_count}: Incremented by {amount}, new value: {self.value}")
        return self.value
    
    def get_value(self) -> int:
        """Get current counter value."""
        return self.value
    
    def reset(self) -> int:
        """Reset counter to zero."""
        old_value = self.value
        self.value = 0
        self.call_count = 0
        print(f"Counter reset from {old_value} to 0")
        return old_value

def demonstrate_basic_actors():
    """
    ESSENTIAL: Shows fundamental actor concepts.
    
    Key differences from tasks:
    - Actors maintain state
    - Same actor instance handles all method calls
    - Methods called with .remote() like tasks
    """
    print("\n=== BASIC ACTOR DEMONSTRATION ===")
    
    # Create actor instance
    counter = SimpleCounter.remote(initial_value=10)
    print("Created counter actor")
    
    # Call actor methods (returns ObjectRefs)
    increment_future = counter.increment.remote(5)
    result1 = ray.get(increment_future)
    print(f"First increment result: {result1}")
    
    # State is preserved between calls
    result2 = ray.get(counter.increment.remote(3))
    print(f"Second increment result: {result2}")
    
    # Get current state
    current_value = ray.get(counter.get_value.remote())
    print(f"Current counter value: {current_value}")
    
    # Reset state
    old_value = ray.get(counter.reset.remote())
    final_value = ray.get(counter.get_value.remote())
    print(f"After reset - old: {old_value}, new: {final_value}")

# =============================================================================
# 2. STATEFUL COMPUTATION PATTERNS - GOLD
# =============================================================================

@ray.remote
class DataProcessor:
    """
    Actor that accumulates and processes data over time.
    SCENARIO: "When would you use actors instead of tasks?"
    ANSWER: When you need to maintain state between operations!
    """
    
    def __init__(self, processor_id: str):
        self.processor_id = processor_id
        self.data_buffer = []
        self.processed_count = 0
        self.total_processing_time = 0
        print(f"DataProcessor {processor_id} initialized")
    
    def add_data(self, data: List[Any]) -> int:
        """
        Add data to internal buffer.
        Shows stateful accumulation pattern.
        """
        self.data_buffer.extend(data)
        print(f"Processor {self.processor_id}: Added {len(data)} items, "
              f"buffer size now: {len(self.data_buffer)}")
        return len(self.data_buffer)
    
    def process_batch(self, batch_size: int = 10) -> Dict[str, Any]:
        """
        Process a batch of data from internal buffer.
        Demonstrates stateful processing pattern.
        """
        if len(self.data_buffer) < batch_size:
            return {
                "status": "insufficient_data",
                "available": len(self.data_buffer),
                "requested": batch_size
            }
        
        # Process batch
        start_time = time.time()
        batch = self.data_buffer[:batch_size]
        self.data_buffer = self.data_buffer[batch_size:]
        
        # Simulate processing
        processed_batch = [x * 2 for x in batch if isinstance(x, (int, float))]
        processing_time = time.time() - start_time
        
        # Update state
        self.processed_count += len(processed_batch)
        self.total_processing_time += processing_time
        
        return {
            "status": "success",
            "processor_id": self.processor_id,
            "batch_size": len(processed_batch),
            "processed_data": processed_batch,
            "remaining_buffer": len(self.data_buffer),
            "total_processed": self.processed_count,
            "processing_time": processing_time
        }
    
    def get_stats(self) -> Dict[str, Any]:
        """Get processor statistics."""
        return {
            "processor_id": self.processor_id,
            "buffer_size": len(self.data_buffer),
            "total_processed": self.processed_count,
            "total_time": self.total_processing_time,
            "avg_time_per_item": (
                self.total_processing_time / self.processed_count 
                if self.processed_count > 0 else 0
            )
        }

@ray.remote
class WorkQueue:
    """
    Actor implementing a distributed work queue.
    PATTERN: Producer-consumer with persistent state.
    """
    
    def __init__(self, queue_name: str):
        self.queue_name = queue_name
        self.tasks = []
        self.completed_tasks = []
        self.workers = set()
        
    def add_task(self, task_data: Any) -> int:
        """Add task to queue."""
        task_id = len(self.tasks) + len(self.completed_tasks)
        task = {
            "id": task_id,
            "data": task_data,
            "created_at": time.time(),
            "status": "pending"
        }
        self.tasks.append(task)
        print(f"Queue {self.queue_name}: Added task {task_id}")
        return task_id
    
    def get_task(self, worker_id: str) -> Optional[Dict[str, Any]]:
        """Get next task for worker."""
        if not self.tasks:
            return None
        
        task = self.tasks.pop(0)
        task["status"] = "in_progress"
        task["worker_id"] = worker_id
        task["started_at"] = time.time()
        
        self.workers.add(worker_id)
        print(f"Queue {self.queue_name}: Assigned task {task['id']} to {worker_id}")
        return task
    
    def complete_task(self, task_id: int, result: Any) -> bool:
        """Mark task as completed."""
        # In real implementation, you'd track in-progress tasks
        completed_task = {
            "id": task_id,
            "result": result,
            "completed_at": time.time()
        }
        self.completed_tasks.append(completed_task)
        print(f"Queue {self.queue_name}: Task {task_id} completed")
        return True
    
    def get_queue_stats(self) -> Dict[str, Any]:
        """Get queue statistics."""
        return {
            "queue_name": self.queue_name,
            "pending_tasks": len(self.tasks),
            "completed_tasks": len(self.completed_tasks),
            "active_workers": len(self.workers)
        }

def demonstrate_stateful_patterns():
    """
    Shows stateful computation patterns with actors.
    GOLD: Demonstrates when actors are essential!
    """
    print("\n=== STATEFUL COMPUTATION PATTERNS ===")
    
    # Pattern 1: Data accumulation and batch processing
    print("1. Data accumulation pattern:")
    processor = DataProcessor.remote("processor-1")
    
    # Add data over time
    ray.get(processor.add_data.remote([1, 2, 3, 4, 5]))
    ray.get(processor.add_data.remote([6, 7, 8, 9, 10]))
    ray.get(processor.add_data.remote([11, 12, 13, 14, 15]))
    
    # Process batches
    batch1 = ray.get(processor.process_batch.remote(5))
    print(f"Batch 1 result: {batch1['processed_data']}")
    
    batch2 = ray.get(processor.process_batch.remote(5))
    print(f"Batch 2 result: {batch2['processed_data']}")
    
    # Get final stats
    stats = ray.get(processor.get_stats.remote())
    print(f"Processor stats: {stats}")
    
    # Pattern 2: Work queue
    print("\n2. Work queue pattern:")
    queue = WorkQueue.remote("main-queue")
    
    # Add some tasks
    for i in range(5):
        ray.get(queue.add_task.remote(f"task-data-{i}"))
    
    # Simulate workers getting tasks
    task1 = ray.get(queue.get_task.remote("worker-1"))
    task2 = ray.get(queue.get_task.remote("worker-2"))
    
    print(f"Worker 1 got: {task1}")
    print(f"Worker 2 got: {task2}")
    
    # Complete tasks
    if task1:
        ray.get(queue.complete_task.remote(task1["id"], "result-1"))
    if task2:
        ray.get(queue.complete_task.remote(task2["id"], "result-2"))
    
    # Check queue stats
    queue_stats = ray.get(queue.get_queue_stats.remote())
    print(f"Queue stats: {queue_stats}")

# =============================================================================
# 3. ACTOR-TASK INTERACTION - CRITICAL TOPIC
# =============================================================================

@ray.remote
class ParameterServer:
    """
    Parameter server pattern - common in ML applications.
    SCENARIO: "How do actors and tasks work together?"
    """
    
    def __init__(self, dimensions: int):
        self.parameters = np.random.rand(dimensions)
        self.version = 0
        print(f"ParameterServer initialized with {dimensions} dimensions")
    
    def get_parameters(self) -> tuple:
        """Get current parameters and version."""
        return self.parameters.copy(), self.version
    
    def update_parameters(self, gradients: np.ndarray, learning_rate: float = 0.01) -> int:
        """Update parameters with gradients."""
        self.parameters -= learning_rate * gradients
        self.version += 1
        print(f"Parameters updated to version {self.version}")
        return self.version
    
    def get_parameter_stats(self) -> Dict[str, Any]:
        """Get parameter statistics."""
        return {
            "version": self.version,
            "mean": float(np.mean(self.parameters)),
            "std": float(np.std(self.parameters)),
            "min": float(np.min(self.parameters)),
            "max": float(np.max(self.parameters))
        }

@ray.remote
def worker_task(worker_id: int, param_server, data_batch: List[float]) -> Dict[str, Any]:
    """
    Worker task that interacts with parameter server.
    CONCEPT: Tasks can call actor methods!
    """
    print(f"Worker {worker_id} starting training")
    
    # Get current parameters from actor
    parameters, version = ray.get(param_server.get_parameters.remote())
    
    # Simulate gradient computation
    time.sleep(0.1)  # Simulate computation
    gradients = np.random.rand(len(parameters)) * 0.1
    
    # Update parameters through actor
    new_version = ray.get(param_server.update_parameters.remote(gradients))
    
    return {
        "worker_id": worker_id,
        "old_version": version,
        "new_version": new_version,
        "batch_size": len(data_batch)
    }

def demonstrate_actor_task_interaction():
    """
    Shows how actors and tasks work together.
    CRITICAL: Understand the interaction patterns!
    """
    print("\n=== ACTOR-TASK INTERACTION ===")
    
    # Create parameter server actor
    param_server = ParameterServer.remote(dimensions=10)
    
    # Get initial stats
    initial_stats = ray.get(param_server.get_parameter_stats.remote())
    print(f"Initial parameters: {initial_stats}")
    
    # Launch multiple worker tasks that interact with the actor
    num_workers = 4
    data_batches = [[random.random() for _ in range(100)] for _ in range(num_workers)]
    
    # Tasks call actor methods during execution
    worker_futures = [
        worker_task.remote(i, param_server, data_batches[i])
        for i in range(num_workers)
    ]
    
    # Wait for all workers to complete
    worker_results = ray.get(worker_futures)
    
    for result in worker_results:
        print(f"Worker {result['worker_id']}: "
              f"v{result['old_version']} -> v{result['new_version']}")
    
    # Get final parameter stats
    final_stats = ray.get(param_server.get_parameter_stats.remote())
    print(f"Final parameters: {final_stats}")

# =============================================================================
# 4. ACTOR LIFECYCLE MANAGEMENT - KNOWLEDGE
# =============================================================================

@ray.remote
class ResourceManager:
    """
    Actor demonstrating lifecycle management patterns.
    TOPIC: "How do you manage actor resources and cleanup?"
    """
    
    def __init__(self, manager_id: str):
        self.manager_id = manager_id
        self.resources = {}
        self.active_connections = 0
        self.start_time = time.time()
        print(f"ResourceManager {manager_id} started")
    
    def allocate_resource(self, resource_id: str, resource_type: str) -> bool:
        """Allocate a resource."""
        if resource_id in self.resources:
            return False
        
        self.resources[resource_id] = {
            "type": resource_type,
            "allocated_at": time.time(),
            "status": "active"
        }
        self.active_connections += 1
        print(f"Allocated resource {resource_id} of type {resource_type}")
        return True
    
    def release_resource(self, resource_id: str) -> bool:
        """Release a resource."""
        if resource_id not in self.resources:
            return False
        
        self.resources[resource_id]["status"] = "released"
        self.resources[resource_id]["released_at"] = time.time()
        self.active_connections -= 1
        print(f"Released resource {resource_id}")
        return True
    
    def cleanup_resources(self) -> int:
        """Clean up all resources."""
        released_count = 0
        for resource_id, resource in self.resources.items():
            if resource["status"] == "active":
                resource["status"] = "released"
                resource["released_at"] = time.time()
                released_count += 1
        
        self.active_connections = 0
        print(f"Cleaned up {released_count} active resources")
        return released_count
    
    def get_resource_stats(self) -> Dict[str, Any]:
        """Get resource statistics."""
        total_resources = len(self.resources)
        active_resources = sum(1 for r in self.resources.values() if r["status"] == "active")
        
        return {
            "manager_id": self.manager_id,
            "uptime": time.time() - self.start_time,
            "total_resources": total_resources,
            "active_resources": active_resources,
            "active_connections": self.active_connections
        }

def demonstrate_actor_lifecycle():
    """
    Shows actor lifecycle management patterns.
    TIP: Understand when and how to clean up actors!
    """
    print("\n=== ACTOR LIFECYCLE MANAGEMENT ===")
    
    # Create resource manager
    manager = ResourceManager.remote("manager-1")
    
    # Allocate some resources
    resources = ["db-conn-1", "cache-conn-1", "file-handle-1"]
    for resource_id in resources:
        result = ray.get(manager.allocate_resource.remote(resource_id, "connection"))
        print(f"Allocation result for {resource_id}: {result}")
    
    # Check stats
    stats = ray.get(manager.get_resource_stats.remote())
    print(f"Stats after allocation: {stats}")
    
    # Release some resources
    ray.get(manager.release_resource.remote("db-conn-1"))
    
    # Check stats again
    stats = ray.get(manager.get_resource_stats.remote())
    print(f"Stats after release: {stats}")
    
    # Cleanup all resources
    released = ray.get(manager.cleanup_resources.remote())
    print(f"Cleanup released {released} resources")
    
    # Final stats
    final_stats = ray.get(manager.get_resource_stats.remote())
    print(f"Final stats: {final_stats}")

# =============================================================================
# 5. PERFORMANCE PATTERNS AND ANTI-PATTERNS - CRITICAL
# =============================================================================

@ray.remote
class PerformanceActor:
    """
    Actor demonstrating performance considerations.
    FOCUS: When actors help vs hurt performance.
    """
    
    def __init__(self):
        self.call_count = 0
        self.total_time = 0
    
    def fast_method(self, x: int) -> int:
        """Fast method - good for frequent calls."""
        self.call_count += 1
        return x * 2
    
    def slow_method(self, data: List[int]) -> List[int]:
        """Slow method - demonstrates when actor overhead matters."""
        start_time = time.time()
        # Simulate expensive computation
        result = [x ** 2 for x in data]
        time.sleep(0.01)  # Simulate I/O or complex computation
        
        self.total_time += time.time() - start_time
        self.call_count += 1
        return result
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """Get performance statistics."""
        return {
            "call_count": self.call_count,
            "total_time": self.total_time,
            "avg_time_per_call": self.total_time / self.call_count if self.call_count > 0 else 0
        }

@ray.remote
def equivalent_task(data: List[int]) -> List[int]:
    """Equivalent task for performance comparison."""
    time.sleep(0.01)
    return [x ** 2 for x in data]

def demonstrate_performance_patterns():
    """
    Shows performance considerations with actors.
    GOLD: Understand when to use actors vs tasks!
    """
    print("\n=== PERFORMANCE PATTERNS ===")
    
    # Performance comparison: Actor vs Tasks
    print("1. Performance comparison - Actor vs Tasks:")
    
    # Test data
    test_batches = [[i] * 10 for i in range(10)]
    
    # Actor approach
    perf_actor = PerformanceActor.remote()
    
    start_time = time.time()
    actor_futures = [perf_actor.slow_method.remote(batch) for batch in test_batches]
    actor_results = ray.get(actor_futures)
    actor_time = time.time() - start_time
    
    # Task approach
    start_time = time.time()
    task_futures = [equivalent_task.remote(batch) for batch in test_batches]
    task_results = ray.get(task_futures)
    task_time = time.time() - start_time
    
    print(f"Actor approach: {actor_time:.3f}s")
    print(f"Task approach: {task_time:.3f}s")
    print(f"Difference: {abs(actor_time - task_time):.3f}s")
    
    # Actor stats
    actor_stats = ray.get(perf_actor.get_performance_stats.remote())
    print(f"Actor stats: {actor_stats}")
    
    # Performance insights
    print("\n2. Performance insights:")
    print("✅ Use actors when:")
    print("   - You need persistent state")
    print("   - Frequent method calls with small overhead")
    print("   - Resource management (connections, caches)")
    print("   - Coordination between tasks")
    
    print("\n❌ Avoid actors when:")
    print("   - Pure computation without state")
    print("   - Infrequent method calls")
    print("   - Want maximum parallelism")
    print("   - Method calls have high computation/communication ratio")

# =============================================================================
# 6. COMMON ANTI-PATTERNS AND DEBUGGING - PREP
# =============================================================================

@ray.remote
class ProblematicActor:
    """
    Actor demonstrating common problems.
    PREP: Know these issues and how to fix them!
    """
    
    def __init__(self):
        self.data = []
        self.processing = False
    
    def blocking_method(self, delay: float) -> str:
        """
        ANTI-PATTERN: Blocking the actor for too long.
        This prevents other method calls from being processed!
        """
        print(f"Starting blocking operation for {delay}s")
        time.sleep(delay)  # Bad: blocks the entire actor
        return f"Blocked for {delay}s"
    
    def memory_leak_method(self, size: int) -> int:
        """
        ANTI-PATTERN: Memory leak in actor.
        Data accumulates without cleanup!
        """
        # Bad: keeps growing without cleanup
        self.data.extend([0] * size)
        return len(self.data)
    
    def race_condition_method(self) -> str:
        """
        ANTI-PATTERN: Race condition in actor state.
        Multiple calls can interfere with each other!
        """
        if self.processing:
            return "Already processing - skipped"
        
        self.processing = True
        time.sleep(0.1)  # Simulate work
        # Race condition: what if another call comes here?
        self.processing = False
        return "Processing completed"

def demonstrate_anti_patterns():
    """
    Shows common actor anti-patterns and debugging techniques.
    CRITICAL: Know what NOT to do with actors!
    """
    print("\n=== COMMON ANTI-PATTERNS AND DEBUGGING ===")
    
    problematic_actor = ProblematicActor.remote()
    
    # Anti-pattern 1: Blocking operations
    print("1. Blocking operations (ANTI-PATTERN):")
    print("Launching blocking call - other calls will queue up!")
    
    # This blocks the actor - no other methods can run
    blocking_future = problematic_actor.blocking_method.remote(2.0)
    
    # These will queue up behind the blocking call
    quick_futures = [
        problematic_actor.race_condition_method.remote()
        for _ in range(3)
    ]
    
    # Wait for blocking call
    blocking_result = ray.get(blocking_future)
    print(f"Blocking result: {blocking_result}")
    
    # Now the queued calls can execute
    quick_results = ray.get(quick_futures)
    for i, result in enumerate(quick_results):
        print(f"Quick call {i}: {result}")
    
    # Anti-pattern 2: Memory leaks
    print("\n2. Memory leak (ANTI-PATTERN):")
    for i in range(3):
        size = ray.get(problematic_actor.memory_leak_method.remote(1000))
        print(f"Actor memory usage growing: {size} items")
    
    print("\n3. How to debug actors:")
    print("✅ Use ray.get() judiciously - don't block unnecessarily")
    print("✅ Monitor actor state and memory usage")
    print("✅ Design for non-blocking operations when possible")
    print("✅ Implement proper cleanup methods")
    print("✅ Use actor pools for high-throughput scenarios")

# =============================================================================
# 7. MAIN DEMONSTRATION FUNCTION
# =============================================================================

def main():
    """
    Run all actor demonstrations.
    PREP: Understand each pattern and when to use it!
    """
    try:
        print("RAY CORE ACTORS DEMONSTRATION")
        print("============================")
        
        if not ray.is_initialized():
            ray.init(num_cpus=4, ignore_reinit_error=True)
        
        # Core actor concepts
        demonstrate_basic_actors()
        demonstrate_stateful_patterns()
        demonstrate_actor_task_interaction()
        demonstrate_actor_lifecycle()
        demonstrate_performance_patterns()
        demonstrate_anti_patterns()
        
        print("\n=== SUCCESS: All actor patterns demonstrated! ===")
        
    except Exception as e:
        print(f"Error in main: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if ray.is_initialized():
            ray.shutdown()

if __name__ == "__main__":
    main()

# =============================================================================
# PREPARATION SUMMARY
# =============================================================================
"""
RAY ACTORS - MASTERY CHECKLIST:

✅ BASIC CONCEPTS:
   - Actors maintain state between method calls
   - Use @ray.remote class decorator
   - Call methods with .remote() -> returns ObjectRefs
   - Each actor instance runs on single worker

✅ WHEN TO USE ACTORS:
   - Need persistent state between operations
   - Resource management (connections, caches)
   - Coordination between multiple tasks
   - Implementing services (queues, parameter servers)

✅ WHEN TO USE TASKS:
   - Pure computation without state
   - Maximum parallelism needed
   - Infrequent operations
   - High computation-to-communication ratio

✅ STATEFUL PATTERNS:
   - Data accumulation and batch processing
   - Work queues and job scheduling
   - Parameter servers for ML
   - Resource management and pooling

✅ ACTOR-TASK INTERACTION:
   - Tasks can call actor methods
   - Actors coordinate multiple tasks
   - Parameter server pattern
   - Shared state management

✅ PERFORMANCE CONSIDERATIONS:
   - Actors have scheduling overhead
   - Good for frequent small operations
   - Avoid long-blocking operations
   - Monitor memory usage and cleanup

✅ COMMON PITFALLS:
   - Blocking the actor with long operations
   - Memory leaks from accumulating state
   - Race conditions in actor methods
   - Overusing actors for stateless computation

QUESTIONS TO MASTER:
1. "When would you use an actor vs a task?" -> State persistence needs
2. "How do actors and tasks work together?" -> Tasks call actor methods
3. "What are the performance implications?" -> Overhead vs state benefits
4. "How do you debug slow actor performance?" -> Monitor blocking calls
5. "What's a parameter server pattern?" -> ML coordination with actors

NEXT: Practice with 04-ray-core-debugging.py for troubleshooting skills!
""" 