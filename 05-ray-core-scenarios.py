#!/usr/bin/env python3
"""
RAY CORE SCENARIOS - File 05
======================================

This file contains realistic scenarios that combine all Ray Core concepts:
1. Complete coding challenges
2. Debugging scenarios
3. Performance optimization problems
4. Architecture design questions
5. Real-world system implementations

PREPARATION:
- Practice implementing each scenario from scratch
- Understand the reasoning behind each solution
- Be ready to explain trade-offs and alternatives
- Practice debugging and optimization techniques
"""

import ray
import time
import random
import threading
import numpy as np
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
from collections import defaultdict
import json

# =============================================================================
# SCENARIO 1: DISTRIBUTED DATA PROCESSING PIPELINE
# =============================================================================

@dataclass
class DataBatch:
    """Data batch for processing pipeline."""
    batch_id: int
    data: List[Dict[str, Any]]
    created_at: float

@ray.remote
class DataValidator:
    """Actor that validates incoming data batches."""
    
    def __init__(self, validator_id: str):
        self.validator_id = validator_id
        self.processed_batches = 0
        self.validation_errors = 0
    
    def validate_batch(self, batch: DataBatch) -> Dict[str, Any]:
        """
        Validate a data batch.
        SCENARIO: "Implement a data validation service"
        """
        start_time = time.time()
        valid_records = []
        invalid_records = []
        
        for record in batch.data:
            # Validation rules
            if self._is_valid_record(record):
                valid_records.append(record)
            else:
                invalid_records.append(record)
        
        self.processed_batches += 1
        if invalid_records:
            self.validation_errors += 1
        
        return {
            "batch_id": batch.batch_id,
            "validator_id": self.validator_id,
            "valid_count": len(valid_records),
            "invalid_count": len(invalid_records),
            "valid_records": valid_records,
            "invalid_records": invalid_records,
            "processing_time": time.time() - start_time
        }
    
    def _is_valid_record(self, record: Dict[str, Any]) -> bool:
        """Validate individual record."""
        required_fields = ["id", "timestamp", "value"]
        
        # Check required fields
        if not all(field in record for field in required_fields):
            return False
        
        # Check data types
        try:
            int(record["id"])
            float(record["timestamp"])
            float(record["value"])
        except (ValueError, TypeError):
            return False
        
        # Range validation
        if record["value"] < 0 or record["value"] > 1000:
            return False
        
        return True
    
    def get_stats(self) -> Dict[str, Any]:
        """Get validator statistics."""
        return {
            "validator_id": self.validator_id,
            "processed_batches": self.processed_batches,
            "validation_errors": self.validation_errors,
            "error_rate": self.validation_errors / self.processed_batches if self.processed_batches > 0 else 0
        }

@ray.remote
def data_transformation_task(validated_batch: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform validated data batch.
    SCENARIO: "Process data after validation"
    """
    batch_id = validated_batch["batch_id"]
    valid_records = validated_batch["valid_records"]
    
    # Transformation logic
    transformed_records = []
    for record in valid_records:
        transformed_record = {
            "id": record["id"],
            "timestamp": record["timestamp"],
            "value": record["value"],
            "processed_value": record["value"] * 1.1,  # Apply transformation
            "category": "high" if record["value"] > 500 else "low"
        }
        transformed_records.append(transformed_record)
    
    # Simulate processing time
    time.sleep(0.1)
    
    return {
        "batch_id": batch_id,
        "transformed_records": transformed_records,
        "transformation_count": len(transformed_records)
    }

@ray.remote
class DataAggregator:
    """Actor that aggregates processed data."""
    
    def __init__(self):
        self.aggregated_data = defaultdict(list)
        self.total_processed = 0
    
    def aggregate_batch(self, transformed_batch: Dict[str, Any]) -> Dict[str, Any]:
        """
        Aggregate transformed data.
        CONCEPT: Stateful aggregation with actors
        """
        batch_id = transformed_batch["batch_id"]
        records = transformed_batch["transformed_records"]
        
        # Aggregate by category
        for record in records:
            category = record["category"]
            self.aggregated_data[category].append(record)
        
        self.total_processed += len(records)
        
        return {
            "batch_id": batch_id,
            "records_aggregated": len(records),
            "total_processed": self.total_processed
        }
    
    def get_aggregated_results(self) -> Dict[str, Any]:
        """Get final aggregated results."""
        results = {}
        for category, records in self.aggregated_data.items():
            values = [r["processed_value"] for r in records]
            results[category] = {
                "count": len(records),
                "mean": sum(values) / len(values) if values else 0,
                "min": min(values) if values else 0,
                "max": max(values) if values else 0
            }
        
        return {
            "category_stats": results,
            "total_processed": self.total_processed
        }

def scenario_1_data_pipeline():
    """
    SCENARIO 1: Distributed Data Processing Pipeline
    
    REQUIREMENTS:
    - Process data batches through validation, transformation, and aggregation
    - Handle errors gracefully
    - Maintain statistics
    - Optimize for throughput
    
    QUESTIONS:
    1. "Design a distributed data processing pipeline"
    2. "How do you handle data validation at scale?"
    3. "What happens if a validator fails?"
    4. "How do you optimize this pipeline?"
    """
    print("\n" + "="*60)
    print("SCENARIO 1: DISTRIBUTED DATA PROCESSING PIPELINE")
    print("="*60)
    
    # Create sample data batches
    def create_sample_batch(batch_id: int, size: int = 100) -> DataBatch:
        data = []
        for i in range(size):
            # Mix of valid and invalid data
            record = {
                "id": i,
                "timestamp": time.time() + i,
                "value": random.uniform(-10, 1100) if random.random() > 0.8 else random.uniform(0, 1000)
            }
            # Sometimes omit required fields
            if random.random() > 0.9:
                del record["value"]
            
            data.append(record)
        
        return DataBatch(batch_id=batch_id, data=data, created_at=time.time())
    
    # Step 1: Create pipeline components
    print("1. Setting up pipeline components...")
    
    # Create multiple validators for load balancing
    validators = [DataValidator.remote(f"validator-{i}") for i in range(3)]
    aggregator = DataAggregator.remote()
    
    # Step 2: Generate data batches
    print("2. Generating data batches...")
    batches = [create_sample_batch(i, 50) for i in range(10)]
    
    # Step 3: Process through pipeline
    print("3. Processing through pipeline...")
    
    # Phase 1: Validation (load balanced)
    validation_futures = []
    for i, batch in enumerate(batches):
        validator = validators[i % len(validators)]  # Load balance
        future = validator.validate_batch.remote(batch)
        validation_futures.append(future)
    
    # Wait for validation results
    validation_results = ray.get(validation_futures)
    
    # Phase 2: Transformation (parallel)
    transformation_futures = []
    for validated_batch in validation_results:
        if validated_batch["valid_count"] > 0:  # Only transform if valid data exists
            future = data_transformation_task.remote(validated_batch)
            transformation_futures.append(future)
    
    transformation_results = ray.get(transformation_futures)
    
    # Phase 3: Aggregation (sequential through actor)
    aggregation_futures = []
    for transformed_batch in transformation_results:
        future = aggregator.aggregate_batch.remote(transformed_batch)
        aggregation_futures.append(future)
    
    aggregation_results = ray.get(aggregation_futures)
    
    # Step 4: Get final results and statistics
    print("4. Collecting results and statistics...")
    
    # Validator statistics
    validator_stats = ray.get([v.get_stats.remote() for v in validators])
    print("Validator Statistics:")
    for stats in validator_stats:
        print(f"  {stats['validator_id']}: {stats['processed_batches']} batches, "
              f"{stats['error_rate']:.2%} error rate")
    
    # Final aggregated results
    final_results = ray.get(aggregator.get_aggregated_results.remote())
    print("Final Aggregated Results:")
    for category, stats in final_results["category_stats"].items():
        print(f"  {category.upper()}: {stats['count']} records, "
              f"mean={stats['mean']:.2f}, range=[{stats['min']:.2f}, {stats['max']:.2f}]")
    
    print(f"Total records processed: {final_results['total_processed']}")
    
    return final_results

# =============================================================================
# SCENARIO 2: DISTRIBUTED MACHINE LEARNING TRAINING
# =============================================================================

@ray.remote
class ParameterServer:
    """
    Parameter server for distributed ML training.
    SCENARIO: "Implement parameter server pattern"
    """
    
    def __init__(self, model_dim: int, learning_rate: float = 0.01):
        self.parameters = np.random.rand(model_dim) * 0.1
        self.learning_rate = learning_rate
        self.version = 0
        self.gradient_count = 0
        
    def get_parameters(self) -> Tuple[np.ndarray, int]:
        """Get current parameters and version."""
        return self.parameters.copy(), self.version
    
    def update_parameters(self, gradients: np.ndarray, worker_id: str) -> int:
        """Update parameters with gradients from worker."""
        # Apply gradients
        self.parameters -= self.learning_rate * gradients
        self.version += 1
        self.gradient_count += 1
        
        print(f"Updated parameters to v{self.version} from {worker_id}")
        return self.version
    
    def get_training_stats(self) -> Dict[str, Any]:
        """Get training statistics."""
        return {
            "version": self.version,
            "gradient_updates": self.gradient_count,
            "parameter_norm": float(np.linalg.norm(self.parameters)),
            "parameter_mean": float(np.mean(self.parameters))
        }

@ray.remote
def training_worker(worker_id: str, param_server, training_data: np.ndarray, epochs: int = 5) -> Dict[str, Any]:
    """
    Distributed training worker.
    SCENARIO: "Implement distributed training worker"
    """
    worker_stats = {
        "worker_id": worker_id,
        "epochs_completed": 0,
        "total_loss": 0.0,
        "training_time": 0.0
    }
    
    start_time = time.time()
    
    for epoch in range(epochs):
        # Get current parameters
        parameters, version = ray.get(param_server.get_parameters.remote())
        
        # Simulate training step
        loss = compute_loss(parameters, training_data)
        gradients = compute_gradients(parameters, training_data)
        
        # Update parameter server
        new_version = ray.get(param_server.update_parameters.remote(gradients, worker_id))
        
        worker_stats["epochs_completed"] += 1
        worker_stats["total_loss"] += loss
        
        print(f"Worker {worker_id} epoch {epoch}: loss={loss:.4f}, v{version}->v{new_version}")
        
        # Simulate computation time
        time.sleep(0.1)
    
    worker_stats["training_time"] = time.time() - start_time
    worker_stats["avg_loss"] = worker_stats["total_loss"] / epochs
    
    return worker_stats

def compute_loss(parameters: np.ndarray, data: np.ndarray) -> float:
    """Simulate loss computation."""
    # Simple quadratic loss for demonstration
    target = np.ones_like(parameters) * 0.5
    return float(np.mean((parameters - target) ** 2))

def compute_gradients(parameters: np.ndarray, data: np.ndarray) -> np.ndarray:
    """Simulate gradient computation."""
    # Simple gradient for quadratic loss
    target = np.ones_like(parameters) * 0.5
    gradients = 2 * (parameters - target) + np.random.normal(0, 0.01, parameters.shape)
    return gradients

def scenario_2_distributed_training():
    """
    SCENARIO 2: Distributed Machine Learning Training
    
    REQUIREMENTS:
    - Implement parameter server pattern
    - Handle multiple workers training in parallel
    - Coordinate parameter updates
    - Track training progress
    
    QUESTIONS:
    1. "How do you implement distributed ML training?"
    2. "What are the challenges with parameter servers?"
    3. "How do you handle worker failures?"
    4. "What about gradient synchronization?"
    """
    print("\n" + "="*60)
    print("SCENARIO 2: DISTRIBUTED MACHINE LEARNING TRAINING")
    print("="*60)
    
    # Configuration
    model_dim = 100
    num_workers = 4
    epochs_per_worker = 10
    
    print(f"Training configuration: {model_dim}D model, {num_workers} workers, {epochs_per_worker} epochs each")
    
    # Step 1: Create parameter server
    print("1. Initializing parameter server...")
    param_server = ParameterServer.remote(model_dim, learning_rate=0.01)
    
    # Step 2: Generate training data for each worker
    print("2. Generating training data...")
    training_datasets = []
    for i in range(num_workers):
        # Each worker gets different data
        data = np.random.rand(1000, model_dim)
        training_datasets.append(data)
    
    # Step 3: Launch distributed training
    print("3. Launching distributed training...")
    
    training_start = time.time()
    
    # Start all workers
    worker_futures = []
    for i, data in enumerate(training_datasets):
        worker_id = f"worker-{i}"
        future = training_worker.remote(worker_id, param_server, data, epochs_per_worker)
        worker_futures.append(future)
    
    # Wait for all workers to complete
    worker_results = ray.get(worker_futures)
    
    training_time = time.time() - training_start
    
    # Step 4: Collect and analyze results
    print("4. Analyzing training results...")
    
    # Worker statistics
    print("Worker Statistics:")
    total_epochs = 0
    for result in worker_results:
        print(f"  {result['worker_id']}: {result['epochs_completed']} epochs, "
              f"avg_loss={result['avg_loss']:.4f}, time={result['training_time']:.2f}s")
        total_epochs += result['epochs_completed']
    
    # Parameter server statistics
    server_stats = ray.get(param_server.get_training_stats.remote())
    print("Parameter Server Statistics:")
    print(f"  Final version: {server_stats['version']}")
    print(f"  Gradient updates: {server_stats['gradient_updates']}")
    print(f"  Parameter norm: {server_stats['parameter_norm']:.4f}")
    print(f"  Parameter mean: {server_stats['parameter_mean']:.4f}")
    
    print(f"Total training time: {training_time:.2f}s")
    print(f"Total epochs across all workers: {total_epochs}")
    
    return {
        "training_time": training_time,
        "worker_results": worker_results,
        "server_stats": server_stats
    }

# =============================================================================
# SCENARIO 3: REAL-TIME MONITORING AND ALERTING SYSTEM
# =============================================================================

@ray.remote
class MetricsCollector:
    """
    Actor that collects metrics from various sources.
    SCENARIO: "Build a real-time monitoring system"
    """
    
    def __init__(self, collector_id: str):
        self.collector_id = collector_id
        self.metrics_buffer = []
        self.alert_thresholds = {
            "cpu_usage": 80.0,
            "memory_usage": 90.0,
            "error_rate": 5.0
        }
    
    def collect_metric(self, metric: Dict[str, Any]) -> bool:
        """Collect a single metric."""
        metric["collected_at"] = time.time()
        metric["collector_id"] = self.collector_id
        self.metrics_buffer.append(metric)
        
        # Check for alerts
        alert_triggered = self._check_alert_conditions(metric)
        
        return alert_triggered
    
    def _check_alert_conditions(self, metric: Dict[str, Any]) -> bool:
        """Check if metric triggers any alerts."""
        metric_type = metric.get("type")
        value = metric.get("value", 0)
        
        threshold = self.alert_thresholds.get(metric_type)
        if threshold and value > threshold:
            print(f"🚨 ALERT: {metric_type} = {value} exceeds threshold {threshold}")
            return True
        
        return False
    
    def get_recent_metrics(self, count: int = 10) -> List[Dict[str, Any]]:
        """Get recent metrics."""
        return self.metrics_buffer[-count:]
    
    def get_metrics_summary(self) -> Dict[str, Any]:
        """Get summary of collected metrics."""
        if not self.metrics_buffer:
            return {"total_metrics": 0}
        
        # Group by metric type
        type_counts = defaultdict(int)
        for metric in self.metrics_buffer:
            type_counts[metric.get("type", "unknown")] += 1
        
        return {
            "collector_id": self.collector_id,
            "total_metrics": len(self.metrics_buffer),
            "metrics_by_type": dict(type_counts),
            "collection_period": self.metrics_buffer[-1]["collected_at"] - self.metrics_buffer[0]["collected_at"]
        }

@ray.remote
def system_monitor_task(system_id: str, metrics_collector, duration: float = 10.0) -> Dict[str, Any]:
    """
    Task that monitors a system and sends metrics to collector.
    SCENARIO: "Monitor system health and send metrics"
    """
    print(f"Starting monitoring for {system_id}")
    
    start_time = time.time()
    metrics_sent = 0
    alerts_triggered = 0
    
    while time.time() - start_time < duration:
        # Generate realistic system metrics
        metrics = [
            {
                "system_id": system_id,
                "type": "cpu_usage",
                "value": random.uniform(0, 100),
                "timestamp": time.time()
            },
            {
                "system_id": system_id,
                "type": "memory_usage",
                "value": random.uniform(20, 95),
                "timestamp": time.time()
            },
            {
                "system_id": system_id,
                "type": "error_rate",
                "value": random.uniform(0, 10),
                "timestamp": time.time()
            }
        ]
        
        # Send metrics to collector
        for metric in metrics:
            alert_triggered = ray.get(metrics_collector.collect_metric.remote(metric))
            if alert_triggered:
                alerts_triggered += 1
            metrics_sent += 1
        
        # Wait before next collection
        time.sleep(1.0)
    
    monitoring_time = time.time() - start_time
    
    return {
        "system_id": system_id,
        "monitoring_time": monitoring_time,
        "metrics_sent": metrics_sent,
        "alerts_triggered": alerts_triggered
    }

@ray.remote
class AlertManager:
    """
    Actor that manages alerts and notifications.
    SCENARIO: "Handle alerts and notifications"
    """
    
    def __init__(self):
        self.active_alerts = {}
        self.alert_history = []
    
    def process_alert(self, alert: Dict[str, Any]) -> bool:
        """Process an incoming alert."""
        alert_id = f"{alert['system_id']}-{alert['metric_type']}"
        
        # Check if this is a new alert or update
        if alert_id not in self.active_alerts:
            # New alert
            self.active_alerts[alert_id] = alert
            self.alert_history.append(alert)
            print(f"🔔 NEW ALERT: {alert_id} - {alert['message']}")
            return True
        else:
            # Update existing alert
            self.active_alerts[alert_id].update(alert)
            print(f"📝 UPDATED ALERT: {alert_id}")
            return False
    
    def resolve_alert(self, alert_id: str) -> bool:
        """Resolve an active alert."""
        if alert_id in self.active_alerts:
            resolved_alert = self.active_alerts.pop(alert_id)
            resolved_alert["resolved_at"] = time.time()
            print(f"✅ RESOLVED ALERT: {alert_id}")
            return True
        return False
    
    def get_alert_summary(self) -> Dict[str, Any]:
        """Get summary of alert status."""
        return {
            "active_alerts": len(self.active_alerts),
            "total_alerts": len(self.alert_history),
            "active_alert_ids": list(self.active_alerts.keys())
        }

def scenario_3_monitoring_system():
    """
    SCENARIO 3: Real-time Monitoring and Alerting System
    
    REQUIREMENTS:
    - Collect metrics from multiple systems
    - Detect threshold violations
    - Generate and manage alerts
    - Handle high-frequency data
    
    QUESTIONS:
    1. "Design a real-time monitoring system"
    2. "How do you handle high-frequency metrics?"
    3. "What about alert fatigue and deduplication?"
    4. "How do you scale metric collection?"
    """
    print("\n" + "="*60)
    print("SCENARIO 3: REAL-TIME MONITORING AND ALERTING SYSTEM")
    print("="*60)
    
    # Step 1: Set up monitoring infrastructure
    print("1. Setting up monitoring infrastructure...")
    
    # Create metrics collectors (one per region/datacenter)
    collectors = [MetricsCollector.remote(f"collector-{i}") for i in range(3)]
    alert_manager = AlertManager.remote()
    
    # Step 2: Launch system monitors
    print("2. Launching system monitors...")
    
    systems_to_monitor = ["web-server-1", "db-server-1", "cache-server-1", "api-server-1"]
    monitor_futures = []
    
    for i, system_id in enumerate(systems_to_monitor):
        # Distribute systems across collectors
        collector = collectors[i % len(collectors)]
        future = system_monitor_task.remote(system_id, collector, 5.0)  # Monitor for 5 seconds
        monitor_futures.append(future)
    
    print(f"Monitoring {len(systems_to_monitor)} systems across {len(collectors)} collectors...")
    
    # Step 3: Wait for monitoring to complete
    monitor_results = ray.get(monitor_futures)
    
    # Step 4: Collect and analyze results
    print("3. Analyzing monitoring results...")
    
    # System monitoring results
    print("System Monitoring Results:")
    total_metrics = 0
    total_alerts = 0
    
    for result in monitor_results:
        print(f"  {result['system_id']}: {result['metrics_sent']} metrics, "
              f"{result['alerts_triggered']} alerts, "
              f"monitoring_time={result['monitoring_time']:.1f}s")
        total_metrics += result['metrics_sent']
        total_alerts += result['alerts_triggered']
    
    # Collector statistics
    print("Collector Statistics:")
    collector_summaries = ray.get([c.get_metrics_summary.remote() for c in collectors])
    
    for summary in collector_summaries:
        if summary['total_metrics'] > 0:
            print(f"  {summary['collector_id']}: {summary['total_metrics']} metrics collected")
            print(f"    Types: {summary['metrics_by_type']}")
    
    # Alert manager statistics
    alert_summary = ray.get(alert_manager.get_alert_summary.remote())
    print("Alert Manager Statistics:")
    print(f"  Active alerts: {alert_summary['active_alerts']}")
    print(f"  Total alerts: {alert_summary['total_alerts']}")
    
    print(f"Overall: {total_metrics} metrics processed, {total_alerts} alerts triggered")
    
    return {
        "total_metrics": total_metrics,
        "total_alerts": total_alerts,
        "monitor_results": monitor_results,
        "collector_summaries": collector_summaries,
        "alert_summary": alert_summary
    }

# =============================================================================
# SCENARIO 4: PERFORMANCE DEBUGGING CHALLENGE
# =============================================================================

def scenario_4_performance_debugging():
    """
    SCENARIO 4: Performance Debugging Challenge
    
    PROBLEM:
    You have a Ray application that's running slower than expected.
    Your job is to identify bottlenecks and optimize performance.
    
    QUESTIONS:
    1. "This Ray app is slow - debug and fix it"
    2. "How do you identify performance bottlenecks?"
    3. "What tools would you use to profile Ray applications?"
    4. "How do you optimize task scheduling?"
    """
    print("\n" + "="*60)
    print("SCENARIO 4: PERFORMANCE DEBUGGING CHALLENGE")
    print("="*60)
    
    print("You have been given a slow Ray application to debug and optimize...")
    
    # PROBLEM CODE: Intentionally inefficient Ray application
    
    @ray.remote
    def inefficient_task(data: List[int]) -> int:
        """Inefficient task with multiple problems."""
        # Problem 1: Unnecessary computation
        result = 0
        for i in data:
            for j in range(1000):  # Unnecessary nested loop
                result += i
        
        # Problem 2: Blocking operation
        time.sleep(0.5)  # Simulating slow I/O
        
        return result
    
    @ray.remote
    class InefficientActor:
        """Actor with performance problems."""
        
        def __init__(self):
            self.data = []
        
        def add_data(self, items: List[int]) -> None:
            """Problem: Frequent small operations."""
            for item in items:
                self.data.append(item)  # Could batch this
        
        def process_all_data(self) -> int:
            """Problem: Processing all data at once."""
            # This will block the actor for a long time
            total = 0
            for item in self.data:
                time.sleep(0.001)  # Simulate per-item processing
                total += item
            return total
    
    # Demonstrate the problems
    print("1. Running inefficient code...")
    
    start_time = time.time()
    
    # Problem: Sequential execution pattern
    print("Problem: Sequential task execution")
    sequential_results = []
    for i in range(5):
        data = list(range(10))
        result = ray.get(inefficient_task.remote(data))  # Sequential ray.get()
        sequential_results.append(result)
    
    sequential_time = time.time() - start_time
    print(f"Sequential execution time: {sequential_time:.2f}s")
    
    # Problem: Actor bottleneck
    print("Problem: Actor bottleneck")
    actor_start = time.time()
    
    actor = InefficientActor.remote()
    
    # Add data in small chunks (inefficient)
    for i in range(10):
        ray.get(actor.add_data.remote([i]))  # Sequential small operations
    
    # Process all data (blocks actor)
    total = ray.get(actor.process_all_data.remote())
    
    actor_time = time.time() - actor_start
    print(f"Actor bottleneck time: {actor_time:.2f}s")
    
    print("\n2. Identifying and fixing problems...")
    
    # SOLUTIONS: Optimized versions
    
    @ray.remote
    def efficient_task(data: List[int]) -> int:
        """Optimized task."""
        # Fix 1: Simplified computation
        result = sum(data) * 1000  # Equivalent but much faster
        
        # Fix 2: Non-blocking or optimized I/O
        # In real scenario, this might be async I/O or caching
        time.sleep(0.01)  # Reduced from 0.5s
        
        return result
    
    @ray.remote
    class EfficientActor:
        """Optimized actor."""
        
        def __init__(self):
            self.data = []
        
        def add_data_batch(self, items: List[int]) -> None:
            """Fix: Batch operations."""
            self.data.extend(items)  # Batch extend instead of individual appends
        
        def process_data_chunk(self, start_idx: int, end_idx: int) -> int:
            """Fix: Process data in chunks."""
            chunk = self.data[start_idx:end_idx]
            return sum(chunk)  # Fast operation
    
    # Demonstrate the fixes
    print("Solution: Parallel task execution")
    parallel_start = time.time()
    
    # Fix: Parallel execution
    parallel_futures = []
    for i in range(5):
        data = list(range(10))
        future = efficient_task.remote(data)
        parallel_futures.append(future)
    
    parallel_results = ray.get(parallel_futures)  # Batch ray.get()
    parallel_time = time.time() - parallel_start
    print(f"Parallel execution time: {parallel_time:.2f}s")
    print(f"Speedup: {sequential_time / parallel_time:.1f}x")
    
    print("Solution: Optimized actor usage")
    optimized_actor_start = time.time()
    
    optimized_actor = EfficientActor.remote()
    
    # Batch data addition
    all_data = list(range(100))
    ray.get(optimized_actor.add_data_batch.remote(all_data))
    
    # Process in parallel chunks
    chunk_size = 20
    chunk_futures = []
    for start in range(0, 100, chunk_size):
        future = optimized_actor.process_data_chunk.remote(start, start + chunk_size)
        chunk_futures.append(future)
    
    chunk_results = ray.get(chunk_futures)
    optimized_total = sum(chunk_results)
    
    optimized_actor_time = time.time() - optimized_actor_start
    print(f"Optimized actor time: {optimized_actor_time:.2f}s")
    print(f"Actor speedup: {actor_time / optimized_actor_time:.1f}x")
    
    print("\n3. Performance debugging best practices:")
    print("✅ Use Ray Dashboard to visualize task execution")
    print("✅ Avoid sequential ray.get() calls")
    print("✅ Batch operations in actors")
    print("✅ Profile CPU vs I/O bound tasks")
    print("✅ Monitor resource utilization")
    print("✅ Use appropriate parallelism levels")
    
    return {
        "sequential_time": sequential_time,
        "parallel_time": parallel_time,
        "speedup": sequential_time / parallel_time,
        "actor_time": actor_time,
        "optimized_actor_time": optimized_actor_time,
        "actor_speedup": actor_time / optimized_actor_time
    }

# =============================================================================
# MAIN SIMULATION
# =============================================================================

def main():
    """
    Run all scenarios.
    PREP: Practice implementing and explaining each scenario!
    """
    try:
        print("RAY CORE SCENARIOS")
        print("="*80)
        print("PRACTICING COMPLETE CHALLENGES")
        print("="*80)
        
        if not ray.is_initialized():
            ray.init(num_cpus=4, ignore_reinit_error=True)
        
        # Run all scenarios
        results = {}
        
        # Scenario 1: Data Processing Pipeline
        results["scenario_1"] = scenario_1_data_pipeline()
        
        # Scenario 2: Distributed ML Training
        results["scenario_2"] = scenario_2_distributed_training()
        
        # Scenario 3: Monitoring System
        results["scenario_3"] = scenario_3_monitoring_system()
        
        # Scenario 4: Performance Debugging
        results["scenario_4"] = scenario_4_performance_debugging()
        
        print("\n" + "="*80)
        print("SCENARIOS COMPLETED SUCCESSFULLY!")
        print("="*80)
        
        return results
        
    except Exception as e:
        print(f"Error in scenarios: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if ray.is_initialized():
            ray.shutdown()

if __name__ == "__main__":
    main()

# =============================================================================
# MASTERY CHECKLIST
# =============================================================================
"""
RAY CORE MASTERY - FINAL CHECKLIST:

🎯 SCENARIO-BASED SKILLS:

✅ DATA PROCESSING PIPELINES:
   - Multi-stage processing with actors and tasks
   - Load balancing across components
   - Error handling and validation
   - Performance optimization

✅ DISTRIBUTED MACHINE LEARNING:
   - Parameter server pattern
   - Worker coordination
   - Gradient synchronization
   - Training monitoring

✅ REAL-TIME SYSTEMS:
   - High-frequency data processing
   - Alert detection and management
   - Scalable metric collection
   - System monitoring

✅ PERFORMANCE DEBUGGING:
   - Identifying bottlenecks
   - Optimizing task patterns
   - Actor efficiency
   - Resource utilization

🎯 TECHNICAL SKILLS:

✅ PROBLEM ANALYSIS:
   - Understand requirements clearly
   - Identify key constraints
   - Choose appropriate patterns
   - Consider scalability

✅ IMPLEMENTATION:
   - Write clean, correct Ray code
   - Handle errors gracefully
   - Use best practices
   - Optimize for performance

✅ DEBUGGING & OPTIMIZATION:
   - Diagnose performance issues
   - Use Ray tools effectively
   - Apply systematic debugging
   - Measure improvements

✅ COMMUNICATION:
   - Explain design decisions
   - Discuss trade-offs
   - Walk through code clearly
   - Answer follow-up questions

🎯 COMMON QUESTIONS - BE READY:

1. "Design a distributed data processing system"
   → Multi-stage pipeline with actors and tasks

2. "How would you implement distributed ML training?"
   → Parameter server pattern with worker coordination

3. "Build a real-time monitoring system"
   → Actor-based metric collection with alerting

4. "Your Ray app is slow - debug and fix it"
   → Profile, identify bottlenecks, optimize patterns

5. "When do you use actors vs tasks?"
   → State management vs stateless computation

6. "How do you handle failures in Ray?"
   → Error handling, retries, graceful degradation

7. "How do you optimize Ray performance?"
   → Avoid sequential ray.get(), batch operations, monitor resources

8. "How do you debug distributed Ray applications?"
   → Dashboard, logging, monitoring, systematic approach

🎯 FINAL TIPS:

✅ Practice implementing from scratch
✅ Understand the "why" behind each pattern
✅ Be ready to optimize and debug
✅ Explain your thought process clearly
✅ Ask clarifying questions
✅ Consider edge cases and failure modes

""" 