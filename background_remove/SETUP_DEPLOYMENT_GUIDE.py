#!/usr/bin/env python3
"""
System Setup & Deployment Guide
Online Image Background Removal System Using Apache Spark

This guide covers:
1. Environment setup and dependencies
2. Kafka cluster initialization
3. Running camera and processing servers
4. Monitoring and testing
5. Troubleshooting
"""

# ============================================================================
# PART 1: DEPENDENCIES & ENVIRONMENT SETUP
# ============================================================================

"""
Python Version: 3.8+
Operating System: Linux/Windows/MacOS

1.1 Create Virtual Environment
"""

# Create venv
# python -m venv spark_bg_removal_env
# source spark_bg_removal_env/bin/activate  # Linux/Mac
# spark_bg_removal_env\Scripts\activate     # Windows

"""
1.2 Install Required Packages
"""

# pip install --upgrade pip

# Core packages
CORE_PACKAGES = [
    "pyspark==3.5.0",           # Apache Spark
    "kafka-python==2.0.2",      # Kafka consumer/producer
    "opencv-python==4.8.1.78",  # Computer vision
    "numpy==1.24.3",            # Numerical computing
    "Pillow==10.0.0",           # Image processing
]

# Background removal
BG_REMOVAL_PACKAGES = [
    "rembg[gpu]==0.0.54",       # RemBG with GPU support
    "torch==2.0.1",             # PyTorch
    "torchvision==0.15.2",      # Vision models
    "onnx==1.14.1",             # ONNX runtime
    "onnxruntime-gpu==1.16.2",  # GPU acceleration (optional)
]

# Monitoring & utilities
UTILITY_PACKAGES = [
    "prometheus-client==0.18.0", # Metrics
    "tqdm==4.66.1",             # Progress bars
    "pyyaml==6.0.1",            # Config files
    "click==8.1.7",             # CLI builder
]

"""
Installation command:
pip install pyspark kafka-python opencv-python numpy Pillow rembg[gpu] torch
"""

# ============================================================================
# PART 2: KAFKA CLUSTER SETUP
# ============================================================================

"""
2.1 Using Docker Compose (Recommended)

Create docker-compose.yml:
"""

DOCKER_COMPOSE_YAML = """
version: '3.8'

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - "2181:2181"

  kafka-1:
    image: confluentinc/cp-kafka:7.5.0
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka-1:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 2
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3

  kafka-2:
    image: confluentinc/cp-kafka:7.5.0
    depends_on:
      - zookeeper
    ports:
      - "9093:9093"
    environment:
      KAFKA_BROKER_ID: 2
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka-2:29093,PLAINTEXT_HOST://localhost:9093
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 2
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3

  kafka-3:
    image: confluentinc/cp-kafka:7.5.0
    depends_on:
      - zookeeper
    ports:
      - "9094:9094"
    environment:
      KAFKA_BROKER_ID: 3
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka-3:29094,PLAINTEXT_HOST://localhost:9094
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 2
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    depends_on:
      - kafka-1
      - kafka-2
      - kafka-3
    ports:
      - "8080:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka-1:29092,kafka-2:29093,kafka-3:29094
      KAFKA_CLUSTERS_0_ZOOKEEPER: zookeeper:2181

volumes:
  zookeeper-data:
  kafka-data:
"""

"""
2.2 Start Kafka Cluster

# Start Docker Compose
docker-compose -f docker-compose.yml up -d

# Verify services
docker-compose ps

# View Kafka UI: http://localhost:8080

# Stop services
docker-compose down
"""

"""
2.3 Create Kafka Topic

# Inside kafka-1 container
docker exec kafka-1 kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic image-frames-topic \
  --partitions 4 \
  --replication-factor 3 \
  --config retention.ms=86400000

# Verify topic
docker exec kafka-1 kafka-topics --list \
  --bootstrap-server localhost:9092

# Describe topic
docker exec kafka-1 kafka-topics --describe \
  --bootstrap-server localhost:9092 \
  --topic image-frames-topic
"""

# ============================================================================
# PART 3: RUNNING THE SYSTEM
# ============================================================================

"""
3.1 Prepare Test Video

# Create a simple test video using Python
"""

PREPARE_VIDEO_SCRIPT = """
import cv2
import numpy as np

def create_test_video(output_path='test_video.mp4', duration=10, fps=24):
    '''Create synthetic test video with colored shapes and text.'''
    width, height = 640, 480
    fourcc = cv2.VideoWriter_fourcc(*'mp4v')
    out = cv2.VideoWriter(output_path, fourcc, fps, (width, height))
    
    total_frames = duration * fps
    for frame_num in range(total_frames):
        frame = np.zeros((height, width, 3), dtype=np.uint8)
        
        # Draw gradient background
        for y in range(height):
            frame[y, :] = [
                int(255 * frame_num / total_frames),
                int(128),
                int(255 * (1 - frame_num / total_frames))
            ]
        
        # Draw animated shapes
        center_x = int(width/2 + 100 * np.cos(2*np.pi*frame_num/total_frames))
        center_y = int(height/2 + 100 * np.sin(2*np.pi*frame_num/total_frames))
        
        cv2.circle(frame, (center_x, center_y), 50, (0, 255, 0), -1)
        cv2.rectangle(frame, (50, 50), (width-50, height-50), (255, 0, 0), 2)
        
        # Add text
        cv2.putText(frame, f'Frame: {frame_num}', (10, 30),
                   cv2.FONT_HERSHEY_SIMPLEX, 1, (255, 255, 255), 2)
        
        out.write(frame)
    
    out.release()
    print(f'Test video created: {output_path}')

create_test_video()
"""

"""
# Run preparation script
python -c "..." < prepare_video.py
"""

"""
3.2 Start Camera Server

Terminal 1: Run camera simulation

python CameraServer.py \\
    --video test_video.mp4 \\
    --camera-id CAM_001 \\
    --kafka-brokers localhost:9092 \\
    --fps 24 \\
    --quality 85

Expected output:
  [CAM_001] Initialized Camera Server
  [CAM_001] Video properties: 640x480 @ 24.0 FPS
  [CAM_001] Sent 30 frames (24.0 FPS, 0 failed)
  ...
"""

"""
3.3 Start Processing Server

Terminal 2: Run background removal processing

python ProcessingServer.py \\
    --kafka-brokers localhost:9092 \\
    --topic image-frames-topic \\
    --output /output/processed_frames \\
    --checkpoint /tmp/spark-checkpoint \\
    --batch-interval 5

Expected output:
  [ProcessingServer] Initialized Spark Session
  [ProcessingServer] Starting streaming job...
  === Processing Batch 0 (120 frames) ===
    Completed: 120/120 frames successful
  ...
"""

"""
3.4 Monitor Output

Terminal 3: Check processed frames

# Watch output directory
watch -n 1 'ls -lh /output/processed_frames | head -20'

# Count frames
find /output/processed_frames -name "*.png" | wc -l

# View sample frame
eog /output/processed_frames/CAM_001_000000.png
"""

# ============================================================================
# PART 4: MONITORING & METRICS
# ============================================================================

"""
4.1 Kafka Consumer Metrics

# Monitor consumer lag
docker exec kafka-1 kafka-consumer-groups \\
  --bootstrap-server localhost:9092 \\
  --group spark-consumer \\
  --describe

# Expected output shows current offset vs. end offset
"""

"""
4.2 Spark Streaming UI

# Access Spark UI while job is running
http://localhost:4040

# Shows:
- Active batches and processing times
- Input rates and processing rates
- Executor metrics
- Task performance
"""

"""
4.3 Custom Metrics Collection

from prometheus_client import Counter, Histogram, start_http_server

frames_processed = Counter('frames_processed_total', 'Total frames processed')
processing_duration = Histogram('frame_processing_seconds', 'Frame processing duration')

# Usage in processing:
with processing_duration.time():
    # Process frame
    pass

frames_processed.inc()

# Start metrics server
start_http_server(8000)  # Access at http://localhost:8000
"""

# ============================================================================
# PART 5: SCALING & PRODUCTION DEPLOYMENT
# ============================================================================

"""
5.1 Multi-camera Streaming

# Start multiple cameras
python CameraServer.py --camera-id CAM_001 --video video1.mp4 &
python CameraServer.py --camera-id CAM_002 --video video2.mp4 &
python CameraServer.py --camera-id CAM_003 --video video3.mp4 &

# Kafka distributes frames across partitions by camera_id
# Processing server handles all in parallel
"""

"""
5.2 Spark Cluster Mode (YARN)

# Submit to YARN cluster
spark-submit \\
    --master yarn \\
    --deploy-mode cluster \\
    --num-executors 8 \\
    --executor-cores 4 \\
    --executor-memory 8g \\
    --driver-memory 4g \\
    ProcessingServer.py \\
    --kafka-brokers kafka-1:29092,kafka-2:29093,kafka-3:29094
"""

"""
5.3 GPU Acceleration Setup

# Install CUDA 11.8 and cuDNN
# Install GPU-accelerated packages
pip install rembg[gpu] torch torchvision onnxruntime-gpu

# Configure Spark for GPU
spark.config("spark.task.resource.gpu.amount", "1")
spark.config("spark.executor.resource.gpu.amount", "2")
"""

# ============================================================================
# PART 6: TROUBLESHOOTING
# ============================================================================

"""
6.1 Common Issues & Solutions

Issue: Kafka connection refused
Solution:
  - Verify Docker containers: docker-compose ps
  - Check network: docker network ls
  - Restart services: docker-compose restart

Issue: Camera server not sending frames
Solution:
  - Check video file exists and is readable
  - Verify FPS doesn't exceed system capabilities
  - Check Kafka topic was created: docker exec kafka-1 kafka-topics --list --bootstrap-server localhost:9092

Issue: Processing server slow
Solution:
  - Check batch_interval: reduce for lower latency
  - Increase executor cores/memory
  - Enable GPU acceleration for RemBG
  - Check system resources: cpu, memory, disk I/O

Issue: Out of memory
Solution:
  - Reduce batch interval or frame rate
  - Decrease JPEG quality (trade-off with accuracy)
  - Increase executor memory: --executor-memory 16g
  - Reduce output frame resolution

Issue: No frames saved
Solution:
  - Check output directory exists: mkdir -p /output/processed_frames
  - Verify checkpoint directory writable
  - Check disk space: df -h
  - Review logs: tail -f /tmp/spark-logs/*

6.2 Performance Tuning

# Increase parallelism
spark.default.parallelism = 16
spark.sql.shuffle.partitions = 32

# Adjust Kafka consumption
spark.streaming.kafka.maxRatePerPartition = 200

# Enable backpressure
spark.streaming.backpressure.enabled = true

# Optimize serialization
spark.serializer = org.apache.spark.serializer.KryoSerializer
"""

# ============================================================================
# PART 7: TESTING & VALIDATION
# ============================================================================

"""
7.1 Unit Tests for Frame Processing

import unittest
from ProcessingServer import ProcessingServer

class TestBackgroundRemoval(unittest.TestCase):
    def setUp(self):
        self.server = ProcessingServer()
    
    def test_frame_deserialization(self):
        # Create test packet
        test_frame = cv2.imread('test_image.jpg')
        # Test deserialization...
    
    def test_background_removal(self):
        # Test background removal...
        pass
    
    def test_idempotency(self):
        # Process same frame twice, verify identical output
        pass

if __name__ == '__main__':
    unittest.main()
"""

"""
7.2 Integration Testing

# Send test frames
docker exec kafka-1 kafka-console-producer \\
  --broker-list localhost:9092 \\
  --topic image-frames-topic

# Monitor processing
docker logs -f processing-server

# Validate output
ls -la /output/processed_frames/
"""

# ============================================================================
# PART 8: CLEANUP & MAINTENANCE
# ============================================================================

"""
8.1 Stop All Services

# Stop processing server: Ctrl+C in terminal
# Stop camera server: Ctrl+C in terminal

# Stop Kafka cluster
docker-compose down

# Clean checkpoint
rm -rf /tmp/spark-checkpoint/*

# Clean output
rm -rf /output/processed_frames/*
"""

"""
8.2 Maintenance Tasks

# Monitor disk usage
du -sh /output/processed_frames/

# Archive old frames
tar -czf processed_frames_backup_$(date +%Y%m%d).tar.gz \\
    /output/processed_frames/

# Clean Kafka logs (every 24h)
kafka-log-dirs --bootstrap-server localhost:9092 --describe

# Monitor Kafka replication lag
kafka-mirror-maker --consumer.config consumer.properties --producer.config producer.properties
"""

print(__doc__)
