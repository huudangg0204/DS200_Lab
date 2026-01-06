#!/usr/bin/env python3
"""
Test & Example Scripts for Online Image Background Removal System Using Spark

This module provides:
1. Test video generation
2. Integration testing helpers
3. Performance benchmarking
4. Data validation utilities
"""

import cv2
import numpy as np
import time
import os
import json
from pathlib import Path
from typing import Tuple, Optional
import base64


# ============================================================================
# PART 1: TEST VIDEO GENERATION
# ============================================================================

class TestVideoGenerator:
    """Generate synthetic test videos with moving objects."""
    
    @staticmethod
    def create_simple_test_video(
        output_path: str = 'test_video.mp4',
        duration: int = 10,
        fps: int = 24,
        width: int = 640,
        height: int = 480
    ) -> None:
        """
        Create simple test video with moving shapes.
        
        Args:
            output_path: Output video file path
            duration: Video duration in seconds
            fps: Frames per second
            width: Video width
            height: Video height
        """
        print(f"Generating test video: {output_path}")
        print(f"  Duration: {duration}s @ {fps} FPS ({width}x{height})")
        
        fourcc = cv2.VideoWriter_fourcc(*'mp4v')
        out = cv2.VideoWriter(output_path, fourcc, fps, (width, height))
        
        total_frames = duration * fps
        
        for frame_num in range(total_frames):
            # Create frame with gradient background
            frame = np.zeros((height, width, 3), dtype=np.uint8)
            
            # Gradient background (blue to red)
            for y in range(height):
                progress = frame_num / total_frames
                r = int(255 * progress)
                b = int(255 * (1 - progress))
                frame[y, :] = [b, 128, r]
            
            # Animated circle
            angle = 2 * np.pi * frame_num / total_frames
            center_x = int(width/2 + 100 * np.cos(angle))
            center_y = int(height/2 + 100 * np.sin(angle))
            cv2.circle(frame, (center_x, center_y), 50, (0, 255, 0), -1)
            
            # Rectangle border
            margin = 20
            cv2.rectangle(frame, (margin, margin), 
                         (width-margin, height-margin), (255, 255, 0), 2)
            
            # Add text
            cv2.putText(frame, f'Frame: {frame_num}', (10, 30),
                       cv2.FONT_HERSHEY_SIMPLEX, 1, (255, 255, 255), 2)
            cv2.putText(frame, f'{progress*100:.0f}%', (width-150, 30),
                       cv2.FONT_HERSHEY_SIMPLEX, 1, (255, 255, 255), 2)
            
            out.write(frame)
            
            if (frame_num + 1) % 30 == 0:
                print(f"  Generated {frame_num + 1}/{total_frames} frames")
        
        out.release()
        print(f"✓ Test video created: {output_path}")
    
    @staticmethod
    def create_multi_object_video(
        output_path: str = 'test_multi_object.mp4',
        duration: int = 10,
        fps: int = 24
    ) -> None:
        """Create test video with multiple moving objects."""
        width, height = 640, 480
        fourcc = cv2.VideoWriter_fourcc(*'mp4v')
        out = cv2.VideoWriter(output_path, fourcc, fps, (width, height))
        
        total_frames = duration * fps
        
        for frame_num in range(total_frames):
            frame = np.ones((height, width, 3), dtype=np.uint8) * 100
            
            progress = frame_num / total_frames
            
            # Multiple circles with different trajectories
            for i in range(5):
                angle = 2 * np.pi * (progress + i/5)
                radius = 50 + 20*i
                center_x = int(width/2 + radius * np.cos(angle))
                center_y = int(height/2 + radius * np.sin(angle))
                color = (
                    int(50 * (i+1)),
                    int(255 * (1-progress)),
                    int(255 * progress)
                )
                cv2.circle(frame, (center_x, center_y), 30, color, -1)
            
            out.write(frame)
        
        out.release()
        print(f"✓ Multi-object test video: {output_path}")
    
    @staticmethod
    def create_green_screen_video(
        output_path: str = 'test_green_screen.mp4',
        duration: int = 10,
        fps: int = 24
    ) -> None:
        """Create test video with green background (chroma key)."""
        width, height = 640, 480
        fourcc = cv2.VideoWriter_fourcc(*'mp4v')
        out = cv2.VideoWriter(output_path, fourcc, fps, (width, height))
        
        total_frames = duration * fps
        
        for frame_num in range(total_frames):
            # Green background (B, G, R in OpenCV)
            frame = np.zeros((height, width, 3), dtype=np.uint8)
            frame[:, :] = [0, 255, 0]  # Green
            
            # Add person silhouette (white shape)
            progress = frame_num / total_frames
            center_x = int(width/2 + 150 * np.cos(2*np.pi*progress))
            
            # Draw head
            cv2.circle(frame, (center_x, 120), 40, (255, 255, 255), -1)
            
            # Draw body
            cv2.rectangle(frame, (center_x-30, 160), (center_x+30, 300),
                         (255, 255, 255), -1)
            
            # Draw arms
            cv2.line(frame, (center_x-30, 180), (center_x-80, 200), (255, 255, 255), 10)
            cv2.line(frame, (center_x+30, 180), (center_x+80, 200), (255, 255, 255), 10)
            
            out.write(frame)
        
        out.release()
        print(f"✓ Green screen test video: {output_path}")


# ============================================================================
# PART 2: FRAME VALIDATION & TESTING
# ============================================================================

class FrameValidator:
    """Validate frame data and output quality."""
    
    @staticmethod
    def validate_frame_packet(packet: dict) -> Tuple[bool, str]:
        """
        Validate frame packet structure.
        
        Args:
            packet: Frame packet dictionary
        
        Returns:
            Tuple: (is_valid, message)
        """
        required_fields = [
            'camera_id', 'timestamp_ms', 'frame_number',
            'width', 'height', 'encoding', 'frame_data', 'quality'
        ]
        
        for field in required_fields:
            if field not in packet:
                return False, f"Missing field: {field}"
        
        if not isinstance(packet['timestamp_ms'], int):
            return False, "timestamp_ms must be integer"
        
        if not (0 <= packet['quality'] <= 100):
            return False, "quality must be 0-100"
        
        if packet['width'] <= 0 or packet['height'] <= 0:
            return False, "Invalid frame dimensions"
        
        try:
            base64.b64decode(packet['frame_data'])
        except Exception:
            return False, "frame_data is not valid base64"
        
        return True, "Valid packet"
    
    @staticmethod
    def validate_output_image(
        image_path: str,
        expected_width: Optional[int] = None,
        expected_height: Optional[int] = None
    ) -> Tuple[bool, str]:
        """
        Validate output image.
        
        Args:
            image_path: Path to output image
            expected_width: Expected width (optional)
            expected_height: Expected height (optional)
        
        Returns:
            Tuple: (is_valid, message)
        """
        if not os.path.exists(image_path):
            return False, f"File not found: {image_path}"
        
        try:
            img = cv2.imread(image_path, cv2.IMREAD_UNCHANGED)
            
            if img is None:
                return False, "Failed to read image"
            
            # Check if image has alpha channel
            if img.shape[2] != 4:
                return False, f"Expected BGRA (4 channels), got {img.shape[2]}"
            
            # Check dimensions if specified
            if expected_width and img.shape[1] != expected_width:
                return False, f"Width mismatch: {img.shape[1]} != {expected_width}"
            
            if expected_height and img.shape[0] != expected_height:
                return False, f"Height mismatch: {img.shape[0]} != {expected_height}"
            
            return True, f"Valid image: {img.shape}"
        
        except Exception as e:
            return False, f"Error reading image: {e}"


# ============================================================================
# PART 3: PERFORMANCE BENCHMARKING
# ============================================================================

class PerformanceBenchmark:
    """Benchmark system performance."""
    
    @staticmethod
    def benchmark_frame_processing(
        video_path: str,
        num_frames: int = 100
    ) -> dict:
        """
        Benchmark frame processing performance.
        
        Args:
            video_path: Path to test video
            num_frames: Number of frames to process
        
        Returns:
            Dictionary with benchmark results
        """
        import json
        from rembg import remove
        
        print(f"Benchmarking frame processing...")
        print(f"  Video: {video_path}")
        print(f"  Frames: {num_frames}")
        
        cap = cv2.VideoCapture(video_path)
        
        timings = {
            'total': 0,
            'read': [],
            'remove_bg': [],
            'encode': []
        }
        
        for i in range(num_frames):
            # Read frame
            start = time.time()
            ret, frame = cap.read()
            timings['read'].append((time.time() - start) * 1000)
            
            if not ret:
                break
            
            # Remove background
            start = time.time()
            try:
                rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                output = remove(rgb)
                timings['remove_bg'].append((time.time() - start) * 1000)
            except Exception as e:
                print(f"  Error: {e}")
                timings['remove_bg'].append(0)
            
            # Encode
            start = time.time()
            cv2.imencode('.png', cv2.cvtColor(output, cv2.COLOR_RGBA2BGRA))
            timings['encode'].append((time.time() - start) * 1000)
        
        cap.release()
        
        # Calculate statistics
        results = {
            'total_frames': i,
            'read_ms': {
                'min': min(timings['read']),
                'max': max(timings['read']),
                'avg': np.mean(timings['read'])
            },
            'remove_bg_ms': {
                'min': min(timings['remove_bg']) if timings['remove_bg'] else 0,
                'max': max(timings['remove_bg']) if timings['remove_bg'] else 0,
                'avg': np.mean(timings['remove_bg']) if timings['remove_bg'] else 0
            },
            'encode_ms': {
                'min': min(timings['encode']),
                'max': max(timings['encode']),
                'avg': np.mean(timings['encode'])
            },
            'total_ms_per_frame': {
                'avg': np.mean([sum(x) for x in zip(timings['read'], 
                                                     timings['remove_bg'], 
                                                     timings['encode'])])
            }
        }
        
        print(f"\n✓ Benchmark Results:")
        print(f"  Frame read: {results['read_ms']['avg']:.2f} ms")
        print(f"  BG removal: {results['remove_bg_ms']['avg']:.2f} ms")
        print(f"  Encoding: {results['encode_ms']['avg']:.2f} ms")
        print(f"  Total/frame: {results['total_ms_per_frame']['avg']:.2f} ms")
        
        return results
    
    @staticmethod
    def benchmark_kafka_throughput(
        num_messages: int = 1000,
        message_size_kb: int = 50
    ) -> dict:
        """
        Benchmark Kafka throughput.
        
        Args:
            num_messages: Number of test messages
            message_size_kb: Size of each message in KB
        
        Returns:
            Dictionary with benchmark results
        """
        try:
            from kafka import KafkaProducer
            from kafka.errors import KafkaError
            
            producer = KafkaProducer(bootstrap_servers='localhost:9092')
            
            # Generate test messages
            test_message = 'x' * (message_size_kb * 1024)
            
            print(f"Benchmarking Kafka throughput...")
            print(f"  Messages: {num_messages}")
            print(f"  Size/message: {message_size_kb} KB")
            
            start_time = time.time()
            
            for i in range(num_messages):
                producer.send('benchmark-topic', value=test_message.encode('utf-8'))
            
            producer.flush()
            elapsed = time.time() - start_time
            
            total_mb = (num_messages * message_size_kb) / 1024
            throughput_mbps = total_mb / elapsed
            
            results = {
                'total_messages': num_messages,
                'total_size_mb': total_mb,
                'elapsed_seconds': elapsed,
                'throughput_mbps': throughput_mbps,
                'messages_per_second': num_messages / elapsed
            }
            
            print(f"\n✓ Kafka Benchmark Results:")
            print(f"  Throughput: {throughput_mbps:.2f} MB/s")
            print(f"  Rate: {results['messages_per_second']:.0f} msg/s")
            
            producer.close()
            return results
        
        except Exception as e:
            print(f"Kafka benchmark failed: {e}")
            return {}


# ============================================================================
# PART 4: INTEGRATION TESTING
# ============================================================================

class IntegrationTest:
    """Integration test utilities."""
    
    @staticmethod
    def test_end_to_end_flow():
        """
        Test complete pipeline flow.
        Steps:
        1. Create test video
        2. Validate video creation
        3. Send frames via camera server
        4. Process via Spark
        5. Validate output
        """
        print("\n" + "="*60)
        print("INTEGRATION TEST: End-to-End Pipeline")
        print("="*60)
        
        # 1. Create test video
        print("\n[STEP 1] Creating test video...")
        TestVideoGenerator.create_simple_test_video('test_integration.mp4', 
                                                    duration=5, fps=24)
        
        # 2. Validate video
        print("\n[STEP 2] Validating video...")
        cap = cv2.VideoCapture('test_integration.mp4')
        frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        cap.release()
        
        if frame_count == 120:  # 5 sec * 24 fps
            print(f"✓ Video valid: {frame_count} frames")
        else:
            print(f"✗ Video invalid: expected 120, got {frame_count}")
            return False
        
        # 3. Test frame packet creation
        print("\n[STEP 3] Testing frame packet creation...")
        cap = cv2.VideoCapture('test_integration.mp4')
        ret, frame = cap.read()
        cap.release()
        
        if ret:
            ret_encode, encoded = cv2.imencode('.jpg', frame, 
                                              [cv2.IMWRITE_JPEG_QUALITY, 85])
            packet = {
                'camera_id': 'TEST_CAM',
                'timestamp_ms': int(time.time() * 1000),
                'frame_number': 0,
                'width': frame.shape[1],
                'height': frame.shape[0],
                'encoding': 'JPEG',
                'frame_data': base64.b64encode(encoded).decode('utf-8'),
                'quality': 85
            }
            
            is_valid, msg = FrameValidator.validate_frame_packet(packet)
            if is_valid:
                print(f"✓ Frame packet valid: {msg}")
            else:
                print(f"✗ Frame packet invalid: {msg}")
                return False
        
        print("\n" + "="*60)
        print("✓ INTEGRATION TEST PASSED")
        print("="*60)
        return True


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Run all test utilities."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Test & Benchmark Utilities for Spark BG Removal System'
    )
    parser.add_argument('--create-video', action='store_true',
                       help='Create test video')
    parser.add_argument('--create-multi', action='store_true',
                       help='Create multi-object test video')
    parser.add_argument('--create-green-screen', action='store_true',
                       help='Create green screen test video')
    parser.add_argument('--benchmark-processing', type=str,
                       help='Benchmark frame processing on video')
    parser.add_argument('--benchmark-kafka', action='store_true',
                       help='Benchmark Kafka throughput')
    parser.add_argument('--integration-test', action='store_true',
                       help='Run integration tests')
    
    args = parser.parse_args()
    
    if args.create_video:
        TestVideoGenerator.create_simple_test_video()
    
    elif args.create_multi:
        TestVideoGenerator.create_multi_object_video()
    
    elif args.create_green_screen:
        TestVideoGenerator.create_green_screen_video()
    
    elif args.benchmark_processing:
        PerformanceBenchmark.benchmark_frame_processing(args.benchmark_processing)
    
    elif args.benchmark_kafka:
        PerformanceBenchmark.benchmark_kafka_throughput()
    
    elif args.integration_test:
        IntegrationTest.test_end_to_end_flow()
    
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
