#!/usr/bin/env python3

import cv2
import json
import base64
import time
import argparse
from datetime import datetime
from typing import Optional, Tuple
from kafka import KafkaProducer
from kafka.errors import KafkaError


class CameraServer:
    """
    Simulates a video camera streaming frames to Kafka broker.
    
    """
    
    def __init__(self,
                 video_source: str,
                 camera_id: str,
                 kafka_brokers: str = "localhost:9092",
                 topic: str = "image-frames-topic",
                 frame_rate: int = 24,
                 quality: int = 85):
       
        
        self.video_source = video_source
        self.camera_id = camera_id
        self.kafka_brokers = kafka_brokers
        self.topic = topic
        self.frame_rate = frame_rate
        self.quality = quality
        self.frame_count = 0
        
        # Initialize Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_brokers.split(','),
            # ép version
            api_version=(0, 10, 1),
            compression_type='gzip',
            batch_size=16384,
            linger_ms=10,
            acks='all',  # Wait for all replicas
            retries=3
        )
        
        print(f"[{self.camera_id}] Initialized Camera Server")
        print(f"  Video source: {video_source}")
        print(f"  Kafka brokers: {kafka_brokers}")
        print(f"  Target FPS: {frame_rate}")
    
    def capture_frames(self) -> None:
        """
        Main method: captures frames and streams to Kafka.
        Handles both video files and live camera input.
        """
        cap = cv2.VideoCapture(self.video_source)
        
        if not cap.isOpened():
            raise RuntimeError(f"Cannot open video source: {self.video_source}")
        
        # Get video properties
        fps = cap.get(cv2.CAP_PROP_FPS)
        width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        
        print(f"[{self.camera_id}] Video properties: {width}x{height} @ {fps:.1f} FPS")
        
        frame_interval = 1.0 / self.frame_rate
        frame_sent = 0
        frame_failed = 0
        start_time = time.time()
        
        try:
            while True:
                ret, frame = cap.read()
                
                if not ret:
                    print(f"[{self.camera_id}] End of video stream reached")
                    break
                
                try:
                    # Encode frame to JPEG for transmission
                    ret_encode, encoded_frame = cv2.imencode(
                        '.jpg',
                        frame,
                        [cv2.IMWRITE_JPEG_QUALITY, self.quality]
                    )
                    
                    if not ret_encode:
                        frame_failed += 1
                        continue
                    
                    # Create frame packet
                    packet = self._create_packet(
                        frame_number=self.frame_count,
                        frame_data=encoded_frame.tobytes(),
                        width=width,
                        height=height
                    )
                    
                    # Serialize and send to Kafka
                    self._send_to_kafka(packet)
                    
                    frame_sent += 1
                    self.frame_count += 1
                    
                    # Log progress
                    if frame_sent % 30 == 0:
                        elapsed = time.time() - start_time
                        actual_fps = frame_sent / elapsed
                        print(f"[{self.camera_id}] Sent {frame_sent} frames "
                              f"({actual_fps:.1f} FPS, {frame_failed} failed)")
                    
                    # Maintain target frame rate
                    time.sleep(frame_interval)
                
                except Exception as e:
                    print(f"[{self.camera_id}] Frame {self.frame_count} error: {e}")
                    frame_failed += 1
        
        finally:
            cap.release()
            self.producer.flush()
            self.producer.close()
            
            elapsed = time.time() - start_time
            print(f"\n[{self.camera_id}] Streaming completed:")
            print(f"  Sent: {frame_sent} frames")
            print(f"  Failed: {frame_failed} frames")
            print(f"  Duration: {elapsed:.1f} seconds")
            print(f"  Average FPS: {frame_sent / elapsed:.1f}")
    
    def _create_packet(self,
                      frame_number: int,
                      frame_data: bytes,
                      width: int,
                      height: int) -> dict:
        """
        Create frame packet with metadata.
        
        Args:
            frame_number: Sequential frame index
            frame_data: JPEG-encoded frame bytes
            width: Frame width
            height: Frame height
        
        Returns:
            Dictionary containing packet data
        """
        packet = {
            'camera_id': self.camera_id,
            'timestamp_ms': int(time.time() * 1000),
            'frame_number': frame_number,
            'width': width,
            'height': height,
            'encoding': 'JPEG',
            'frame_data': base64.b64encode(frame_data).decode('utf-8'),
            'quality': self.quality,
            'metadata': {
                'source': 'camera_simulation',
                'created_at': datetime.now().isoformat()
            }
        }
        return packet
    
    def _send_to_kafka(self, packet: dict) -> None:
        """
        Send frame packet to Kafka broker.
        
        Args:
            packet: Frame packet dictionary
        
        Raises:
            KafkaError: On Kafka communication failure
        """
        try:
            # Serialize packet to JSON
            value = json.dumps(packet).encode('utf-8')
            
            # Send with camera_id as key (ensures ordering & partitioning)
            future = self.producer.send(
                self.topic,
                key=self.camera_id.encode('utf-8'),
                value=value
            )
            
            # Non-blocking send with optional callback
            future.get(timeout=10)
        
        except KafkaError as e:
            print(f"[{self.camera_id}] Kafka send error: {e}")
            raise


def main():
    """Entry point for camera server simulation."""
    parser = argparse.ArgumentParser(
        description='Camera Server - Stream video frames to Kafka'
    )
    parser.add_argument(
        '--video',
        type=str,
        default='test_video.mp4',
        help='Path to video file or camera index (default: test_video.mp4)'
    )
    parser.add_argument(
        '--camera-id',
        type=str,
        default='CAM_001',
        help='Unique camera identifier (default: CAM_001)'
    )
    parser.add_argument(
        '--kafka-brokers',
        type=str,
        default='localhost:9095',
        help='Kafka broker addresses (default: localhost:9092)'
    )
    parser.add_argument(
        '--fps',
        type=int,
        default=24,
        help='Target frames per second (default: 24)'
    )
    parser.add_argument(
        '--quality',
        type=int,
        default=85,
        help='JPEG compression quality 0-100 (default: 85)'
    )
    
    args = parser.parse_args()
    
    # Handle camera index (e.g., "0" for default camera)
    video_source = args.video
    if video_source.isdigit():
        video_source = int(video_source)
    
    # Create and run camera server
    camera = CameraServer(
        video_source=video_source,
        camera_id=args.camera_id,
        kafka_brokers=args.kafka_brokers,
        frame_rate=args.fps,
        quality=args.quality
    )
    
    try:
        camera.capture_frames()
    except KeyboardInterrupt:
        print(f"\n[{camera.camera_id}] Interrupted by user")
    except Exception as e:
        print(f"\n[{camera.camera_id}] Fatal error: {e}")
        raise


if __name__ == "__main__":
    main()
