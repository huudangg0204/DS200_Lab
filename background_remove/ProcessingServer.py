#!/usr/bin/env python3
"""
Processing Server - FIX SOCKET ERROR (Save-in-Worker)
"""

import os
import sys
import json
import base64
import argparse
from typing import Iterator
from datetime import datetime

# --- 1. CONFIG ENV ---
os.environ["OMP_NUM_THREADS"] = "1"
os.environ["MKL_NUM_THREADS"] = "1"
os.environ["OPENBLAS_NUM_THREADS"] = "1"

#os.environ['HADOOP_HOME'] = r"D:\hadoop"
#hadoop_bin = os.path.join(os.environ['HADOOP_HOME'], 'bin')
#os.environ['PATH'] += os.pathsep + hadoop_bin

current_python = sys.executable 
os.environ['PYSPARK_PYTHON'] = current_python
os.environ['PYSPARK_DRIVER_PYTHON'] = current_python

# Xác định đường dẫn Model (Tuyệt đối)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MODEL_ABS_PATH = os.path.join(BASE_DIR, "selfie_segmenter.tflite")

import cv2
import numpy as np
import mediapipe as mp
from mediapipe.tasks import python
from mediapipe.tasks.python import vision
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType

# --- 2. LOGIC xử lý và lưu luôn (chỉ gửi thông báo sang driver) để tránh tràn ram
def simple_background_removal(frame_bgr: np.ndarray) -> np.ndarray:
    try:
        hsv = cv2.cvtColor(frame_bgr, cv2.COLOR_BGR2HSV)
        lower_green = np.array([35, 40, 40])
        upper_green = np.array([90, 255, 255])
        mask = cv2.inRange(hsv, lower_green, upper_green)
        
        kernel = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (5, 5))
        mask = cv2.morphologyEx(mask, cv2.MORPH_CLOSE, kernel)
        mask = cv2.morphologyEx(mask, cv2.MORPH_OPEN, kernel)
        mask = cv2.GaussianBlur(mask, (5, 5), 0)
        
        bgra = cv2.cvtColor(frame_bgr, cv2.COLOR_BGR2BGRA)
        bgra[:, :, 3] = cv2.bitwise_not(mask)
        return bgra
    except:
        return cv2.cvtColor(frame_bgr, cv2.COLOR_BGR2BGRA)


# 
# Tự lưu ảnh và chỉ trả về trạng thái (String/Int).
def process_and_save_partition(iterator):
    segmenter = None
    has_model = False
    
    try:
        if os.path.exists(MODEL_ABS_PATH):
            base_options = python.BaseOptions(model_asset_path=MODEL_ABS_PATH)
            options = vision.ImageSegmenterOptions(
                base_options=base_options, 
                output_category_mask=True
            )
            segmenter = vision.ImageSegmenter.create_from_options(options)
            has_model = True
        else:
            print(f"❌ [Worker] Model not found at {MODEL_ABS_PATH}")
    except Exception as e:
        print(f"❌ [Worker] Init error: {e}")

    count_success = 0

    for row in iterator:
        try:
            # 1. Deserialize
            frame_b64 = row['frame_data']
            camera_id = row['camera_id']
            frame_number = row['frame_number']
            
            OUTPUT_ROOT = os.path.join(BASE_DIR, "output", "processed_frames")
            if not os.path.exists(OUTPUT_ROOT):
                os.makedirs(OUTPUT_ROOT, exist_ok=True)

            frame_bytes = base64.b64decode(frame_b64)
            np_array = np.frombuffer(frame_bytes, np.uint8)
            frame_bgr = cv2.imdecode(np_array, cv2.IMREAD_COLOR)

            if frame_bgr is None: continue

            # 2. Process
            frame_bgra = None
            if has_model and segmenter:
                try:
                    frame_rgb = cv2.cvtColor(frame_bgr, cv2.COLOR_BGR2RGB)
                    mp_image = mp.Image(image_format=mp.ImageFormat.SRGB, data=frame_rgb)
                    segmentation_result = segmenter.segment(mp_image)
                    category_mask = segmentation_result.category_mask
                    condition = category_mask.numpy_view() > 0.1
                    
                    b, g, r = cv2.split(frame_bgr)
                    alpha = np.where(condition, 0, 255).astype(np.uint8)
                    frame_bgra = cv2.merge((b, g, r, alpha))
                except Exception:
                    frame_bgra = simple_background_removal(frame_bgr)
            else:
                frame_bgra = simple_background_removal(frame_bgr)

            # 3. SAVE IMMEDIATELY (Không trả về Driver)
            filename = os.path.join(OUTPUT_ROOT, f"{camera_id}_{frame_number:06d}.png")
            cv2.imwrite(filename, frame_bgra)
            
            count_success += 1

        except Exception as e:
            # Print lỗi ngay tại Worker để debug (hiện ở console chạy script)
            print(f"❌ Error frame {row.get('frame_number', '?')}: {e}")
            # Không tăng count_success
            continue

    if segmenter: segmenter.close()
    
    return iter([1] * count_success)

# --- 3. CLASS DRIVER ---

class ProcessingServer:
    def __init__(self, kafka_brokers, topic, output_dir, checkpoint_dir):
        self.kafka_brokers = kafka_brokers
        self.topic = topic
        self.output_dir = os.path.abspath(output_dir)
        self.checkpoint_dir = os.path.abspath(checkpoint_dir)
        
        # Tạo thư mục output trước (cho Driver)
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Config Spark
        self.spark = SparkSession.builder \
            .appName("ImageBGRemoval") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
            .config("spark.sql.streaming.schemaInference", "true") \
            .config("spark.driver.memory", "1g") \
            .config("spark.executor.memory", "2g") \
            .getOrCreate()
            
        self.spark.sparkContext.setLogLevel("WARN")

    @staticmethod
    def define_schema():
        return StructType([
            StructField("camera_id", StringType(), True),
            StructField("timestamp_ms", LongType(), True),
            StructField("frame_number", IntegerType(), True),
            StructField("frame_data", StringType(), True)
        ])

    def run_streaming_job(self, batch_interval_seconds=5):
        print(f"\n[ProcessingServer] Job Started. Output: {self.output_dir}")
        
        df_raw = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_brokers) \
            .option("subscribe", self.topic) \
            .option("startingOffsets", "latest") \
            .option("maxOffsetsPerTrigger", 20) \
            .load() # Giảm batch size xuống 20 để test ổn định

        df_parsed = df_raw.select(
            from_json(col("value").cast("string"), self.define_schema()).alias("data")
        ).select("data.*")

        def process_batch_wrapper(batch_df, batch_id):
            if batch_df.isEmpty(): return
            
            print(f"Processing Batch {batch_id}...")
        
            result_rdd = batch_df.rdd.mapPartitions(process_and_save_partition)
            try:
                count = result_rdd.count()
                print(f"✅ Batch {batch_id} finished. Processed {count} items.")
            except Exception as e:
                print(f"❌ Batch {batch_id} failed: {e}")

        query = df_parsed.writeStream \
            .foreachBatch(process_batch_wrapper) \
            .option("checkpointLocation", f"{self.checkpoint_dir}/processing") \
            .trigger(processingTime=f"{batch_interval_seconds} seconds") \
            .start()
            
        query.awaitTermination()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--kafka-brokers', default='localhost:9095') # Khớp với CameraServer
    parser.add_argument('--topic', default='image-frames-topic')
    parser.add_argument('--output', default='./output/processed_frames')
    parser.add_argument('--checkpoint', default='./tmp/spark-checkpoint')
    
    args = parser.parse_args()
    
    server = ProcessingServer(
        kafka_brokers=args.kafka_brokers,
        topic=args.topic,
        output_dir=args.output,
        checkpoint_dir=args.checkpoint
    )
    server.run_streaming_job()

if __name__ == "__main__":
    main()