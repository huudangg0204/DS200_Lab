# Hệ Thống Xoá Phông Nền Video Thời Gian Thực

Hệ thống xử lý video theo thời gian thực sử dụng Apache Spark Streaming và Kafka để xoá phông nền.

## 📋 Yêu Cầu Hệ Thống

- **Python:** 3.8 trở lên
- **Docker Desktop:** Để chạy Kafka cluster
- **RAM:** Tối thiểu 8GB khả dụng

## 🚀 Hướng Dẫn Chạy Dự Án

### Bước 1: Cài Đặt Dependencies

```bash
# Cài đặt các thư viện Python cần thiết
pip install -r requirements.txt
```

### Bước 2: Khởi Động Kafka Cluster

```bash
# Khởi động Docker containers (Kafka, Zookeeper)
docker-compose up -d

# Kiểm tra trạng thái containers
docker-compose ps

# Đợi khoảng 30 giây để Kafka khởi động hoàn toàn
```

### Bước 3: Tạo Video Test (Tùy Chọn)

```bash
# Tạo video test đơn giản để thử nghiệm
python -c "
import cv2
import numpy as np

width, height = 640, 480
fourcc = cv2.VideoWriter_fourcc(*'mp4v')
out = cv2.VideoWriter('test_video.mp4', fourcc, 24, (width, height))

for i in range(240):
    frame = np.zeros((height, width, 3), dtype=np.uint8)
    cv2.circle(frame, (320+int(100*np.cos(i/24)), 240+int(100*np.sin(i/24))), 50, (0,255,0), -1)
    cv2.putText(frame, f'Frame {i}', (10,30), cv2.FONT_HERSHEY_SIMPLEX, 1, (255,255,255), 2)
    out.write(frame)

out.release()
print('test_video.mp4 created')
"
```

### Bước 4: Chạy Processing Server (Terminal 1)

```bash
# Khởi động server xử lý Spark
python ProcessingServer.py --kafka-brokers localhost:9095 --topic image-frames-topic --output ./output/processed_frames
```

### Bước 5: Chạy Camera Server (Terminal 2)

```bash
# Stream video vào Kafka
python CameraServer.py --video test_video.mp4 --camera-id CAM_001 --kafka-brokers localhost:9095 --fps 24 --quality 85
```

### Bước 6: Xem Kết Quả

Các frame đã xoá phông nền sẽ được lưu tại thư mục:

```
./output/processed_frames/
```

Mỗi file có tên dạng: `CAM_001_000001.png`, `CAM_001_000002.png`, ...

## 📊 Giám Sát Hệ Thống

### Kafka UI

Truy cập: http://localhost:8080

- Xem topics, messages, consumer groups
- Theo dõi throughput và lag

### Spark UI

Truy cập: http://localhost:4040 (khi ProcessingServer đang chạy)

- Xem batch processing times
- Monitor executors và tasks

## 🛠️ Tuỳ Chỉnh

### Thay Đổi Video Input

```bash
# Sử dụng webcam (camera mặc định)
python CameraServer.py --video 0

# Sử dụng file video khác
python CameraServer.py --video path/to/your/video.mp4
```

### Điều Chỉnh FPS và Quality

```bash
# Tăng FPS và giảm chất lượng để xử lý nhanh hơn
python CameraServer.py --video test_video.mp4 --fps 30 --quality 70

# Giảm FPS và tăng chất lượng cho kết quả đẹp hơn
python CameraServer.py --video test_video.mp4 --fps 15 --quality 95
```

### Xử Lý Nhiều Camera Cùng Lúc

```bash
# Terminal 3: Camera thứ 2
python CameraServer.py --video video2.mp4 --camera-id CAM_002 --kafka-brokers localhost:9095

# Terminal 4: Camera thứ 3
python CameraServer.py --video video3.mp4 --camera-id CAM_003 --kafka-brokers localhost:9095
```

## 📂 Cấu Trúc Thư Mục

```
background_remove/
├── CameraServer.py              # Server stream video
├── ProcessingServer.py          # Server xử lý Spark
├── requirements.txt             # Python dependencies
├── docker-compose.yml           # Kafka cluster config
├── selfie_segmenter.tflite     # Model AI xoá phông
├── test_video.mp4              # Video test (tự tạo)
├── output/
│   └── processed_frames/       # Kết quả output (PNG)
└── tmp/
    └── spark-checkpoint/       # Spark checkpoint data
```

## Tính Năng Chính

**Xử lý real-time:** 24+ FPS
**Phân tán:** Sử dụng Apache Spark
 **Fault-tolerant:** Checkpoint và recovery tự động
**Multi-camera:** Hỗ trợ nhiều nguồn video
**Background removal:** AI-powered với MediaPipe
