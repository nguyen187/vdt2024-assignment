# VDT 2024 - Assignment
> - file spark_processing_vdt2024.py dùng để chạy spark xử lý dữ liệu
> - file Producer.py dùng để gửi data lên kafka

# Tiêu đề dự án

Dự án này liên quan đến việc xử lý dữ liệu hoạt động của sinh viên sử dụng Apache Kafka, Hadoop, Nifi và Spark. Dữ liệu được đọc từ một tệp CSV, gửi đến một chủ đề Kafka, lưu trữ trong Hadoop Distributed File System (HDFS) và xử lý bằng Spark.

## III. Đẩy dữ liệu vào Kafka Topic

### Tạo topic vdt2024

Truy cập vào Kafka UI tại `localhost:8080` và tạo topic với tên `vdt2024`, 10 partitions, Min In Sync Replicas=2, và Replication 2.

### Tạo File Producer.py đọc file log_action.csv

Đọc dữ liệu từ tệp "log_action.csv" và gửi các bản ghi tương ứng đến một Kafka Topic được xác định ("vdt2024"). 

Chạy lệnh trên terminal: `python Producer.py`

## IV. Triển khai Hadoop

Truy cập `localhost:9870` và tạo đường dẫn chứa dữ liệu action log : `/raw_zone/fact/activity`

Lưu file `danh_sach_sv_de.csv` vào HDFS bằng cách tạo đường dẫn `/raw_zone/fact/ds_de_vdt` và sử dụng PySpark code để sao chép file vào HDFS.

## V. Cấu hình và triển khai Nifi 

Truy cập vào giao diện quản lý của NiFi (`http://localhost:8091`).

Lấy dữ liệu từ Kafka topic `vdt2024` bằng cách tạo 1 processor với type là `ConsumeKafka_2_6`.

Lưu dữ liệu parquet xuống HDFS bằng cách tạo processor với type là `PutParquet`.

Kích hoạt các tiến trình và kiểm tra xem dữ liệu đã được xử lý và lưu trữ đúng cách trên HDFS chưa.

## VI. Triển khai Spark 

Thông tin về xử lý chi tiết trong file `spark_processing_vdt2024.py`

Để có thể submit một spark job, cần truy cập vào container và copy file .py vào container `spark-master`

Truy cập vào container của `spark-master` bằng lệnh: `sudo docker exec -it f59701b22210 bash`

Di chuyển vào thư mục cài spark và tạo 2 thư mục `vdt-assigment-2024`(dùng để lưu file pyspark) và `output` (lưu kết quả trả về) 

Mở terminal khác và chạy lệnh copy file vào container `spark-master`: `sudo docker cp spark_processing_vdt2024.py f59701b22210:/spark/conf/vdt-assigment-2024`

Submit spark job bằng lệnh: 
```bash
./bin/spark-submit \
--master spark://spark-master:7077 \
/spark/conf/vdt-assigment-2024/spark_processing_vdt2024.py \
