# ANSWERS

Bài làm cá nhân. Stack chạy: các service core (Kafka, Qdrant, MLflow, Feast, API, Envoy, OTel/Jaeger, Prometheus/Grafana) cộng thêm `--profile full` (Airflow, Spark-Connect) chạy local; vLLM thì chạy thật từ xa (model `Qwen/Qwen2.5-1.5B-Instruct`) trên notebook Kaggle, nối qua tunnel ngrok vì máy không có GPU. Toàn bộ bằng chứng nằm trong thư mục `evidence/`.

## Trade-off đã chọn

**Dùng vLLM từ xa qua ngrok thay vì `compose.gpu.yaml`.**

**Chạy core profile trước, full profile chỉ khi cần.** `docker compose up -d`

**Tinh chỉnh outlier-detection của Envoy (`base_ejection_time`).** Để gateway
tự loại bỏ pod chưa sẵn sàng (IP08) cần cấu hình passive outlier detection, vì health check chủ động vẫn cố tình trỏ vào `/health` (luôn "alive") chứ không phải `/ready`. Thời gian eject là một trade-off thật: để 3s (mặc định Envoy tự "un-eject" ngay khi health check tiếp theo pass) thì gần như không bao giờ bắt được lúc gateway đang từ chối; để 15s thì test đích pass ổn định
nhưng lại ảnh hưởng dây chuyền sang test kế tiếp (request bình thường vẫn bị từ chối vài giây sau khi dependency đã phục hồi). 5 giây là điểm cân bằng sau
khi thử nghiệm thực tế — chi tiết quá trình debug ở `evidence/failure-recovery-record.md` mục 3
(đây là bug thật của nền tảng, không phải phần "đã có sẵn").

**Chạy từng file test riêng thay vì chạy nguyên bộ integration-tests một lần.**
Sau khi chạy nguyên bộ suite (~27 phút) làm Spark-Connect bị OOM-kill giữa
chừng, kéo theo 41 lỗi dây chuyền không liên quan, tôi chuyển sang chạy từng
file một. Chậm hơn nhưng tách được đâu là lỗi thật, đâu là do máy hết RAM.

## Production gap tìm thấy (thật, không phải liệt kê lý thuyết)

1. **`/ready` không hề kiểm tra Spark/Delta.** Chỉ có kafka, mlflow, qdrant,
   vllm, feast được probe. Khi Spark-Connect bị OOM-kill giữa lúc chạy task,
   `/ready` không hề đổi trạng thái — chỉ phát hiện được nhờ task Airflow bị
   treo. Bản production cần thêm probe cho Spark-Connect/độ mới của Delta log.

2. **Spark-Connect không có giới hạn RAM trong `compose.yaml`.** Không có
   `mem_limit` hay giới hạn heap JVM nên nó tranh RAM vô tội vạ với các service
   khác. Đã bị OOM-kill 2 lần trong phiên làm việc này (`docker inspect` →
   `OOMKilled: true`, exit 137). Trong khi đó manifest Kubernetes lại có khai
   báo `resources` (được `scripts/validate_manifests.py` kiểm tra) — nghĩa là
   môi trường dev (compose) đang lệch so với những gì K8s thực sự giới hạn.

3. **Target scrape vLLM của Prometheus chỉ dành cho GPU local.** Job
   `lab28-vllm-optional` trong `monitoring/prometheus.yml` hardcode
   `host.docker.internal:8001`. Không có cách nào trỏ nó ra một endpoint từ xa
   mà không sửa config dùng chung bằng một giá trị cá nhân — đây là gap thật
   cho bất kỳ ai serve vLLM ngoài cluster, không chỉ riêng cách làm tạm của
   bài lab này.

4. **Chưa test trên cluster Kubernetes thật.** `scripts/validate_manifests.py`
   và `kubectl kustomize deploy/kubernetes/base` đều pass (cấu trúc, non-root,
   không dùng `:latest`, Gateway API v1 — xem
   `evidence/ip-k8s-rendered-manifests.yaml`), nhưng `kubectl apply --dry-run`
   cần cluster thật để tra cứu CRD của Gateway API. Dựng thêm cluster `kind`
   trên máy đã từng OOM 2 lần là rủi ro không đáng, nên phần drift/rollback
   trong `runbooks/gitops-rollback.md` vẫn chỉ là quy trình trên giấy, chưa
   chạy thật.

5. **Outlier detection một replica chỉ là giải pháp tạm.** Việc tinh chỉnh
   Envoy ở trên giúp 1 pod API xử lý được tình huống degraded, nhưng cách làm
   đúng cho production là nhiều replica đứng sau Gateway API trong K8s — loại
   1 pod lỗi thì traffic tự chuyển sang pod còn lại, không cần tinh chỉnh thời
   gian eject. Môi trường docker-compose ở đây chỉ chạy 1 container `api` nên
   không thể minh họa được điều này.

## Đóng góp

Tôi đã cài đặt 4 hàm starter trong
`src/lab28_platform/integration_tasks.py` (`event_headers`, `dedupe_latest`, `feast_online_request`, `readiness_status`) theo đúng docstring và `starter-tests`, sau đó tự vận hành toàn bộ stack thật, chạy và debug bộ integration-tests, tìm và sửa bug gateway nói ở trên, và tạo ra toàn bộ bằng
chứng trong `evidence/`.
