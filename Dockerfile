# 区域地震台网告警可靠投递中继 / 应急广播网关接收模拟器
# 纯 Python 标准库实现，镜像内无第三方依赖。
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# 先放代码（镜像层可缓存）
COPY relay ./relay
COPY tests ./tests
COPY scripts ./scripts

RUN mkdir -p /data && \
    python -m compileall -q relay tests scripts

EXPOSE 8080 8081

# 默认启动 API；compose 中 receiver 服务覆盖为 relay.receiver。
CMD ["python", "-m", "relay.api"]
