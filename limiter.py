#시간, CPU연결, 백그라운드, Redis사용
import time 
import psutil
import asyncio
import redis.asyncio as aioredis

# Redis 클라이언트 연결 -> 비동기방식, 반환문자열 바로사용
redis_client = aioredis.from_url("redis://localhost:6379", decode_responses=True)

# 로컬 메모리 캐시. 평균 및 표준편차, 싱크시간 딕셔너리.
_local_memory_cache = {
    "mu": 3000.0,
    "sigma": 300.0,
    "last_sync_time": 0.0,
    "is_offline_fallback": False
}

# CPU 평활화(EMA) CPU상태에 따른 지수이동평균과 단위시간 dt계산 스탬프
_last_s_cpu = 50.0
_last_check_time = time.time()


async def background_sync_worker():
    """
    정기 백그라운드 배치
    주기적으로 Redis/DB에서 최신 통계를 메모리로 동기화하여
    지연 시간(Latency)을 최소화
    """
    while True:
        try:
            # 서버 + 시간(월/일/시). 조회용
            server_id = "default_server"
            current_time_key = f"stats:{server_id}:" + time.strftime("%m-%d-%H")

            #평균, 표편
            cached_mu = await redis_client.get(f"{current_time_key}:mu")
            cached_sigma = await redis_client.get(f"{current_time_key}:sigma")
            
            current_time = time.time()

            #데이터가 있나? -> 갱신. 없나? -> 기존거 재사용. 
            if cached_mu is not None and cached_sigma is not None:
                _local_memory_cache["mu"] = float(cached_mu)
                _local_memory_cache["sigma"] = float(cached_sigma)
                _local_memory_cache["last_sync_time"] = current_time
                _local_memory_cache["is_offline_fallback"] = False
            else:
                three_days_sec = 3 * 24 * 60 * 60
                if _local_memory_cache["last_sync_time"] > 0 and (current_time - _local_memory_cache["last_sync_time"]) > three_days_sec:
                    _local_memory_cache["is_offline_fallback"] = True
        #통신에러-> 로그남기고 폴백
        except Exception as e:
            print(f"Background Sync Error (Using Fallback Cache): {e}")
            _local_memory_cache["is_offline_fallback"] = True
            
        await asyncio.sleep(60)  # 60초마다 주기적 동기화 ==> 주기적 연결상태 확인

#방어 임계치 함수
async def calculate_dynamic_limit(server_id: str) -> tuple[int, float, float]:
    """
    1. psutil 실시간 CPU 측정
    2. Redis ZSET 기반 트래픽 변화율(Slope) 계산
    3. 메모리 캐시 및 3일 이상 Fallback 적용
    4. EMA 평활화 및 60%~85% 연속 감쇠(k_cpu) 적용
    """
    global _last_s_cpu, _last_check_time
    
    now = time.time()
    dt = now - _last_check_time
    if dt <= 0:
        dt = 1.0
    _last_check_time = now

    # 1. 실제 OS CPU 사용률 측정 연동 (psutil)
    current_cpu = psutil.cpu_percent(interval=None)

    # 2. 트래픽 변화율(Slope / 미분값) 계산 로직. 노트7페이지 공식참고.
    try:
        slope_key = f"traffic_slope:{server_id}"
        pipe = redis_client.pipeline()
        pipe.zadd(slope_key, {str(now): now})
        pipe.zremrangebyscore(slope_key, 0, now - 10)  # 최근 10초간 기록 유지
        pipe.zcard(slope_key)
        res = await pipe.execute()
        recent_count = res[2]
        traffic_slope = recent_count / 10.0  # 초당 유입 증가율 (Slope)
    except Exception:
        traffic_slope = 0.0

    # 3. 메모리 캐시에서 시즌리티 값 추출 (DB 접근 최소화)
    mu = _local_memory_cache["mu"]
    sigma = _local_memory_cache["sigma"]

    # 4. 지수 이동 평균(EMA)을 통한 실시간 CPU 평활화 S_CPU(t)
    alpha = 0.3  
    s_cpu = alpha * current_cpu + (1 - alpha) * _last_s_cpu
    _last_s_cpu = s_cpu  

    # 5. 연속 감쇠 함수 k(CPU) 산출 (60% ~ 85% 선형 감소)
    if s_cpu <= 60.0:
        k_cpu = 1.0
    elif 60.0 < s_cpu <= 85.0:
        k_cpu = 1.0 - 0.032 * (s_cpu - 60.0)
    elif 85.0 < s_cpu <= 95.0:
        k_cpu = 0.2  
    else:
        k_cpu = 0.05  # 최후의 방어선 (하드 락다운)

    # 트래픽 가속도(Slope)가 비정상적으로 급증할 경우 추가 패널티 부여
    if traffic_slope > 150:
        k_cpu *= 0.75

    # 6. 최종 동적 임계치 계산: final_limit = (mu + sigma) * k_cpu
    final_limit = int((mu + sigma) * k_cpu)
    if final_limit < 10:
        final_limit = 10

    return final_limit, round(_last_s_cpu, 2), round(traffic_slope, 2)

def get_current_cpu_ema() -> float:
    return round(_last_s_cpu, 2)

async def get_latest_defense_status(server_id: str) -> dict:
    """대시보드 UI 표출을 위한 최신 방어 지표 요약 반환 함수"""
    limit, s_cpu, slope = await calculate_dynamic_limit(server_id)
    return {
        "final_limit": limit,
        "s_cpu": s_cpu,
        "slope": slope
    }