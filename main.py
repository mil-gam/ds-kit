from fastapi import FastAPI, Request, HTTPException, Security
from fastapi.security.api_key import APIKeyQuery
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
import httpx
import asyncio
import time

from limiter import calculate_dynamic_limit, get_current_cpu_ema, background_sync_worker, redis_client, get_latest_defense_status

app = FastAPI()

# 템플릿 폴더 지정 (templates 디렉토리 연동)
templates = Jinja2Templates(directory="templates")

ADMIN_SECRET_KEY = "ds-kit-admin-secure-2026"
api_key_query = APIKeyQuery(name="key", auto_error=False)

#정상요청에 대한 반환 서버주소
UPSTREAM_SERVERS = {
    "/wiki": "http://127.0.0.1:8080",
    "/api": "http://127.0.0.1:8090",
    "/default": "http://127.0.0.1:8080"
}
http_client = httpx.AsyncClient(timeout=10.0)

#표시될 요청의 구분 200/429와 토탈
DEFAULT_WINDOW_SIZE = 10
stats = {"total_requests": 0, "passed_200": 0, "blocked_429": 0}
latest_slope = 0.0

#on_event-> lifespan 추후 변경. 60초단위 서버 인터넷연결 점검
@app.on_event("startup")
async def startup_event():
    asyncio.create_task(background_sync_worker())

#프록시로 들어오는 http요청
@app.middleware("http")
async def ds_kit_gateway_middleware(request: Request, call_next):
    global latest_slope
    path = request.url.path


    if path.startswith("/admin") or path.startswith("/dashboard"):
        return await call_next(request)

    stats["total_requests"] += 1

    #클라이언트의 IP를 식별. 프록시있음=> IP 없으면 접속주소. 실패하면 127.0.0.1
    client_ip = request.headers.get("X-Forwarded-For", request.client.host)
    if not client_ip:
        client_ip = "127.0.0.1"

    #접근한 서버-> 명칭, CPU, 허용량등 ===> limit를 설계, 속도를 수치화
    server_id = "default_server"
    limit_max, current_s_cpu, traffic_slope = await calculate_dynamic_limit(server_id)
    latest_slope = traffic_slope
    window_size = DEFAULT_WINDOW_SIZE

    #윈도우 슬라이딩 단위시간 10초. Redis저장용 이름 redis_key
    current_time = time.time() * 1000  
    window_start = current_time - (window_size * 1000)
    redis_key = f"rate_limit:{client_ip}"

    #윈도우 슬라이딩방식. 단위시간 이전 요청 초기화-> 남은내용 묶어서 요청수조회 및 만료시간설정
    try:
        pipe = redis_client.pipeline()
        pipe.zremrangebyscore(redis_key, 0, window_start)
        pipe.zcard(redis_key)
        pipe.zadd(redis_key, {str(current_time): current_time})
        pipe.expire(redis_key, window_size + 2)

        
        results = await pipe.execute()
        request_count = results[1]

        #조건1. 단일 임계치가 limit_max를 초과하는가? 차단하고 로그남김
        if request_count >= limit_max:
            stats["blocked_429"] += 1
            return JSONResponse(
                status_code=429,
                content={
                    "detail": "Too Many Requests - Blocked by DS KIT L7 Defense", 
                    "dynamic_limit": limit_max,
                    "s_cpu": current_s_cpu,
                    "slope": traffic_slope
                }
            )
    except Exception as e:
        print(f"Redis Error: {e}")

    stats["passed_200"] += 1

    #정상 요청에 대한 백엔드 서버주소 선택
    target_backend = UPSTREAM_SERVERS["/default"]
    for prefix, backend_url in UPSTREAM_SERVERS.items():
        if prefix != "/default" and path.startswith(prefix):
            target_backend = backend_url
            break

    #클라이언트 요청의 원본 커리 및 body를 추출하여 백엔드 서버로 전송. 서버로 부터의 응답을 다시 클라이언트에게 전송
    try:
        sub_url = str(request.url.path)
        if request.url.query:
            sub_url += f"?{request.url.query}"
            
        body = await request.body()
        async with httpx.AsyncClient(base_url=target_backend, timeout=10.0) as client:
            response = await client.request(
                method=request.method,
                url=sub_url,
                headers=dict(request.headers),
                content=body
            )
            return HTMLResponse(
                content=response.content,
                status_code=response.status_code,
                headers=dict(response.headers)
            )
    except httpx.RequestError:
        return JSONResponse(status_code=503, content={"detail": "Upstream Backend Unavailable"})

#관리자 대시보드 통계용 API엔드포인트
@app.get("/admin/stats")
async def get_admin_stats(key: str = Security(api_key_query)):
    if key != ADMIN_SECRET_KEY:
        raise HTTPException(status_code=403, detail="Unauthorized")
    
    defense_status = await get_latest_defense_status("default_server")
    
    return {
        "status": "active",
        "total_requests": stats["total_requests"],
        "passed_200": stats["passed_200"],
        "blocked_429": stats["blocked_429"],
        "current_s_cpu": defense_status["s_cpu"],
        "final_limit": defense_status["final_limit"],
        "traffic_slope": defense_status["slope"]
    }

#외부 브라우저의 대시보드 접속용 엔드포인트
@app.get("/dashboard", response_class=HTMLResponse)
async def admin_dashboard(request: Request, key: str):
    """HTML 파일을 별도로 불러와서 렌더링하는 엔드포인트"""
    if key != ADMIN_SECRET_KEY:
        return HTMLResponse("<h1>403 Unauthorized - Invalid Admin Key</h1>", status_code=403)
    
    # templates/dashboard.html 파일을 렌더링하며 api_key 변수 전달
    return templates.TemplateResponse("dashboard.html", {"request": request, "api_key": key})


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)