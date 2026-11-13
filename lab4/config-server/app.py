import os
from collections import defaultdict
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

PORT = int(os.getenv("PORT", "8888"))

app = FastAPI(title="config-server")

# Service registry: service_name -> set of instance URLs
registry: dict[str, set[str]] = defaultdict(set)


class RegistrationRequest(BaseModel):
    service_name: str
    instance_url: str


@app.get("/health")
async def health() -> dict:
    return {
        "status": "ok",
        "registered_services": {name: list(instances) for name, instances in registry.items()},
    }


@app.post("/register")
async def register_service(req: RegistrationRequest) -> dict:
    url = req.instance_url.rstrip("/")
    registry[req.service_name].add(url)
    print(f"[config-server] Registered {req.service_name} at {url} (Total {len(registry[req.service_name])})", flush=True)
    return {
        "status": "registered",
        "service_name": req.service_name,
        "instance_url": url,
        "instances": list(registry[req.service_name]),
    }


@app.get("/services/{service_name}")
async def get_service_instances(service_name: str) -> dict:
    instances = list(registry.get(service_name, []))
    if not instances:
        # Return empty list rather than 404 to avoid crashing dynamic discovery during startup
        return {"service_name": service_name, "instances": []}
    return {"service_name": service_name, "instances": instances}


@app.get("/services")
async def get_all_services() -> dict:
    return {"services": {name: list(instances) for name, instances in registry.items()}}


@app.post("/reset")
async def reset_registry() -> dict:
    registry.clear()
    print("[config-server] Registry cleared", flush=True)
    return {"status": "reset"}


if __name__ == "__main__":
    print(f"[config-server] Starting configuration server on port {PORT}", flush=True)
    uvicorn.run(app, host="0.0.0.0", port=PORT, access_log=False, log_level="warning")
