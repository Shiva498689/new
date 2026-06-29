"""
main.py — FastAPI application entry point.

Architecture (from context.md / Instructions.md):
  - POST /api/start-pipeline  : accepts ticker, spawns background LangGraph task, returns job_id
  - GET  /api/stream/{job_id} : SSE endpoint streaming per-node progress + final markdown memo
  - Zero-click autonomy: no HITL, pipeline runs start-to-finish unattended
  - StreamingResponse / SSE to handle 2-5 min LangGraph execution (prevents HTTP timeout)
  - All secrets loaded from .env via python-dotenv
  - Graceful error handling: third-party failures emit error SSE events, server stays up
"""

import asyncio
import json
import os
import uuid
from contextlib import asynccontextmanager
from typing import Any, Dict

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse

# Load .env before any module that reads os.getenv at import time
load_dotenv()

# Import the graph builder — done after load_dotenv so env vars are available
from masterlanggraph import build_graph

# ---------------------------------------------------------------------------
# In-memory job store: job_id -> asyncio.Queue
# Each queue item is a dict: {"event": str, "data": str}
# A sentinel None signals the SSE generator to close.
# ---------------------------------------------------------------------------
_job_queues: Dict[str, asyncio.Queue] = {}

# ---------------------------------------------------------------------------
# Application lifecycle
# ---------------------------------------------------------------------------
graph = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global graph
    graph = build_graph()
    print("[startup] LangGraph compiled successfully.")
    yield
    print("[shutdown] FDD Platform shutting down.")


app = FastAPI(
    title="FDD Platform API",
    description="Autonomous Financial Due Diligence pipeline with SSE streaming.",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",   # Vite dev server
        "http://localhost:8080",
        "http://localhost:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------
class PipelineRequest(BaseModel):
    ticker: str


class PipelineResponse(BaseModel):
    job_id: str
    message: str


# ---------------------------------------------------------------------------
# Background pipeline runner
# ---------------------------------------------------------------------------
async def _run_pipeline(job_id: str, ticker: str) -> None:
    """
    Runs the LangGraph pipeline in a thread (blocking call) and pushes
    SSE events to the job queue as each node completes.
    """
    queue = _job_queues[job_id]

    # Build the initial AgentState — inject server-side env vars here
    initial_state: Dict[str, Any] = {
        "ticker": ticker,
        "groq_api_key": os.getenv("GROQ_API_KEY", ""),
        "error": None,
    }

    try:
        await queue.put({"event": "pipeline_start", "data": json.dumps({"ticker": ticker})})

        # Stream node events by running graph.stream synchronously in a thread
        # and posting events to the queue as they come
        def _run_and_emit():
            # graph.stream() yields dict items of the form {"node_name": state_update}
            for event in graph.stream(initial_state):
                for node_name, state_chunk in event.items():
                    event_data = json.dumps({"node": node_name})
                    asyncio.get_event_loop().call_soon_threadsafe(
                        queue.put_nowait,
                        {"event": "node_complete", "data": event_data}
                    )

                    # If this node set an error, report it and terminate graph stream early
                    if "error" in state_chunk and state_chunk["error"]:
                        err_msg = state_chunk["error"]
                        asyncio.get_event_loop().call_soon_threadsafe(
                            queue.put_nowait,
                            {"event": "error", "data": json.dumps({"message": err_msg})}
                        )
                        return

                    # If this node produced the final report, extract it
                    if "markdown_report" in state_chunk:
                        report = state_chunk["markdown_report"]
                        asyncio.get_event_loop().call_soon_threadsafe(
                            queue.put_nowait,
                            {"event": "result", "data": json.dumps({"markdown_report": report})}
                        )

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, _run_and_emit)

    except Exception as e:
        # Per Instructions.md §4: catch errors, log to SSE stream, don't crash server
        error_msg = f"Pipeline error for {ticker}: {str(e)}"
        print(f"[error] {error_msg}")
        await queue.put({"event": "error", "data": json.dumps({"message": error_msg})})
    finally:
        # Sentinel: tells the SSE generator the stream is done
        await queue.put(None)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
@app.post("/api/start-pipeline", response_model=PipelineResponse)
async def start_pipeline(request: PipelineRequest):
    """
    Task 2.4 — Accepts a ticker symbol, starts the LangGraph pipeline
    as a background task, and immediately returns a job_id.
    """
    ticker = request.ticker.strip().upper()
    if not ticker:
        raise HTTPException(status_code=400, detail="ticker must not be empty")

    job_id = str(uuid.uuid4())
    _job_queues[job_id] = asyncio.Queue()

    # Fire-and-forget: background coroutine runs the blocking LangGraph pipeline
    asyncio.create_task(_run_pipeline(job_id, ticker))

    return PipelineResponse(
        job_id=job_id,
        message=f"Pipeline started for {ticker}. Connect to /api/stream/{job_id} for live updates.",
    )


@app.get("/api/stream/{job_id}")
async def stream_pipeline(job_id: str, request: Request):
    """
    SSE endpoint. Yields per-node progress events and the final
    markdown memo as the LangGraph pipeline runs.
    """
    if job_id not in _job_queues:
        raise HTTPException(status_code=404, detail=f"job_id '{job_id}' not found.")

    queue = _job_queues[job_id]

    async def event_generator():
        try:
            while True:
                # 1. Watch for client disconnects
                if await request.is_disconnected():
                    break

                # 2. Wait for the next event in the queue
                item = await queue.get()
                if item is None:
                    break

                # 3. Yield the event to the frontend
                yield {"event": item["event"], "data": item["data"]}

                # 4. If the event was an error, terminate the stream gracefully
                if item["event"] == "error":
                    break
        except asyncio.CancelledError:
            # Client disconnected mid-stream
            pass
        finally:
            # Clean up the queue
            _job_queues.pop(job_id, None)

    return EventSourceResponse(event_generator())


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------
@app.get("/health")
async def health():
    return {"status": "ok", "graph_compiled": graph is not None}
